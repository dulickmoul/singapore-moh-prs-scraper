import csv
import json
import re
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Dict

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

START_URL = "https://prs.moh.gov.sg/prs/internet/profSearch/mshowSearchSummaryByName.action?hpe=SMC"
POST_URL = "https://prs.moh.gov.sg/prs/internet/profSearch/mgetSearchSummaryByName.action"

RUN_MONTH = datetime.now().strftime("%Y-%m")
OUT_DIR = Path("output")
OUT_DIR.mkdir(exist_ok=True)

RAW_CSV = OUT_DIR / "smc_full_index_AZ_repair.csv"
PAGE_LOG_CSV = OUT_DIR / "smc_page_log.csv"
STATE_FILE = Path("hybrid_az_repair_state.json")
DB = "smc_prs.db"

LAST_RE = re.compile(r"gotoPageDEFAULT\((\d+)\)")

REQUEST_DELAY_SEC = 0.5
MAX_PAGE_RETRIES = 4


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def load_state() -> Dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {"letter": "A", "page": 1}


def save_state(letter: str, page: int) -> None:
    STATE_FILE.write_text(
        json.dumps({"letter": letter, "page": page}, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def db_connect():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    return con


def init_db(con):
    con.executescript("""
    PRAGMA journal_mode=WAL;

    DROP TABLE IF EXISTS index_snapshot;

    CREATE TABLE IF NOT EXISTS index_snapshot (
      run_month TEXT NOT NULL,
      license TEXT NOT NULL,
      name TEXT,
      detail_url TEXT,
      query_letter TEXT,
      first_seen TEXT,
      last_seen TEXT,
      PRIMARY KEY (run_month, license, query_letter, detail_url)
    );
    """)
    con.commit()


def ensure_csv_headers():
    if not RAW_CSV.exists():
        with RAW_CSV.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["run_month", "license", "name", "detail_url", "query_letter"])

    if not PAGE_LOG_CSV.exists():
        with PAGE_LOG_CSV.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "run_month",
                "query_letter",
                "page",
                "expected_total_pages",
                "indexed_count",
                "retry_count",
                "status"
            ])


def wait_until_results_ready(driver, timeout=300):
    def _ready(d):
        try:
            body_text = d.find_element(By.TAG_NAME, "body").text or ""
            return "Displaying" in body_text and "records" in body_text
        except Exception:
            return False

    WebDriverWait(driver, timeout).until(_ready)


def selenium_bootstrap_for_letter(letter: str) -> Tuple[requests.Session, str]:
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    driver.get(START_URL)

    print(f"\nManual step for letter {letter}:")
    print(f"1. Type {letter} in the Name box")
    print("2. Tick Terms")
    print("3. Click Search")
    print("4. Solve reCAPTCHA if it appears")
    input("When results are visible in Chrome, press Enter here... ")

    wait_until_results_ready(driver, timeout=300)
    html = driver.page_source

    sess = requests.Session()
    for c in driver.get_cookies():
        sess.cookies.set(c["name"], c["value"])

    driver.quit()
    return sess, html


def get_total_pages_from_html(html: str) -> int:
    soup = BeautifulSoup(html, "html.parser")
    last_link = soup.find("a", string=lambda s: s and s.strip() == "Last")
    if not last_link:
        return 1
    href = last_link.get("href", "")
    m = LAST_RE.search(href)
    return int(m.group(1)) if m else 1


def parse_items_from_html(html: str) -> List[Tuple[str, str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    items: List[Tuple[str, str, str]] = []

    for link in soup.find_all("a", string=lambda s: s and "View more details" in s):
        href = (link.get("href") or "").strip()
        onclick = (link.get("onclick") or "").strip()

        # temporary debug for first unresolved links on a page
        if len(items) == 0:
            print("DEBUG LINK href =", href)
            print("DEBUG LINK onclick =", onclick)
            try:
                print("DEBUG LINK parent text =", link.parent.get_text(" ", strip=True)[:300])
            except Exception:
                pass

        detail_url = href
        if detail_url.startswith("/"):
            detail_url = "https://prs.moh.gov.sg" + detail_url
        elif detail_url.startswith("javascript") or detail_url == "#" or not detail_url:
            detail_url = ""

        lic_match = re.search(r"(M\d+[A-Z])", href) or re.search(r"(M\d+[A-Z])", onclick)

        name = ""
        license_no = ""

        parent = link.parent
        for _ in range(8):
            if parent is None:
                break

            text = parent.get_text(" ", strip=True)

            if not lic_match:
                lic_match = re.search(r"\((M\d+[A-Z])\)", text)

            if lic_match and not license_no:
                license_no = lic_match.group(1)

            if license_no:
                name_match = re.search(
                    r"(.+?)\s*\(" + re.escape(license_no) + r"\)",
                    text
                )
                if name_match:
                    name = " ".join(name_match.group(1).split()).strip(" ,")
                    break

            parent = parent.parent

        if not license_no and lic_match:
            license_no = lic_match.group(1)

        if not detail_url and license_no:
            detail_url = (
                "https://prs.moh.gov.sg/prs/internet/profSearch/"
                f"mgetSearchDetails.action?regNo={license_no}"
            )

        if license_no:
            items.append((name, license_no, detail_url))

    return items


def debug_page_counts(html: str):
    soup = BeautifulSoup(html, "html.parser")

    detail_count = 0
    parsed_licenses = 0

    for link in soup.find_all("a", string=lambda s: s and "View more details" in s):
        detail_count += 1

        href = (link.get("href") or "").strip()
        onclick = (link.get("onclick") or "").strip()

        lic_match = re.search(r"(M\d+[A-Z])", href) or re.search(r"(M\d+[A-Z])", onclick)

        if not lic_match:
            parent = link.parent
            for _ in range(8):
                if parent is None:
                    break
                text = parent.get_text(" ", strip=True)
                lic_match = re.search(r"\((M\d+[A-Z])\)", text)
                if lic_match:
                    break
                parent = parent.parent

        if lic_match:
            parsed_licenses += 1

    return detail_count, parsed_licenses


def fetch_page_once(session: requests.Session, letter: str, page: int) -> str:
    data = {
        "hpe": "SMC",
        "regNo": "",
        "psearchParamVO.language": "eng",
        "psearchParamVO.searchBy": "N",
        "psearchParamVO.name": letter,
        "psearchParamVO.pracPlaceName": "",
        "psearchParamVO.rbtnRegister": "all",
        "psearchParamVO.regNo": "",
        "selectType": "all",
    }

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://prs.moh.gov.sg",
        "Referer": POST_URL,
        "User-Agent": "Mozilla/5.0",
    }

    session.cookies.set("cookie.tableId", "DEFAULT")
    session.cookies.set("cookie.currentPage", f"DEFAULT.{page}")

    resp = session.post(
        POST_URL,
        headers=headers,
        data=data,
        timeout=120
    )
    resp.raise_for_status()
    return resp.text


def fetch_page_with_retry(
    session: requests.Session,
    letter: str,
    page: int,
    total_pages: int,
    max_retries: int = MAX_PAGE_RETRIES
) -> Tuple[str, List[Tuple[str, str, str]], int, str]:
    last_html = ""
    last_items: List[Tuple[str, str, str]] = []

    is_last_page = page == total_pages
    min_good_count = 1 if is_last_page else 10

    for attempt in range(1, max_retries + 1):
        try:
            html = fetch_page_once(session, letter, page)
            items = parse_items_from_html(html)

            last_html = html
            last_items = items

            if len(items) >= min_good_count:
                return html, items, attempt - 1, "ok"

            print(
                f"Letter {letter} page {page}/{total_pages}: "
                f"weak page ({len(items)} items), retry {attempt}/{max_retries}"
            )
            time.sleep(1.5 * attempt)

        except Exception as e:
            print(
                f"Letter {letter} page {page}/{total_pages}: "
                f"request failed on retry {attempt}/{max_retries}: {e}"
            )
            time.sleep(2 * attempt)

    if last_items:
        return last_html, last_items, max_retries, "weak"
    return last_html, [], max_retries, "failed"


def append_raw_rows(rows: List[Tuple[str, str, str]], letter: str):
    with RAW_CSV.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for name, lic, url in rows:
            w.writerow([RUN_MONTH, lic, name, url, letter])


def append_page_log(letter: str, page: int, total_pages: int, indexed_count: int, retry_count: int, status: str):
    with PAGE_LOG_CSV.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([RUN_MONTH, letter, page, total_pages, indexed_count, retry_count, status])


def main():
    ensure_csv_headers()
    con = db_connect()
    init_db(con)

    state = load_state()
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    start_idx = letters.index(state["letter"]) if state["letter"] in letters else 0

    for li in range(start_idx, len(letters)):
        letter = letters[li]
        start_page = state["page"] if letter == state["letter"] else 1

        session, first_html = selenium_bootstrap_for_letter(letter)
        total_pages = get_total_pages_from_html(first_html)
        print(f"Total pages for {letter}: {total_pages}")

        for page in range(start_page, total_pages + 1):
            save_state(letter, page)

            if page == 1:
                html = first_html
                items = parse_items_from_html(html)
                retry_count = 0
                status = "ok" if len(items) > 0 else "weak"
            else:
                html, items, retry_count, status = fetch_page_with_retry(
                    session=session,
                    letter=letter,
                    page=page,
                    total_pages=total_pages,
                    max_retries=MAX_PAGE_RETRIES
                )
                time.sleep(REQUEST_DELAY_SEC)

            detail_count, parsed_licenses = debug_page_counts(html)
            print(
                f"DEBUG letter {letter} page {page}: "
                f"detail_links={detail_count}, parsed_licenses={parsed_licenses}, parsed_items={len(items)}"
            )

            for name, lic, url in items:
                con.execute(
                    """
                    INSERT OR REPLACE INTO index_snapshot
                    (run_month, license, name, detail_url, query_letter, first_seen, last_seen)
                    VALUES (?, ?, ?, ?, ?, COALESCE(
                        (SELECT first_seen FROM index_snapshot
                         WHERE run_month = ? AND license = ? AND query_letter = ? AND detail_url = ?), ?
                    ), ?)
                    """,
                    (
                        RUN_MONTH, lic, name, url, letter,
                        RUN_MONTH, lic, letter, url, now_iso(), now_iso()
                    )
                )

            con.commit()
            append_raw_rows(items, letter)
            append_page_log(letter, page, total_pages, len(items), retry_count, status)

            print(
                f"Letter {letter} page {page}/{total_pages}: "
                f"indexed {len(items)}"
                + (f" [{status}, retries={retry_count}]" if retry_count or status != "ok" else "")
            )

        save_state(letter, total_pages + 1)

    con.close()
    print(f"\nDone. Raw CSV saved to: {RAW_CSV.resolve()}")
    print(f"Page log saved to: {PAGE_LOG_CSV.resolve()}")
    print("You can delete hybrid_az_repair_state.json if you want a fresh rerun.")


if __name__ == "__main__":
    main()