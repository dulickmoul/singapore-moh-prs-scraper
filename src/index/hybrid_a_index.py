import csv
import re
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

START_URL = "https://prs.moh.gov.sg/prs/internet/profSearch/mshowSearchSummaryByName.action?hpe=SMC"
POST_URL = "https://prs.moh.gov.sg/prs/internet/profSearch/mgetSearchSummaryByName.action"

RUN_MONTH = datetime.now().strftime("%Y-%m")
OUT_DIR = Path("output")
OUT_DIR.mkdir(exist_ok=True)
INDEX_CSV = OUT_DIR / "smc_full_index_A.csv"
DB = "smc_prs.db"

LICENSE_RE = re.compile(r"\((M\d{4,6}[A-Z])\)")
LAST_RE = re.compile(r"gotoPageDEFAULT\((\d+)\)")


def db_connect():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    return con


def init_db(con):
    con.executescript("""
    PRAGMA journal_mode=WAL;

    CREATE TABLE IF NOT EXISTS index_snapshot (
      run_month TEXT NOT NULL,
      license TEXT NOT NULL,
      name TEXT,
      detail_url TEXT,
      first_seen TEXT,
      last_seen TEXT,
      PRIMARY KEY (run_month, license)
    );
    """)
    con.commit()


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def ensure_csv_header():
    if not INDEX_CSV.exists():
        with open(INDEX_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["run_month", "license", "name", "detail_url"])


def wait_until_results_ready(driver, timeout=300):
    def _ready(d):
        try:
            body_text = d.find_element(By.TAG_NAME, "body").text or ""
            return "Displaying" in body_text and "records" in body_text
        except Exception:
            return False

    WebDriverWait(driver, timeout).until(_ready)


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

    detail_urls = []
    for a in soup.find_all("a", href=True):
        text = a.get_text(" ", strip=True)
        if "View more details" in text:
            href = a["href"]
            if href.startswith("/"):
                href = "https://prs.moh.gov.sg" + href
            detail_urls.append(href)

    # Get visible text lines and extract NAME (LICENSE)
    text = soup.get_text("\n", strip=True)
    matches = re.findall(r"([A-Z][A-Z\s\.'\-]+)\s*\((M\d{4,6}[A-Z])\)", text)

    # Deduplicate by license while preserving order
    seen = set()
    cleaned = []
    for name, lic in matches:
        if lic not in seen:
            seen.add(lic)
            cleaned.append((name.strip(), lic.strip()))

    items = []
    for i in range(min(len(cleaned), len(detail_urls))):
        name, lic = cleaned[i]
        url = detail_urls[i]
        items.append((name, lic, url))

    return items


def selenium_bootstrap():
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    driver.get(START_URL)

    print("\nManual step:")
    print("1. Type A in the Name box")
    print("2. Tick Terms")
    print("3. Click Search")
    print("4. Solve reCAPTCHA if it appears")
    input("When results are visible in Chrome, press Enter here... ")

    wait_until_results_ready(driver, timeout=300)
    html = driver.page_source

    # Transfer cookies to requests
    sess = requests.Session()
    for c in driver.get_cookies():
        sess.cookies.set(c["name"], c["value"])

    driver.quit()
    return sess, html


def fetch_page(session: requests.Session, page: int) -> str:
    # Keep same payload as successful search, but without g-recaptcha-response
    data = {
        "hpe": "SMC",
        "regNo": "",
        "psearchParamVO.language": "eng",
        "psearchParamVO.searchBy": "N",
        "psearchParamVO.name": "A",
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

    # This is the key pagination control
    session.cookies.set("cookie.tableId", "DEFAULT")
    session.cookies.set("cookie.currentPage", f"DEFAULT.{page}")

    resp = session.post(POST_URL, headers=headers, data=data, timeout=60)
    resp.raise_for_status()
    return resp.text


def main():
    ensure_csv_header()
    con = db_connect()
    init_db(con)

    session, first_html = selenium_bootstrap()

    total_pages = get_total_pages_from_html(first_html)
    print(f"Total pages for A: {total_pages}")

    for page in range(1, total_pages + 1):
        if page == 1:
            html = first_html
        else:
            html = fetch_page(session, page)
            time.sleep(0.2)

        items = parse_items_from_html(html)

        for name, lic, url in items:
            con.execute(
                """
                INSERT OR REPLACE INTO index_snapshot
                (run_month, license, name, detail_url, first_seen, last_seen)
                VALUES (?, ?, ?, ?, COALESCE(
                    (SELECT first_seen FROM index_snapshot WHERE run_month = ? AND license = ?), ?
                ), ?)
                """,
                (RUN_MONTH, lic, name, url, RUN_MONTH, lic, now_iso(), now_iso())
            )

            with open(INDEX_CSV, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([RUN_MONTH, lic, name, url])

        con.commit()
        print(f"Page {page}/{total_pages}: indexed {len(items)}")

    con.close()
    print(f"\nDone. CSV saved to: {INDEX_CSV.resolve()}")


if __name__ == "__main__":
    main()