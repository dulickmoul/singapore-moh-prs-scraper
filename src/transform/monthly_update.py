import csv
import re
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

RUN_MONTH = datetime.now().strftime("%Y-%m")

START_URL = "https://prs.moh.gov.sg/prs/internet/profSearch/mshowSearchSummaryByName.action?hpe=SMC"

GOTO_RE = re.compile(r"gotoPageDEFAULT\((\d+)\)")

OUT_DIR = Path("output")
OUT_DIR.mkdir(exist_ok=True)

DB_FILE = Path("smc_prs.db")
INDEX_CSV = OUT_DIR / "smc_full_index.csv"

PACING_PAGE_SEC = 1.2


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def db_connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_FILE)
    con.row_factory = sqlite3.Row
    return con


def init_db(con: sqlite3.Connection) -> None:
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


def ensure_csv_header() -> None:
    if not INDEX_CSV.exists():
        with INDEX_CSV.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["run_month", "license", "name", "detail_url"])


def wait_until_results_ready(driver, timeout: int = 300) -> None:
    def _ready(d):
        try:
            body_text = d.find_element(By.TAG_NAME, "body").text or ""
        except Exception:
            return False

        return (
            "Displaying" in body_text and "records" in body_text
        ) or ("View more details" in body_text)

    WebDriverWait(driver, timeout).until(_ready)


def get_total_pages(driver) -> int:
    last_links = driver.find_elements(By.XPATH, "//a[normalize-space()='Last']")
    if not last_links:
        return 1

    href = last_links[0].get_attribute("href") or ""
    m = GOTO_RE.search(href)
    return int(m.group(1)) if m else 1


def goto_page(driver, page: int, retries: int = 3) -> None:
    last_err = None

    for attempt in range(1, retries + 1):
        try:
            before = driver.find_element(By.TAG_NAME, "body").text
            driver.execute_script(f"gotoPageDEFAULT({page});")

            WebDriverWait(driver, 180).until(
                lambda d: d.find_element(By.TAG_NAME, "body").text != before
            )

            WebDriverWait(driver, 180).until(
                lambda d: "Displaying" in (d.find_element(By.TAG_NAME, "body").text or "")
            )

            time.sleep(1)
            return

        except Exception as e:
            last_err = e
            print(f"goto_page retry {attempt} failed for page {page}: {e}")
            time.sleep(3)

    raise last_err


def collect_items_from_current_page(driver) -> List[Tuple[str, str, str]]:
    items: List[Tuple[str, str, str]] = []
    seen = set()

    detail_links = driver.find_elements(
        By.XPATH,
        "//a[contains(normalize-space(.), 'View more details')]"
    )

    for link in detail_links:
        href = link.get_attribute("href") or ""
        if not href:
            continue

        if href.startswith("/"):
            href = "https://prs.moh.gov.sg" + href

        block_text = ""

        for xp in [
            "./ancestor::div[1]",
            "./ancestor::div[2]",
            "./ancestor::td[1]",
            "./ancestor::tr[1]",
        ]:
            try:
                txt = link.find_element(By.XPATH, xp).text.strip()
                if "View more details" in txt:
                    block_text = txt
                    break
            except Exception:
                pass

        if not block_text:
            continue

        lic_match = re.search(r"\((M\d{4,6}[A-Z])\)", block_text)
        if not lic_match:
            continue

        lic = lic_match.group(1)

        name_match = re.search(
            r"([A-Z][A-Z\s\.'\-]+)\s*\(" + re.escape(lic) + r"\)",
            block_text
        )

        name = (
            name_match.group(1).strip()
            if name_match
            else block_text.split("(" + lic + ")", 1)[0].strip()
        )

        key = (lic, href)
        if key in seen:
            continue

        seen.add(key)
        items.append((name, lic, href))

    return items


def main() -> None:
    con = db_connect()
    init_db(con)
    ensure_csv_header()

    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    try:
        driver.get(START_URL)

        print("\nManual step (required for CAPTCHA bypass):")
        print("1. Type A in the Name box")
        print("2. Tick Terms")
        print("3. Click Search")
        print("4. Solve reCAPTCHA if it appears")
        input("When results are visible, press Enter... ")

        wait_until_results_ready(driver)

        total_pages = get_total_pages(driver)
        print(f"Total pages for A: {total_pages}")

        with INDEX_CSV.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            for page in range(1, total_pages + 1):
                items = collect_items_from_current_page(driver)

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

                    writer.writerow([RUN_MONTH, lic, name, url])

                con.commit()

                print(f"Page {page}/{total_pages}: indexed {len(items)}")

                if page < total_pages:
                    time.sleep(PACING_PAGE_SEC)
                    goto_page(driver, page + 1)

        print("\nDone.")
        print(f"CSV: {INDEX_CSV.resolve()}")
        print(f"DB: {DB_FILE.resolve()}")

    finally:
        input("\nPress Enter to close browser...")
        driver.quit()
        con.close()


if __name__ == "__main__":
    main()