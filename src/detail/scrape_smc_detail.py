import os
import re
import csv
import json
import time
import random
from typing import Dict, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://prs.moh.gov.sg"
DETAIL_POST_URL = "https://prs.moh.gov.sg/prs/internet/profSearch/mgetSearchDetails.action"

INPUT_CSV = r"output\smc_unique_license.csv"
OUTPUT_CSV = r"output\smc_detail.csv"
OUTPUT_JSONL = r"output\smc_detail.jsonl"
FAILED_CSV = r"output\smc_detail_failed.csv"
STATE_FILE = r"output\smc_detail_state.json"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://prs.moh.gov.sg",
    "Referer": "https://prs.moh.gov.sg/prs/internet/profSearch/mgetSearchSummaryByName.action",
    "Connection": "keep-alive",
}

REQUEST_TIMEOUT = 40
MAX_RETRIES = 5
SLEEP_MIN = 0.8
SLEEP_MAX = 1.5
SAVE_EVERY = 20
RUN_MONTH = time.strftime("%Y-%m")

LICENSE_COL_CANDIDATES = ["license", "regNo", "registration_no", "license_no"]
NAME_COL_CANDIDATES = ["name", "doctor_name", "practitioner_name"]
LETTER_COL_CANDIDATES = ["query_letter", "letter", "search_letter"]
HPE_COL_CANDIDATES = ["hpe", "board", "council"]

KNOWN_LABELS = {
    "Qualifications": "qualifications",
    "Type of first registration / date": "type_of_first_registration_date",
    "Type of current registration / date": "type_of_current_registration_date",
    "Practising Certificate Start Date": "practising_certificate_start_date",
    "Practising Certificate End Date": "practising_certificate_end_date",
    "Type of Register: Medical Professional": "type_of_register_medical_professional",
    "Type of Register Medical Professional": "type_of_register_medical_professional",
    "Specialty / Entry date into the Register of Specialists": "specialty_entry_date_register_of_specialists",
    "Entry date into Register of Family Physicians": "entry_date_into_register_of_family_physicians",
    "Primary Place of Practice": "primary_place_of_practice",
    "Department / Name of Practice Place": "department_name_of_practice_place",
    "Address of Place of Practice": "address_of_place_of_practice",
    "Tel": "tel",
    "Fax": "fax",
    "Email": "email",
}


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text)).strip()


def ensure_output_dir():
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)


def detect_first_existing_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    lowered = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lowered:
            return lowered[cand.lower()]
    return None


def safe_get(row: pd.Series, col: Optional[str], default="") -> str:
    if not col:
        return default
    val = row.get(col, default)
    if pd.isna(val):
        return default
    return str(val)


def load_state() -> Dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "done_keys": [],
        "last_index": -1,
        "total_processed": 0,
        "total_success": 0,
        "total_failed": 0,
    }


def save_state(state: Dict):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def append_csv_rows(path: str, rows: List[Dict], fieldnames: List[str]):
    exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerows(rows)


def append_jsonl(path: str, rows: List[Dict]):
    with open(path, "a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def request_post_with_retry(session: requests.Session, url: str, data: Dict[str, str]) -> requests.Response:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.post(url, headers=HEADERS, data=data, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp
            last_err = Exception(f"HTTP {resp.status_code}")
        except Exception as e:
            last_err = e

        sleep_sec = min(10, attempt * 1.5) + random.uniform(0.2, 0.8)
        print(f"Retry {attempt}/{MAX_RETRIES} after error: {last_err}")
        time.sleep(sleep_sec)

    raise last_err


def page_contains_error(html: str) -> bool:
    text = html.lower()
    return (
        "the system encountered an error processing your request" in text
        or "please email us at prs_helpdesk" in text
    )


def extract_text_lines(soup: BeautifulSoup) -> List[str]:
    lines = []
    for s in soup.stripped_strings:
        t = normalize_space(s)
        if t:
            lines.append(t)
    return lines


def extract_header_name_and_license(lines: List[str]) -> Dict[str, str]:
    result = {
        "detail_name": "",
        "detail_license": "",
    }
    for line in lines[:30]:
        m = re.match(r"^(.*?)\s*\(([A-Za-z0-9]+)\)$", normalize_space(line))
        if m:
            result["detail_name"] = normalize_space(m.group(1))
            result["detail_license"] = normalize_space(m.group(2))
            return result
    return result


def parse_label_value_from_lines(lines: List[str]) -> Dict[str, str]:
    fields = {v: "" for v in KNOWN_LABELS.values()}
    filtered = [normalize_space(x) for x in lines if normalize_space(x)]

    i = 0
    while i < len(filtered):
        line = filtered[i]
        if line in KNOWN_LABELS:
            field = KNOWN_LABELS[line]
            values = []
            j = i + 1
            while j < len(filtered) and filtered[j] not in KNOWN_LABELS:
                values.append(filtered[j])
                j += 1
            fields[field] = " | ".join(values)
            i = j
        else:
            i += 1

    return fields


def split_address_block(address_text: str) -> Dict[str, str]:
    result = {
        "practice_address_full": address_text,
        "practice_postal_code": "",
    }
    if not address_text:
        return result

    m = re.search(r"Singapore\s+(\d{6})", address_text, flags=re.I)
    if m:
        result["practice_postal_code"] = m.group(1)
    return result


def parse_detail_page(html: str, final_url: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    lines = extract_text_lines(soup)

    parsed = {
        "final_detail_url": final_url,
        "page_title": normalize_space(soup.title.get_text(" ", strip=True)) if soup.title else "",
        "page_error_flag": "Y" if page_contains_error(html) else "N",
        "raw_text_snapshot": " | ".join(lines[:250]),
    }

    parsed.update(extract_header_name_and_license(lines))
    parsed.update(parse_label_value_from_lines(lines))
    parsed.update(split_address_block(parsed.get("address_of_place_of_practice", "")))

    return parsed


def build_payload(row: pd.Series, license_col: str, name_col: Optional[str], letter_col: Optional[str], hpe_col: Optional[str]) -> Dict[str, str]:
    reg_no = normalize_space(safe_get(row, license_col, ""))
    name_value = normalize_space(safe_get(row, name_col, ""))
    query_letter = normalize_space(safe_get(row, letter_col, "")) if letter_col else ""
    hpe_value = normalize_space(safe_get(row, hpe_col, "")) if hpe_col else "SMC"

    if not query_letter and name_value:
        query_letter = name_value[:1]

    payload = {
        "hpe": hpe_value or "SMC",
        "regNo": reg_no,
        "psearchParamVO.language": "eng",
        "psearchParamVO.searchBy": "N",
        "psearchParamVO.name": (query_letter or "").lower(),
        "psearchParamVO.pracPlaceName": "",
        "psearchParamVO.rbtnRegister": "all",
        "g-recaptcha-response": "",
        "psearchParamVO.regNo": "",
        "selectType": "all",
    }
    return payload


def main():
    ensure_output_dir()

    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(f"Input file not found: {INPUT_CSV}")

    df = pd.read_csv(INPUT_CSV, dtype=str).fillna("")
    print(f"Loaded input rows: {len(df)}")

    license_col = detect_first_existing_column(df, LICENSE_COL_CANDIDATES)
    name_col = detect_first_existing_column(df, NAME_COL_CANDIDATES)
    letter_col = detect_first_existing_column(df, LETTER_COL_CANDIDATES)
    hpe_col = detect_first_existing_column(df, HPE_COL_CANDIDATES)

    if not license_col:
        raise ValueError(f"Could not find a license column. Columns: {list(df.columns)}")

    print(f"Detected license column: {license_col}")
    print(f"Detected name column: {name_col}")
    print(f"Detected query letter column: {letter_col}")
    print(f"Detected hpe column: {hpe_col}")

    state = load_state()
    done_keys = set(state.get("done_keys", []))

    session = requests.Session()

    success_buffer = []
    failed_buffer = []
    dynamic_fields = set()

    if os.path.exists(OUTPUT_CSV):
        try:
            existing_df = pd.read_csv(OUTPUT_CSV, nrows=3)
            dynamic_fields.update(existing_df.columns.tolist())
        except Exception:
            pass

    total = len(df)
    processed_this_run = 0

    for idx, row in df.iterrows():
        license_value = normalize_space(safe_get(row, license_col, ""))
        record_key = f"LIC::{license_value}" if license_value else f"ROW::{idx}"

        if record_key in done_keys:
            continue

        name_value = normalize_space(safe_get(row, name_col, ""))

        payload = build_payload(row, license_col, name_col, letter_col, hpe_col)

        print(f"[{idx+1}/{total}] Posting detail for license={license_value}")

        try:
            resp = request_post_with_retry(session, DETAIL_POST_URL, payload)
            detail = parse_detail_page(resp.text, resp.url)

            out_row = {}
            for col in df.columns:
                out_row[col] = safe_get(row, col, "")

            out_row["run_month"] = RUN_MONTH
            out_row["scrape_status"] = "success"
            out_row["detail_post_url"] = DETAIL_POST_URL

            for k, v in payload.items():
                out_row[f"post_{k}"] = v

            for k, v in detail.items():
                out_row[k] = v

            success_buffer.append(out_row)
            dynamic_fields.update(out_row.keys())
            state["total_success"] += 1

        except Exception as e:
            failed_row = {
                "row_index": idx,
                "license": license_value,
                "name": name_value,
                "error": str(e),
            }
            failed_buffer.append(failed_row)
            state["total_failed"] += 1
            print(f"FAILED: {failed_row}")

        finally:
            state["total_processed"] += 1
            processed_this_run += 1
            done_keys.add(record_key)
            state["done_keys"] = list(done_keys)
            state["last_index"] = idx

        if len(success_buffer) >= SAVE_EVERY:
            fieldnames = sorted(dynamic_fields)
            append_csv_rows(OUTPUT_CSV, success_buffer, fieldnames)
            append_jsonl(OUTPUT_JSONL, success_buffer)
            success_buffer = []

            if failed_buffer:
                append_csv_rows(
                    FAILED_CSV,
                    failed_buffer,
                    ["row_index", "license", "name", "error"]
                )
                failed_buffer = []

            save_state(state)
            print(
                f"Checkpoint saved. processed={state['total_processed']} "
                f"success={state['total_success']} failed={state['total_failed']}"
            )

        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    if success_buffer:
        fieldnames = sorted(dynamic_fields)
        append_csv_rows(OUTPUT_CSV, success_buffer, fieldnames)
        append_jsonl(OUTPUT_JSONL, success_buffer)

    if failed_buffer:
        append_csv_rows(
            FAILED_CSV,
            failed_buffer,
            ["row_index", "license", "name", "error"]
        )

    save_state(state)

    print("\nDone detail scraping.")
    print(f"Processed this run: {processed_this_run}")
    print(f"Total processed: {state['total_processed']}")
    print(f"Total success: {state['total_success']}")
    print(f"Total failed: {state['total_failed']}")
    print(f"Detail CSV: {OUTPUT_CSV}")
    print(f"Detail JSONL: {OUTPUT_JSONL}")
    print(f"Failed CSV: {FAILED_CSV}")
    print(f"State file: {STATE_FILE}")


if __name__ == "__main__":
    main()