# 🇸🇬 Singapore MOH PRS Scraper (Healthcare Registry Pipeline)

## Overview

This project builds a full data pipeline to extract publicly available healthcare practitioner data from the Singapore Ministry of Health (MOH) PRS system.

It is not a simple scraper. The pipeline handles:

* Alphabet-based indexing (A–Z)
* Pagination and unstable page handling
* Session-based scraping (Selenium + requests hybrid)
* Deduplication by license
* Detail profile extraction
* Data transformation into structured format

The final output is a clean, analysis-ready dataset of medical professionals.

---

## 🧠 Key Features

* Hybrid scraping approach (Selenium + requests)
* Handles pagination across large datasets
* Retry logic for weak / incomplete pages
* Checkpoint / resume capability (state file)
* SQLite snapshot for tracking records
* Deduplication by license number
* Separate detail scraping pipeline
* Clean structured CSV output
* JSONL export for debugging and scaling
* Supports multi-affiliation handling (multiple practice locations per practitioner)

---

## ⚙️ Pipeline Architecture

### Step 1 — Index Scraping (A–Z)

* Bootstrap session manually (solve CAPTCHA once)
* Extract all practitioners by alphabet
* Handle pagination via cookies + POST requests

Scripts:

* `hybrid_a_index.py`
* `hybrid_az_index.py`

---

### Step 2 — Repair Weak Pages

* Detect pages with missing or incomplete records
* Retry multiple times
* Log page quality and retries

Script:

* `hybrid_az_index_repair.py`

---

### Step 3 — Deduplication

* Merge A and A–Z outputs
* Remove duplicates based on license number

Scripts:

* `dedup_smc.py`
* `merge_and_dedupe.py`

---

### Step 4 — Detail Scraping

* Fetch practitioner profiles by license number
* Extract structured fields from HTML
* Handle request failures with retry logic
* Save intermediate results (CSV + JSONL)

Script:

* `scrape_smc_detail.py`

---

### Step 5 — Data Transformation

* Clean and normalize fields
* Split specialty and dates
* Map raw fields into flat structure

Script:

* `transform_smc_detail.py`

---

## 🧱 Project Structure

```
singapore-moh-prs-scraper/
├── src/
│   ├── index/
│   ├── detail/
│   ├── transform/
│   └── init_db.py
├── sample_data/
├── README.md
├── requirements.txt
└── .gitignore
```

---

## ▶️ How to Run

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

---

### 2. Run index scraping

```bash
python src/index/hybrid_az_index.py
```

---

### 3. Repair weak pages

```bash
python src/index/hybrid_az_index_repair.py
```

---

### 4. Deduplicate records

```bash
python src/transform/merge_and_dedupe.py
```

---

### 5. Scrape detail profiles

```bash
python src/detail/scrape_smc_detail.py
```

---

### 6. Transform output

```bash
python src/transform/transform_smc_detail.py
```

---

## 📊 Sample Data

A small sample dataset is included in the `sample_data/` folder.

This allows users to:

* Understand the output format
* Explore the data structure
* Validate the transformation logic

The full dataset is not included due to size and data handling considerations.

---

## 🚀 Why This Project Matters

This project demonstrates:

* Real-world scraping of a government healthcare registry
* Handling unstable pages and partial failures
* Building a multi-step data pipeline
* Data cleaning, normalization, and deduplication
* Production-style logic (retry, checkpoint, logging)

It reflects practical experience in healthcare data extraction and processing.

---

## ⚠️ Challenges Solved

* Handling CAPTCHA-protected entry point
* Extracting data from inconsistent HTML structure
* Matching names and licenses reliably
* Preventing duplicate records across A–Z search
* Recovering from partial or failed page loads
* Scaling scraping with checkpoint/resume logic

---

## 📌 Notes

* This project uses only publicly available data.
* Designed for educational and portfolio purposes.
* Please review the target website’s terms of use before running at scale.

---

## 👤 Author

Built by Dulick - a Data Matching Lead at Veeva with hands-on experience in healthcare data across Vietnam, Pakistan, and Singapore.
