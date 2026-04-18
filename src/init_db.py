import sqlite3

DB = "smc_prs.db"

schema = """
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

CREATE TABLE IF NOT EXISTS profiles (
  license TEXT PRIMARY KEY,
  name TEXT,
  detail_url TEXT,
  qualifications TEXT,
  first_registration TEXT,
  current_registration TEXT,
  pc_start TEXT,
  pc_end TEXT,
  type_of_register TEXT,
  specialist_entry TEXT,
  family_physician_entry TEXT,
  department_or_place_name TEXT,
  address TEXT,
  tel TEXT,
  map_url TEXT,
  profile_hash TEXT,
  first_seen TEXT,
  last_seen TEXT,
  last_changed TEXT
);

CREATE TABLE IF NOT EXISTS runs (
  run_month TEXT PRIMARY KEY,
  started_at TEXT,
  finished_at TEXT,
  note TEXT,
  total_indexed INTEGER,
  new_licenses INTEGER,
  updated_profiles INTEGER
);
"""

with sqlite3.connect(DB) as con:
    con.executescript(schema)

print(f"Initialized {DB}")