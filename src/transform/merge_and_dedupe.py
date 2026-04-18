from pathlib import Path
import pandas as pd

BASE_DIR = Path("output")
BASE_DIR.mkdir(exist_ok=True)

A_FILE = BASE_DIR / "smc_full_index_A.csv"
BZ_FILE = BASE_DIR / "smc_full_index_AZ.csv"

RAW_OUTPUT = BASE_DIR / "smc_full_index_all_raw.csv"
CLEAN_OUTPUT = BASE_DIR / "smc_registry_clean.csv"


def main() -> None:
    a = pd.read_csv(A_FILE, dtype=str).fillna("")
    bz = pd.read_csv(BZ_FILE, dtype=str).fillna("")

    combined = pd.concat([a, bz], ignore_index=True)

    # Normalize column names just in case
    combined.columns = [c.strip() for c in combined.columns]

    # Deduplicate by license
    deduped = combined.drop_duplicates(subset=["license"], keep="first")

    print("A rows:", len(a))
    print("B-Z rows:", len(bz))
    print("Combined rows:", len(combined))
    print("Unique licenses:", len(deduped))

    combined.to_csv(RAW_OUTPUT, index=False, encoding="utf-8-sig")
    deduped.to_csv(CLEAN_OUTPUT, index=False, encoding="utf-8-sig")

    print(f"Saved raw combined file to {RAW_OUTPUT}")
    print(f"Saved deduped file to {CLEAN_OUTPUT}")


if __name__ == "__main__":
    main()