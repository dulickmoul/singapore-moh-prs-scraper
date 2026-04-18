from pathlib import Path
import pandas as pd

BASE_DIR = Path("output")
BASE_DIR.mkdir(exist_ok=True)

INPUT_FILE = BASE_DIR / "smc_full_index_az_repair.csv"
DUPLICATE_OUTPUT = BASE_DIR / "smc_duplicate_license_check.csv"


def main() -> None:
    df = pd.read_csv(INPUT_FILE, dtype=str).fillna("")
    df.columns = [c.strip() for c in df.columns]

    if "license" not in df.columns:
        raise ValueError(f"'license' column not found. Columns: {list(df.columns)}")

    duplicate_rows = df[df.duplicated(subset=["license"], keep=False)].copy()
    duplicate_rows = duplicate_rows.sort_values(by=["license"]).reset_index(drop=True)

    unique_count = df["license"].nunique(dropna=True)
    duplicate_license_count = duplicate_rows["license"].nunique(dropna=True)

    print("Total rows:", len(df))
    print("Unique licenses:", unique_count)
    print("Duplicate license groups:", duplicate_license_count)
    print("Duplicate rows:", len(duplicate_rows))

    if len(duplicate_rows) > 0:
        duplicate_rows.to_csv(DUPLICATE_OUTPUT, index=False, encoding="utf-8-sig")
        print(f"Saved duplicate review file to {DUPLICATE_OUTPUT}")
    else:
        print("No duplicate licenses found.")


if __name__ == "__main__":
    main()