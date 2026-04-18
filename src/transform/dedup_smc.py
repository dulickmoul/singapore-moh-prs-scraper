from pathlib import Path
import pandas as pd

BASE_DIR = Path("output")

INPUT_FILE = BASE_DIR / "smc_full_index_AZ_repair.csv"
OUTPUT_FILE = BASE_DIR / "smc_unique_license.csv"

df = pd.read_csv(INPUT_FILE)

print("Raw rows:", len(df))

df_unique = df.drop_duplicates(subset=["license"])

print("Unique licenses:", len(df_unique))

df_unique.to_csv(OUTPUT_FILE, index=False)

print("Saved:", OUTPUT_FILE)