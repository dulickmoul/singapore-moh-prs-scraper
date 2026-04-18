from pathlib import Path
import pandas as pd
import re

BASE_DIR = Path("output")
INPUT_FILE = BASE_DIR / "smc_detail.csv"
OUTPUT_FILE = BASE_DIR / "smc_detail_mapped.csv"

df = pd.read_csv(INPUT_FILE, dtype=str).fillna("")

def clean_text(x):
    return re.sub(r"\s+", " ", str(x)).strip()

def split_specialty_and_date(text):
    text = clean_text(text)
    if not text:
        return "", ""
    m = re.match(r"^(.*?)\s*\((\d{2}/\d{2}/\d{4})\)$", text)
    if m:
        return clean_text(m.group(1)), clean_text(m.group(2))
    return text, ""

out = pd.DataFrame()

out["Full_name"] = df.get("name", "").apply(clean_text)
out["Reg_number"] = df.get("license", "").apply(clean_text)

specialty_split = df.get(
    "specialty_entry_date_register_of_specialists", ""
).apply(split_specialty_and_date)

out["Specialty"] = specialty_split.apply(lambda x: x[0])
out["Specialty_Entry_Date"] = specialty_split.apply(lambda x: x[1])

out["Specialty_2"] = ""
out["Specialty_3"] = ""
out["Specialty_4"] = ""
out["Specialty_5"] = ""

out["Entry_date_family_physician_register"] = df.get(
    "entry_date_into_register_of_family_physicians", ""
).apply(clean_text)

out["MedicalDegrees"] = df.get("qualifications", "").apply(clean_text)

out["Practicing_cert_start_date"] = df.get(
    "practising_certificate_start_date", ""
).apply(clean_text)

out["Practicing_cert_end_date"] = df.get(
    "practising_certificate_end_date", ""
).apply(clean_text)

out["ParentHCO_Name"] = df.get(
    "department_name_of_practice_place", ""
).apply(clean_text)

out["ParentHCO_Address"] = df.get(
    "address_of_place_of_practice", ""
).apply(clean_text)

for i in range(2, 8):
    out[f"ParentHCO_Name_{i}"] = ""
    out[f"ParentHCO_Address_{i}"] = ""

out["Primary_place_of_practice"] = df.get(
    "primary_place_of_practice", ""
).apply(clean_text)
out["Postal_Code"] = df.get("practice_postal_code", "").apply(clean_text)
out["Tel"] = df.get("tel", "").apply(clean_text)
out["Email"] = df.get("email", "").apply(clean_text)
out["Fax"] = df.get("fax", "").apply(clean_text)
out["Type_of_first_registration_date"] = df.get(
    "type_of_first_registration_date", ""
).apply(clean_text)
out["Type_of_current_registration_date"] = df.get(
    "type_of_current_registration_date", ""
).apply(clean_text)
out["Type_of_register_medical_professional"] = df.get(
    "type_of_register_medical_professional", ""
).apply(clean_text)
out["details"] = df.get("raw_text_snapshot", "").apply(clean_text)
out["final_detail_url"] = df.get("final_detail_url", "").apply(clean_text)
out["page_error_flag"] = df.get("page_error_flag", "").apply(clean_text)

out.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
print(f"Saved: {OUTPUT_FILE}")
print(f"Rows: {len(out)}")