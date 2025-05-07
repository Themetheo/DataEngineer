import pandas as pd
import json
import re
import logging
from rapidfuzz import process, fuzz
from tqdm import tqdm

# ---------------------- LOGGING SETUP ----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("matcher.log", encoding="utf-8")
    ]
)

# ---------------------- STEP 1: LOAD FILES ----------------------
SAMPLE_DATA_CSV = "C:\Users\ASUS\Desktop\Dataprojrct\modules\Menumatcher\Data_menumatcher\sample_data.csv"
MENU_MAPPING_JSON = "C:\Users\ASUS\Desktop\Dataprojrct\modules\Menumatcher\Data_menumatcher\menu_mapping.json"
CODEMENU_JSON = "C:\Users\ASUS\Desktop\Dataprojrct\modules\Menumatcher\Data_menumatcher\codemenu.json"

sample_df = pd.read_csv(SAMPLE_DATA_CSV)
with open(MENU_MAPPING_JSON, "r", encoding="utf-8") as f:
    menu_dict = json.load(f)
with open(CODEMENU_JSON, "r", encoding="utf-8") as f:
    codemenu = json.load(f)

# ---------------------- STEP 2: BUILD REVERSE MAPPING ----------------------
name_to_code = {v: k for k, v in menu_dict.items() if isinstance(v, str)}
manual_rules = {v.replace("ธรรมดา", "").strip(): v for v in name_to_code if v.endswith("ธรรมดา")}

# ---------------------- STEP 3: TEXT CLEANING ----------------------
def clean_text(text):
    if pd.isna(text):
        return ""
    text = re.sub(r"x\s*\d+", "", text)
    text = re.sub(r"[!""'.,()\[\]]", "", text)
    text = re.sub(r"(กล่อง|ถุง|แถมฟรี)", "", text)
    text = re.sub(r"\s+", "", text)
    return text.strip()

# ---------------------- STEP 4: EXTRACT KEYWORDS FROM CODEMENU ----------------------
def extract_keywords(text):
    tokens = re.findall(r'[ก-๙]+', str(text))
    keywords = []
    for token in tokens:
        for word in codemenu.values():
            if word in token:
                keywords.append(word)
    return ''.join(keywords)

# ---------------------- STEP 5: PREPARE CLEANED MAPPING ----------------------
cleaned_menu_map = {
    clean_text(name): (code, name) for code, name in menu_dict.items() if isinstance(name, str)
}

# ---------------------- STEP 6: MATCHING FUNCTION ----------------------
def match_menu(raw_text):
    if pd.isna(raw_text):
        return "", ""

    cleaned = clean_text(raw_text)
    keyworded = extract_keywords(raw_text)
    logging.info(f"Matching: {raw_text} → Cleaned: {cleaned} | Keywords: {keyworded}")

    if cleaned in manual_rules:
        std = manual_rules[cleaned]
        code = name_to_code.get(std, "")
        logging.info(f"✅ Manual rule match → {std} ({code})")
        return std, code

    if cleaned in cleaned_menu_map:
        code, std = cleaned_menu_map[cleaned]
        logging.info(f"✅ Exact match with cleaned → {std} ({code})")
        return std, code

    if keyworded in cleaned_menu_map:
        code, std = cleaned_menu_map[keyworded]
        logging.info(f"✅ Exact keyword match → {std} ({code})")
        return std, code

    match = process.extractOne(keyworded, cleaned_menu_map.keys(), scorer=fuzz.WRatio, score_cutoff=70)
    if match:
        code, std = cleaned_menu_map[match[0]]
        logging.info(f"✅ Fuzzy matched → {std} ({code}) from keyword: {keyworded}")
        return std, code

    logging.warning(f"❌ Failed to match: {raw_text}")
    return "", ""

# ---------------------- STEP 7: APPLY TO DATA ----------------------
sample_df["Cleaned"] = sample_df["Menu"].apply(clean_text)
sample_df["Keywords"] = sample_df["Menu"].apply(extract_keywords)

std_menu_result = []
product_id_result = []

for text in tqdm(sample_df["Menu"], desc="Matching Menus"):
    std, pid = match_menu(text)
    std_menu_result.append(std)
    product_id_result.append(pid)

sample_df["std_menu"] = std_menu_result
sample_df["Product_ID"] = product_id_result

# ---------------------- STEP 8: EXPORT ----------------------
sample_df.to_csv("sample_data_matched_result.csv", index=False)
print("✅ แมตช์เสร็จแล้ว บันทึกเป็น sample_data_matched_result.csv")
logging.info("✅ Matching complete and exported to sample_data_matched_result.csv")
