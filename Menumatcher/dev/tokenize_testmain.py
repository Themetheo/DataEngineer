import os
import pandas as pd
import re
import json
from typing import Dict, Any, Optional, Tuple, List
from rapidfuzz import process, fuzz
from tqdm import tqdm
from datetime import datetime
from pythainlp.tokenize import word_tokenize

# 📌 ใช้ logger แบบง่าย (แทน setup_logger จริง)
def setup_logger(name):
    import logging
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

class MenuMatcher:
    def __init__(self, sample_df: pd.DataFrame, menu_dict: Dict[str, Any], codemenu: Dict[str, str],
                 module_name: str = "menu_matcher", tokenizer_engine: str = "newmm"):
        self.logger = setup_logger(module_name)
        self.sample_df = sample_df
        self.menu_dict = menu_dict
        self.codemenu = codemenu
        self.tokenizer_engine = tokenizer_engine

        self.name_to_code: Dict[str, str] = {v: k for k, v in self.menu_dict.items() if isinstance(v, str)}
        self.manual_rules: Dict[str, str] = {
            v.replace("ธรรมดา", "").strip(): v for v in self.name_to_code if v.endswith("ธรรมด")
        }
        self.cleaned_menu_map: Dict[str, Tuple[str, str]] = {
            self.clean_text(name): (code, name) for code, name in self.menu_dict.items() if isinstance(name, str)
        }
        self.match_result_log: List[Dict[str, Any]] = []

    def clean_text(self, text: str) -> str:
        if pd.isna(text):
            return ""
        text = re.sub(r"x\\s*\\d+", "", text)
        text = re.sub(r"[!\\\"',.\\[\\]()]+", "", text)
        text = re.sub(r"(กล่อง|ถุง|แถมฟรี|แถม)", "", text)
        text = re.sub(r"\\s+", "", text)
        return text.strip()

    def extract_keywords(self, text: str) -> Tuple[str, List[str]]:
        tokens = word_tokenize(str(text), engine=self.tokenizer_engine)
        keywords = [word for word in tokens if word in self.codemenu.values()]
        return ''.join(keywords), tokens

    def match_menu_verbose(self, raw_text: str) -> Tuple[str, str, str, Optional[int], str, str, List[str]]:
        if pd.isna(raw_text):
            return "", "", "unmatched", None, "", "", []

        if isinstance(raw_text, str) and "แถม" in raw_text:
            return "", "", "skipped", None, raw_text, "", []

        cleaned = self.clean_text(raw_text)
        keyworded, tokens = self.extract_keywords(raw_text)

        if cleaned in self.manual_rules:
            std = self.manual_rules[cleaned]
            return std, self.name_to_code.get(std, ""), "manual", None, cleaned, keyworded, tokens

        if cleaned in self.cleaned_menu_map:
            code, std = self.cleaned_menu_map[cleaned]
            return std, code, "exact", None, cleaned, keyworded, tokens

        if keyworded in self.cleaned_menu_map:
            code, std = self.cleaned_menu_map[keyworded]
            return std, code, "keyword", None, cleaned, keyworded, tokens

        match = process.extractOne(keyworded, self.cleaned_menu_map.keys(), scorer=fuzz.WRatio, score_cutoff=50)
        if match:
            matched_text = match[0]
            score = match[1]
            code, std = self.cleaned_menu_map[matched_text]
            return std, code, "fuzzy", score, cleaned, keyworded, tokens

        self.logger.warning(f"❌ Failed to match: {raw_text}")
        return "", "", "unmatched", None, cleaned, keyworded, tokens

    def match_all(self, output_dir_data: str = r"Output/data", output_dir_detail: str = r"Output/result") -> None:
        os.makedirs(output_dir_data, exist_ok=True)
        os.makedirs(output_dir_detail, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(output_dir_data, f"sample_data_matched_{timestamp}.csv")
        detail_path = os.path.join(output_dir_detail, f"match_detail_result_{timestamp}.csv")

        std_menu_result: List[str] = []
        product_id_result: List[str] = []
        tokenized_list_result: List[str] = []

        for text in tqdm(self.sample_df["Menu"], desc="Matching Menus"):
            std, pid, match_type, score, cleaned, keyworded, tokens = self.match_menu_verbose(text)

            self.match_result_log.append({
                "raw_text": text,
                "cleaned_text": cleaned,
                "keyworded_text": keyworded,
                "match_type": match_type,
                "matched_name": std,
                "product_id": pid,
                "score": score,
                "tokenized_list": tokens
            })

            std_menu_result.append(std)
            product_id_result.append(pid)
            tokenized_list_result.append(str(tokens))  # 👉 ดูง่ายใน Excel

        self.sample_df["std_menu"] = std_menu_result
        self.sample_df["Product_ID"] = product_id_result
        self.sample_df["Tokenized"] = tokenized_list_result

        self.sample_df.to_csv(output_path, index=False)
        pd.DataFrame(self.match_result_log).to_csv(detail_path, index=False)

        self.logger.info(f"✅ Matching complete → saved: {output_path}, {detail_path}")
        print(f"✅ recorded: {output_path}, {detail_path}")

# load JSON จาก Data_menumatcher
if __name__ == "__main__":
    # Load menu_dict and codemenu from Data_menumatcher/
    with open(r"C:\Users\ASUS\Desktop\Dataprojrct\Menumatcher\Data_menumatcher\menu_mapping.json", encoding="utf-8") as f:
        menu_dict = json.load(f)
    with open(r"C:\Users\ASUS\Desktop\Dataprojrct\Menumatcher\Data_menumatcher\codemenu.json", encoding="utf-8") as f:
        codemenu = json.load(f)

    # sample data for matching
    sample_df = pd.read_csv(r"C:\Users\ASUS\Desktop\Dataprojrct\mockdata\mock1.csv")
    matcher = MenuMatcher(sample_df, menu_dict, codemenu)
    matcher.match_all()
