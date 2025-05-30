import os
import pandas as pd
import re
from typing import Dict, Any, Optional, Tuple, List
from rapidfuzz import process, fuzz
from tqdm import tqdm  # type: ignore
from datetime import datetime
from pythainlp.tokenize import word_tokenize
from utlis import setup_logger

class MenuMatcher:
    def __init__(
        self,
        sample_df: pd.DataFrame,
        menu_dict: Dict[str, Any],
        codemenu: Dict[str, str],
        module_name: str = "menu_matcher",
        tokenizer_engine: str = "newmm"
    ):
        self.logger = setup_logger(module_name)
        self.sample_df = sample_df
        self.menu_dict = menu_dict
        self.codemenu = codemenu
        self.tokenizer_engine = tokenizer_engine

        self.name_to_code: Dict[str, str] = {
            v: k for k, v in self.menu_dict.items() if isinstance(v, str)
        }
        self.manual_rules: Dict[str, str] = {
            v.replace("ธรรมดา", "").strip(): v for v in self.name_to_code if v.endswith("ธรรมด")
        }
        self.cleaned_menu_map: Dict[str, Tuple[str, str]] = {
            self.clean_text(name): (code, name)
            for code, name in self.menu_dict.items() if isinstance(name, str)
        }

        print(f"📦 LOADED menu_dict: {list(self.menu_dict.items())[:3]}")
        print(f"🧠 CLEANED_MAP KEYS: {list(self.cleaned_menu_map.keys())[:5]}")

        self.match_result_log: List[Dict[str, Any]] = []

    def clean_text(self, text: str) -> str:
        if pd.isna(text):
            return ""
        text = re.sub(r"x\s*\d+", "", text)
        text = re.sub(r"[!\"',.\[\]()]+", "", text)
        text = re.sub(r"(กล่อง|ถุง|แถมฟรี|แถม)", "", text)
        text = re.sub(r"\s+", "", text)
        return text.strip()

    def extract_keywords(self, text: str) -> Tuple[str, List[str]]:
        tokens = word_tokenize(str(text), engine=self.tokenizer_engine)
        keywords = [word for word in tokens if word in self.codemenu.values()]
        return ''.join(keywords), tokens

    def match_menu_verbose(self, raw_text: str) -> Tuple[str, str, str, Optional[int], str, str, List[str]]:
        if pd.isna(raw_text):
            print("❗ raw_text is NaN")
            return "", "", "unmatched", None, "", "", []

        if isinstance(raw_text, str) and "แถม" in raw_text:
            print(f"⛔ Skipped (แถม): {raw_text}")
            return "", "", "skipped", None, raw_text, "", []

        cleaned = self.clean_text(raw_text)
        keyworded, tokens = self.extract_keywords(raw_text)

        print("=" * 50)
        print(f"🔍 RAW: {raw_text}")
        print(f"🧹 CLEANED: {cleaned}")
        print(f"🔑 KEYWORDS: {keyworded}")

        if cleaned in self.manual_rules:
            std = self.manual_rules[cleaned]
            print(f"✅ Manual match → {std}")
            return std, self.name_to_code.get(std, ""), "manual", None, cleaned, keyworded, tokens

        if cleaned in self.cleaned_menu_map:
            code, std = self.cleaned_menu_map[cleaned]
            print(f"✅ Exact match → {std}")
            return std, code, "exact", None, cleaned, keyworded, tokens

        if keyworded in self.cleaned_menu_map:
            code, std = self.cleaned_menu_map[keyworded]
            print(f"✅ Keyword match → {std}")
            return std, code, "keyword", None, cleaned, keyworded, tokens

        match = process.extractOne(keyworded, self.cleaned_menu_map.keys(), scorer=fuzz.WRatio, score_cutoff=50)
        if match:
            matched_text = match[0]
            score = match[1]
            code, std = self.cleaned_menu_map[matched_text]
            print(f"✅ Fuzzy match → {std} | score: {score}")
            return std, code, "fuzzy", score, cleaned, keyworded, tokens

        print("❌ NO MATCH FOUND")
        self.logger.warning(f"❌ Failed to match: {raw_text}")
        return "", "", "unmatched", None, cleaned, keyworded, tokens

    def match_all(self, output_dir_data: str = r"modules\Menumatcher\Output\data",
                  output_dir_detail: str = r"modules\Menumatcher\Output\result") -> None:
        os.makedirs(output_dir_data, exist_ok=True)
        os.makedirs(output_dir_detail, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(output_dir_data, f"sample_data_matched_{timestamp}.csv")
        detail_path = os.path.join(output_dir_detail, f"match_detail_result_{timestamp}.csv")

        std_menu_result: List[str] = []
        product_id_result: List[str] = []

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

        self.sample_df["std_menu"] = std_menu_result
        self.sample_df["Product_ID"] = product_id_result

        self.sample_df.to_csv(output_path, index=False)
        pd.DataFrame(self.match_result_log).to_csv(detail_path, index=False)

        self.logger.info(f"✅ Matching complete → saved: {output_path}, {detail_path}")
        print(f"✅ บันทึกผลลัพธ์แล้ว: {output_path}, {detail_path}")
