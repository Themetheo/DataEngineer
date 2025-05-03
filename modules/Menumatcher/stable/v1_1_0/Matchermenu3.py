# -*- coding: utf-8 -*-

import pandas as pd
import json
import re
import os
from rapidfuzz import process, fuzz
from tqdm import tqdm
from utlis.utlis import setup_logger
from typing import Tuple, List, Optional

class FoodMatcher:
    def __init__(self, sample_file: str, menu_mapping_file: str, codemenu_file: str, output_folder: str = "output", selected_columns: Optional[List[str]] = None) -> None:
        self.logger = setup_logger("FoodMatcher", encoding="utf-8")
        self.sample_file = sample_file
        self.menu_mapping_file = menu_mapping_file
        self.codemenu_file = codemenu_file
        self.output_folder = output_folder
        self.selected_columns = selected_columns

        self.full_sample_df = None
        self.sample_df = None
        self.menu_dict = None
        self.codemenu = None
        self.name_to_code = None
        self.manual_rules = None
        self.cleaned_menu_map = None
        self.long_texts_df = pd.DataFrame()

        self.load_files()
        self.build_reverse_mapping()

    def load_files(self) -> None:
        self.logger.info("Loading input files...")
        self.full_sample_df = pd.read_csv(self.sample_file)
        self.sample_df = self.full_sample_df.copy()
        if self.selected_columns:
            missing_cols = [col for col in self.selected_columns if col not in self.sample_df.columns]
            if missing_cols:
                raise ValueError(f"Selected columns not found in file: {missing_cols}")
            self.logger.info(f"Selecting columns: {self.selected_columns}")
            self.sample_df = self.sample_df[self.selected_columns]
        with open(self.menu_mapping_file, "r", encoding="utf-8") as f:
            self.menu_dict = json.load(f)
        with open(self.codemenu_file, "r", encoding="utf-8") as f:
            self.codemenu = json.load(f)
        self.logger.info("Files loaded successfully.")

    def build_reverse_mapping(self) -> None:
        self.logger.info("Building reverse mapping and manual rules...")
        self.name_to_code = {v: k for k, v in self.menu_dict.items() if isinstance(v, str)}
        self.manual_rules = {v.replace("ธรรมดา", "").strip(): v for v in self.name_to_code if v.endswith("ธรรมดา")}
        self.cleaned_menu_map = {
            self.clean_text(name): (code, name)
            for code, name in self.menu_dict.items()
            if isinstance(name, str)
        }
        self.logger.info(f"Mapping complete: {len(self.cleaned_menu_map)} items")

    @staticmethod
    def clean_text(text: str) -> str:
        if pd.isna(text):
            return ""
        text = re.sub(r"x\s*\d+", "", text)
        text = re.sub(r"[!\"'.,()\[\]]", "", text)
        text = re.sub(r"(กล่อง|ถุง|แถมฟรี)", "", text)
        text = re.sub(r"\s+", "", text)
        return text.strip()

    def extract_keywords(self, text: str) -> str:
        tokens = re.findall(r"[ก-๙]+", str(text))
        keywords = []
        for token in tokens:
            for word in self.codemenu.values():
                if word in token:
                    keywords.append(word)
        return ''.join(keywords)

    def match_menu(self, raw_text: str, row_data: pd.Series) -> Tuple[str, str]:
        if pd.isna(raw_text) or raw_text.strip() == "":
            self.logger.warning("Empty or NaN text encountered during matching.")
            return "", ""

        if len(raw_text) > 20:
            self.logger.warning(f"Text too long to match (>20 chars): {raw_text}")
            print(f"⚠️ Text too long to match (>20 chars): {raw_text}")
            self.long_texts_df = pd.concat([self.long_texts_df, row_data.to_frame().T], ignore_index=True)
            return "", ""

        cleaned = self.clean_text(raw_text)
        keyworded = self.extract_keywords(raw_text)
        self.logger.info(f"Matching: {raw_text} -> Cleaned: {cleaned} | Keywords: {keyworded}")

        if cleaned in self.manual_rules:
            std = self.manual_rules[cleaned]
            code = self.name_to_code.get(std, "")
            self.logger.info(f"✅ Manual matched: {std} ({code})")
            return std, code

        if cleaned in self.cleaned_menu_map:
            code, std = self.cleaned_menu_map[cleaned]
            self.logger.info(f"✅ Exact cleaned matched: {std} ({code})")
            return std, code

        if keyworded in self.cleaned_menu_map:
            code, std = self.cleaned_menu_map[keyworded]
            self.logger.info(f"✅ Exact keyword matched: {std} ({code})")
            return std, code

        match = process.extractOne(keyworded, self.cleaned_menu_map.keys(), scorer=fuzz.WRatio, score_cutoff=70)
        if match:
            code, std = self.cleaned_menu_map[match[0]]
            self.logger.info(f"✅ Fuzzy matched: {std} ({code})")
            return std, code

        self.logger.warning(f"❌ Failed to match: {raw_text}")
        return "", ""

    def process_matching(self) -> "FoodMatcher":
        self.logger.info("Starting matching process...")
        std_menu_result = []
        product_id_result = []

        for idx, row in tqdm(self.sample_df.iterrows(), total=len(self.sample_df), desc="Matching Menus"):
            menu_name = row.get("ชื่อเมนู", None)

            if pd.isna(menu_name) or str(menu_name).strip() == "":
                self.logger.warning(f"⚠️ Row {idx} missing 'ชื่อเมนู'. Skipping.")
                std_menu_result.append("")
                product_id_result.append("")
                continue

            std, pid = self.match_menu(menu_name, row)
            std_menu_result.append(std)
            product_id_result.append(pid)

        self.full_sample_df["std_menu"] = std_menu_result
        self.full_sample_df["Product_ID"] = product_id_result

        if not self.long_texts_df.empty:
            long_texts_path = os.path.join(self.output_folder, "long_texts.csv")
            if not os.path.exists(self.output_folder):
                os.makedirs(self.output_folder)
            self.long_texts_df.to_csv(long_texts_path, index=False)
            self.logger.info(f"⚠️ Exported long texts to {long_texts_path}")
            print(f"⚠️ Exported long texts to {long_texts_path}")

        self.logger.info("Matching process completed.")
        return self

    def export_result(self, filename: str = "matched_sample_data.csv") -> "FoodMatcher":
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
        output_path = os.path.join(self.output_folder, filename)
        self.full_sample_df.to_csv(output_path, index=False)
        self.logger.info(f"✅ Output saved to {output_path}")
        print(f"✅ แมตช์เสร็จแล้ว บันทึกเป็น {output_path}")
        return self