import pandas as pd
import json
import os
from rapidfuzz import process, fuzz
from tqdm import tqdm # type: ignore
from utlis.utlis import setup_logger
from typing import Tuple, List, Optional, Union

class CleanData:
    def __init__(self, data: Union[str, pd.DataFrame], config_path: Optional[str] = None, soft_warning: bool = False) -> None:
        self.logger = setup_logger("CleanData")
        if isinstance(data, str):
            self.df = pd.read_csv(data)
            self.logger.info(f"Loaded CSV file: {data}")
        elif isinstance(data, pd.DataFrame):
            self.df = data.copy()
            self.logger.info("Initialized from DataFrame")
        else:
            raise ValueError("Data must be a filepath or DataFrame")

        self.config_path = config_path
        self.soft_warning = soft_warning

    def validate_string_column(self, column: str) -> None:
        if column not in self.df.columns:
            if self.soft_warning:
                self.logger.warning(f"⚠️ Column '{column}' not found in dataframe. Proceeding with caution.")
            else:
                raise ValueError(f"Column '{column}' not found in dataframe")
        elif not pd.api.types.is_string_dtype(self.df[column]):
            if self.soft_warning:
                self.logger.warning(f"⚠️ Column '{column}' is not string type, found {self.df[column].dtype}. Proceeding with caution.")
            else:
                raise TypeError(f"Column '{column}' must be string type, but found {self.df[column].dtype} instead.")
        else:
            self.logger.info(f"✅ Column '{column}' validated as string type.")

    def rename_columns(self, rename_dict: Optional[dict] = None) -> "CleanData":
        if rename_dict is None and self.config_path:
            with open(self.config_path, "r", encoding="utf-8") as f:
                rename_dict = json.load(f)
        if rename_dict:
            self.df.rename(columns=rename_dict, inplace=True)
            self.logger.info("Renamed columns successfully")
        else:
            self.logger.warning("No rename dict provided, skipping.")
        return self

    def drop_total_rows(self, column: str = "Date", total_value: str = "Total") -> "CleanData":
        if column in self.df.columns:
            self.df = self.df[self.df[column] != total_value]
            self.logger.info(f"Dropped rows where {column} == {total_value}")
        else:
            self.logger.warning(f"Column '{column}' not found.")
        return self

    def dropna_column(self, column: str = "Menu") -> "CleanData":
        self.validate_string_column(column)
        self.df.dropna(subset=[column], inplace=True)
        self.logger.info(f"Dropped NaN from column: {column}")
        return self

    def drop_columns(self, columns: List[str]) -> "CleanData":
        self.df.drop(columns=columns, inplace=True, errors='ignore')
        self.logger.info(f"Dropped columns: {columns}")
        return self

    def fillna_column(self, column: str, value: str) -> "CleanData":
        if column in self.df.columns:
            self.df[column].fillna(value, inplace=True)
            self.logger.info(f"Filled NaN in {column} with {value}")
        else:
            self.logger.warning(f"Column '{column}' not found for fillna.")
        return self

    def export(self, output_path: str) -> None:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        self.df.to_csv(output_path, index=False)
        self.logger.info(f"Exported cleaned data to {output_path}")

    def get_clean_df(self) -> pd.DataFrame:
        self.logger.info("Returning cleaned DataFrame")
        return self.df
