# version 3.2.0 - updated with dynamic config, validation, and better error handling

import pandas as pd
import json
from utlis import setup_logger

class Cleandata:

    def __init__(self, data_folder, data_name, config_path="config/rename_cols.json"):
        self.file = f"{data_folder}/{data_name}.csv"
        self.config_path = config_path
        self.df_raw = pd.read_csv(self.file)
        self.df = self.df_raw.copy()
        self.logger = setup_logger("Classclean")
        self.logger.info("Starting Clean process...")
        self.logger.info(f'Initial Origin Path {self.file}')

    def show_filepath(self, mode):
        if mode == "Ori":
            print(f"📂 Load this file from: {self.file}")
        else:
            print(f"📁 404 Not Found")

    def Loadfile(self):
        self.logger.info(f'Read CSV-File with path: {self.file}')

    def load_rename_config(self):
        """โหลดไฟล์ rename config แทนการ hardcode"""
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                rename_cols = json.load(f)
            self.logger.info(f"Loaded rename config from {self.config_path}")
            return rename_cols
        except Exception as e:
            self.logger.error(f"Error loading rename config: {e}")
            raise

    def cleanlv1(self):
        try:
            rename_cols = self.load_rename_config()
            self.df.rename(columns=rename_cols, inplace=True)
            self.df.dropna(subset=["Menu"], inplace=True)
            self.df.drop(self.df.loc[self.df['Menu'].isin(['ไม่รับเครื่องปรุง','ไม่รับช้อนส้อมพลาสติก'])].index, inplace=True)
            self.logger.info("✅ Cleaned level 1 data")
        except Exception as e:
            self.logger.error(f"Error in cleaning data: {e}")

    def castdtype(self):
        self.df.columns = self.df.columns.str.strip()

        cast_rules = {
            'Date': ('datetime', 'coerce'),
            'Timestamp': ('time', 'coerce'),
            'Receipt_ID': ('str', None),
            'Invoice_No': ('numeric', 'coerce'),
            'Cashier_Code': ('str', None),
            'Menu': ('str', None),
            'Order_Type': ('str', None),
            'Quantity': ('numeric', 'coerce'),
            'Unit_Price': ('numeric', 'coerce'),
            'Subtotal': ('numeric', 'coerce'),
            'Discount_Percent': ('numeric', 'coerce'),
            'Net_Price': ('numeric', 'coerce'),
            'Tax_Type': ('str', None),
            'Table': ('str', None),
            'Customer_Name': ('str', None),
            'Phone': ('str', None),
            'Payment_Type': ('str', None),
            'Note': ('str', None),
            'Group': ('str', None),
            'Category': ('str', None),
            'Branch': ('fillna_int', -1)
        }

        for col, (dtype, option) in cast_rules.items():
            try:
                if dtype == 'datetime':
                    self.df[col] = pd.to_datetime(self.df[col], errors=option)
                elif dtype == 'time':
                    self.df[col] = pd.to_datetime(self.df[col], errors=option).dt.time
                elif dtype == 'numeric':
                    self.df[col] = pd.to_numeric(self.df[col], errors=option)
                elif dtype == 'str':
                    self.df[col] = self.df[col].astype(str)
                elif dtype == 'fillna_int':
                    self.df[col] = self.df[col].fillna(option).astype(int)
                self.logger.info(f"✅ Casted column {col} successfully.")
            except Exception as e:
                self.logger.error(f"❗ Error casting column {col}: {e}")

    def validate_clean_data(self):
        """เช็คว่าสำคัญๆ ไม่มี missing หลัง clean"""
        try:
            critical_columns = ['Menu', 'Net_Price']
            for col in critical_columns:
                if self.df[col].isnull().sum() > 0:
                    raise ValueError(f"Column {col} still has missing values after cleaning!")
            self.logger.info("✅ Validation passed: No missing critical fields.")
        except Exception as e:
            self.logger.error(f"❗ Validation error: {e}")
            raise

    def end_point(self):
        self.logger.info("🏁 Pipeline finished.")