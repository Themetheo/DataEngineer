import hashlib
import uuid

class IdGenerator:
    def __init__(self, df):
        self.df = df.copy()

    def generate_all_ids(self):
        self.df['branch_id'] = self.df['branch'].astype(str).apply(self.generate_md5)
        self.df['payment_method_id'] = self.df['Payment_method'].astype(str).apply(self.generate_md5)

        self.df['Date'] = self.df['Date'].astype(str)
        self.df['session_key'] = (
            self.df['Customer_Name'].astype(str) + "_" +
            self.df['Table'].astype(str) + "_" +
            self.df['Date']
        )
        self.df['customer_id'] = self.df['session_key'].apply(self.generate_md5)

        self.df['sales_id'] = [self.generate_sales_id() for _ in range(len(self.df))]

        return self.df

    @staticmethod
    def generate_md5(key: str) -> str:
        return hashlib.md5(key.encode('utf-8')).hexdigest()[:8]

    @staticmethod
    def generate_sales_id() -> str:
        return str(uuid.uuid4())[:8]

