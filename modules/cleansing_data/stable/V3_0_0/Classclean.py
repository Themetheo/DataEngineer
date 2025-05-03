#version 3.1.1
import pandas as pd
from utlis import  setup_logger

# # Setup logging
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)
# formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(funcName)s:%(message)s')
# file_handler = logging.FileHandler('log/pipeline.log')
# file_handler.setFormatter(formatter)
# logger.addHandler(file_handler)


class Cleandata:

    def __init__(self, data_folder, data_name):
        self.file = f"{data_folder}/{data_name}.csv"
        self.df_raw = pd.read_csv(self.file)
        self.df = self.df_raw.copy()
        self.logger = setup_logger("Classclean")  #set up logging
        self.logger.info("Starting Clean process...")
        self.logger.info(f'Initial Origin Path {self.file}')

    def show_filepath(self, mode):
        if mode == "Ori":
            print(f"📂 Load this file from: {self.file}")
        else:
            print(f"📁 404 Not Found")

    def Loadfile(self):
        self.logger.info(f'Read CSV-File with path: {self.file}')

    def cleanlv1(self):
        try:
            rename_cols = {
                'วันที่ชำระเงิน': 'Date',
                'เวลาที่ชำระเงิน': 'Timestamp',
                'หมายเลขใบเสร็จ / ID': 'Receipt_ID',
                'INV. No': 'Invoice_No',
                'รหัสถาดเก็บเงิน': 'Cashier_Code',
                'ชื่อเมนู': 'Menu',
                'ประเภทการสั่ง': 'Order_Type',
                'จำนวน': 'Quantity',
                'ราคาต่อหน่วย': 'Unit_Price',
                'ยอดก่อนลด': 'Subtotal',
                'ส่วนลดทั้งหมด': 'Discount_Amount',
                'ส่วนลดทั้งหมด %': 'Discount_Percent',
                'ราคาสุทธิ': 'Net_Price',
                'ประเภทภาษีของรายการ': 'Tax_Type',
                'โต๊ะ': 'Table',
                'ชื่อลูกค้า': 'Customer_Name',
                'เบอร์โทรศัพท์': 'Phone',
                'ประเภทการชำระเงิน': 'Payment_Type',
                'หมายเหตุ': 'Note',
                'กลุ่ม': 'Group',
                'หมวดสินค้า': 'Category',
                'สาขา': 'Branch'
            }
            self.df.rename(columns=rename_cols, inplace=True)
            self.df.dropna(subset=["Menu"], inplace=True)
            self.df.drop(self.df.loc[self.df['Menu'].isin(['ไม่รับเครื่องปรุง','ไม่รับช้อนส้อมพลาสติก'])].index, inplace=True)
            self.logger.info("✅ Cleaned level 1 data")
            
        except Exception as e:
            self.logger.error(f"Error in cleaning data: {e}")
            
    def castdtype(self):
        self.df.columns = self.df.columns.str.strip()

        # แปลงวันที่และเวลา
        self.df['Date'] = pd.to_datetime(self.df['Date'], errors='coerce')       # only date
        self.df['Timestamp'] = pd.to_datetime(self.df['Timestamp'], errors='coerce').dt.time  # only Time

        # แปลงประเภทข้อมูลอื่น ๆ
        self.df['Receipt_ID'] = self.df['Receipt_ID'].astype(str)
        self.df['Invoice_No'] = pd.to_numeric(self.df['Invoice_No'], errors='coerce')
        self.df['Cashier_Code'] = self.df['Cashier_Code'].astype(str)
        self.df['Menu'] = self.df['Menu'].astype(str)
        self.df['Order_Type'] = self.df['Order_Type'].astype(str)
        self.df['Quantity'] = pd.to_numeric(self.df['Quantity'], errors='coerce')
        self.df['Unit_Price'] = pd.to_numeric(self.df['Unit_Price'], errors='coerce')
        self.df['Subtotal'] = pd.to_numeric(self.df['Subtotal'], errors='coerce')
        self.df['Discount_Percent'] = pd.to_numeric(self.df['Discount_Percent'], errors='coerce')
        self.df['Net_Price'] = pd.to_numeric(self.df['Net_Price'], errors='coerce')

        self.df['Tax_Type'] = self.df['Tax_Type'].astype(str)
        self.df['Table'] = self.df['Table'].astype(str)
        self.df['Customer_Name'] = self.df['Customer_Name'].astype(str)
        self.df['Phone'] = self.df['Phone'].astype(str)
        self.df['Payment_Type'] = self.df['Payment_Type'].astype(str)
        self.df['Note'] = self.df['Note'].astype(str)
        self.df['Group'] = self.df['Group'].astype(str)
        self.df['Category'] = self.df['Category'].astype(str)

        self.df['Branch'] = self.df['Branch'].fillna(-1).astype(int) # nan is -1

        self.logger.info("🧪 Casted data types for all relevant columns (Timestamp → time only)")


    def end_point(self):
        self.logger.info("🏁 Pipeline finished.")