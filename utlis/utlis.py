import hashlib
import pandas as pd
import logging
import os

# ฟังก์ชันสำหรับแฮชคอลัมน์
def hash_column(series: pd.Series, method='sha224') -> pd.Series:
    """
    แปลงค่าทั้งคอลัมน์เป็น hash โดยใช้ SHA-224 หรือ MD5
    """
    if method == 'sha224':
        hashed_series = series.astype(str).apply(lambda x: hashlib.sha224(x.encode('utf-8')).hexdigest())
    elif method == 'md5':
        hashed_series = series.astype(str).apply(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest())
    else:
        raise ValueError("Method ไม่รองรับ: ใช้ 'sha224' หรือ 'md5'")
    
    return hashed_series

# ฟังก์ชันบันทึกข้อมูล DataFrame ลงใน CSV
def export_file(df: pd.DataFrame, path: str, index=False):
    """
    บันทึก DataFrame ไปยังไฟล์ CSV
    """
    try:
        df.to_csv(path, index=index)
        print(f"✅ Exported to {path}")
    except Exception as e:
        print(f"❌ Export failed: {e}")

# ฟังก์ชันสำหรับตั้งค่า logger และบันทึก log
def setup_logger(module_name: str, encoding: str = "utf-8"):
    """
    ฟังก์ชันนี้จะสร้าง logger ให้แยกตาม module_name
    """
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)

    # สร้างโฟลเดอร์ log ถ้าไม่มี
    log_folder = 'log'
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)
    
    log_file = os.path.join(log_folder, f'{module_name}.log')
    file_handler = logging.FileHandler(log_file, encoding=encoding)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    return logger


# ฟังก์ชันแฮชคอลัมน์และย้ายคอลัมน์ที่แฮชไปหลังคอลัมน์ต้นฉบับ
def hash_and_move_column(df: pd.DataFrame, colname: str, method='sha224'):
    """
    ฟังก์ชันสำหรับแฮชคอลัมน์และย้ายคอลัมน์ที่แฮชไปหลังคอลัมน์ต้นฉบับ
    """
    # แฮชคอลัมน์
    hashed_col = hash_column(df[colname], method)
    hashed_col_name = f"{colname}_hash"
    
    # ad columns for hash in DataFrame
    df[hashed_col_name] = hashed_col
    
    # arrange for hash column beside original column 
    cols = list(df.columns)
    cols.remove(hashed_col_name)
    insert_pos = cols.index(colname) + 1
    cols.insert(insert_pos, hashed_col_name)
    df = df[cols]

    return df
