import csv
import re
import json

def count_value_duplicates_in_csv(file_path, column_index, keyword_json):
    """
    นับจำนวนครั้งที่พบค่าที่ซ้ำกันจาก JSON ในคอลัมน์ที่ระบุของไฟล์ CSV

    Args:
        file_path (str): เส้นทางไปยังไฟล์ CSV
        column_index (int): Index ของคอลัมน์ที่ต้องการตรวจสอบ (เริ่มจาก 0)
        keyword_json (str): JSON string ของคีย์เวิร์ดและค่า

    Returns:
        dict: Dictionary ที่มีค่าที่ซ้ำกันเป็นคีย์ (เช่น "หมูสับหมูสับ")
              และจำนวนครั้งที่พบเป็นค่า
    """
    duplicate_counts = {}
    keywords = json.loads(keyword_json)

    try:
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if len(row) > column_index:
                    text = row[column_index]
                    for value in keywords.values():
                        if value in text:
                            # ตรวจสอบการซ้ำกันของ value
                            pattern = r"(" + re.escape(value) + r")\s*" + r"(" + re.escape(value) + r")"
                            matches = re.findall(pattern, text)
                            if matches:
                                duplicate_value = value + value
                                count = text.count(duplicate_value)
                                if duplicate_value in duplicate_counts:
                                    duplicate_counts[duplicate_value] += count
                                else:
                                    duplicate_counts[duplicate_value] = count
    except FileNotFoundError:
        print(f"ไม่พบไฟล์: {file_path}")
        return None
    return duplicate_counts

# JSON ของคีย์เวิร์ดที่คุณให้มา
keyword_json_input = """
{
  "MS": "หมูสับ",
  "MD": "หมูแดง",
  "MG": "หมูกรอบ",
  "MT": "หมูตุ๋น",
  "KT": "ไก่ตุ๋น"
}
"""

# ตัวอย่างการใช้งาน
file_path = 'your_file.csv'  # แทนที่ด้วยเส้นทางไฟล์ CSV ของคุณ
column_to_check = 1  # สมมติว่าคอลัมน์ที่คุณต้องการตรวจสอบคือคอลัมน์ที่สอง (index 1)

duplicates = count_value_duplicates_in_csv(file_path, column_to_check, keyword_json_input)

if duplicates:
    for duplicate, count in duplicates.items():
        print(f"พบ '{duplicate}' จำนวน {count} ครั้ง")
else:
    print("ไม่พบคำซ้ำจากคีย์เวิร์ดที่กำหนด")