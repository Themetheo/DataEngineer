import re
from fuzzywuzzy import process

# เมนูจากฐานข้อมูล (รหัส -> ชื่อเมนู)
menu_db = {
    "RNTYMD01": "เส้นเล็กต้มยำหมูแดงธรรมดา",
    "RNTYMS01": "เส้นเล็กต้มยำหมูสับธรรมดา",
    "ENWAKT01": "บะหมี่น้ำไก่ตุ๋นธรรมดา"
    # เพิ่มเมนูอื่นๆ ได้ตามต้องการ
}

# mapping คีย์เวิร์ด
keyword_map = {
    "RN": "เส้นเล็ก",
    "WRN": "เส้นใหญ่",
    "EN": "บะหมี่",
    "TRN": "หมี่ขาว",
    "DPL": "เกี้ยว",
    "GN": "วุ้นเส้น",
    "MS": "หมูสับ",
    "MD": "หมูแดง",
    "MG": "หมูกรอบ",
    "MT": "หมูตุ๋น",
    "KT": "ไก่ตุ๋น",
    "WA": "น้ำ",
    "DR": "แห้ง",
    "TS": "ต้มส้ม",
    "TY": "ต้มยำ",
    "SO": "เกาเหลา",
    "01": "ธรรมดา",
    "02": "พิเศษ",
    "WR": "ข้าว",
    "SET": "กับ",
    "CM": "ขาหมู",
    "SAU": "กุนเฉียง"
}

# สร้าง reverse mapping (value -> key)
reverse_map = {v: k for k, v in keyword_map.items()}

def extract_keywords(text, reverse_map, threshold=80):
    tokens = re.findall(r"[ก-๙a-zA-Z0-9]+", text)
    matched_keys = set()

    for token in tokens:
        match, score = process.extractOne(token, reverse_map.keys())
        if score >= threshold:
            matched_keys.add(reverse_map[match])
    
    return matched_keys

def build_menu_code(keys):
    # เรียงลำดับตาม prefix ที่เป็นไปได้
    order = ['RN', 'WRN', 'EN', 'TRN', 'DPL', 'GN',  # เส้น
             'WA', 'DR', 'TS', 'TY', 'SO',           # น้ำ/แห้ง/ประเภทน้ำซุป
             'MS', 'MD', 'MG', 'MT', 'KT', 'CM', 'SAU',  # เนื้อสัตว์
             '01', '02']                              # ขนาด
             
    code = ''
    for key in order:
        if key in keys:
            code += key
    return code

def find_menu(text):
    keys = extract_keywords(text, reverse_map)
    
    # ถ้าไม่มีขนาดอาหาร เช่น 01/02 ให้ default เป็น 01
    if not any(k in keys for k in ['01', '02']):
        keys.add('01')

    menu_code = build_menu_code(keys)

    if menu_code in menu_db:
        return menu_code, menu_db[menu_code]
    else:
        # fuzzy search หาเมนูใกล้เคียง
        best_match, score = process.extractOne(text, menu_db.values())
        for code, name in menu_db.items():
            if name == best_match:
                return code, name + f" (map score {score}%)"
    
    return None, "404 Not found"

# 🔍 tesstttt
order_text = "ก๋วยเตี๋ยวหมูสับต้มยำ x 1เส้นเล็ก x 1"
code, name = find_menu(order_text)
print(f"Matched Code: {code}\nMenu Name: {name}")
