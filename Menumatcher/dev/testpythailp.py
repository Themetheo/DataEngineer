# from pythainlp.tokenize import word_tokenize
# import time


# text = "ข้าวหมูกรอบธรรมดา แถม บะหมี่น้ำหมูสับหมูตุ๋น ข้าวหมูแดงเรือเมล์ ข้าวขาหมู"

# start_time = time.time()

# tokens = word_tokenize(text, engine="attacut")
# print("📌 Tokenized:", tokens)

# end_time = time.time()
# duration = end_time - start_time

# print(f"⏱️ ใช้เวลา: {duration:.4f} วินาที")

import time
from pythainlp.tokenize import word_tokenize
from tqdm import tqdm

# ลองใช้เมนูจำลองจำนวนมาก
sample_texts = [
    "ข้าวหมูกรอบธรรมดา แถม บะหมี่น้ำหมูสับหมูตุ๋น",
    "ข้าวหมูแดงเรือเมล์ ข้าวขาหมูพิเศษ",
    "เกาเหลาไก่ตุ๋น ธรรมดา",
    "ข้าวมันไก่ทอด แถมซุป","ข้าวหมูกรอบธรรมดา แถม บะหมี่น้ำหมูสับหมูตุ๋น ข้าวหมูแดงเรือเมล์ ข้าวขาหมู",
] * 1000  # จำนวน 4,000 บรรทัด

engine = "attacut"  # หรือ "newmm"

start_time = time.time()

for text in tqdm(sample_texts, desc=f"Testing engine: {engine}"):
    word_tokenize(text, engine=engine)

end_time = time.time()
duration = end_time - start_time

print(f"✅ ใช้เวลา tokenize ทั้งหมด ({engine}): {duration:.2f} วินาที")
print(f"🧠 ความเร็วเฉลี่ยต่อข้อความ: {duration/len(sample_texts):.6f} วินาที/ข้อความ")
