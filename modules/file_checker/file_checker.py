import os
import json
from datetime import datetime
from utlis import setup_logger  # import setup_logger 

class FileChecker:
    def __init__(self, metadata_path="file_status.json"):
        self.metadata_path = metadata_path
        self.metadata = self._load_metadata()
        
        # ตั้งค่า logger ด้วยชื่อโมดูล
        self.logger = setup_logger("FileChecker")
        
    def _load_metadata(self):
        """โหลดข้อมูล metadata จากไฟล์"""
        if os.path.exists(self.metadata_path):
            with open(self.metadata_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def is_already_processed(self, filename):
        """เช็คว่าไฟล์เคยถูกประมวลผลหรือยัง"""
        self.logger.info(f"Checking if {filename} is already processed.")
        return filename in self.metadata

    def log_file(self, filename, status="success", matched_pct=None):
        """บันทึกสถานะการประมวลผลไฟล์และเปอร์เซ็นต์การจับคู่"""
        self.logger.info(f"Logging file {filename} with status {status} and matched {matched_pct}%.")
        
        self.metadata[filename] = {
            "status": status,
            "matched": matched_pct,
            "processed_at": datetime.now().isoformat()
        }
        
        with open(self.metadata_path, "w", encoding="utf-8") as f:
            json.dump(self.metadata, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"File {filename} logged successfully.")
