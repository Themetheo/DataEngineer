import json
import os

from google.cloud import bigquery
from google.oauth2 import service_account


DATA_FOLDER = "data"

# ตัวอย่างการกำหนด Path ของ Keyfile ในแบบที่ใช้ Environment Variable มาช่วย
# จะทำให้เราไม่ต้อง Hardcode Path ของไฟล์ไว้ในโค้ดของเรา
# keyfile = os.environ.get("KEYFILE_PATH")

keyfile = "dataengineer-reumeproject-load-files-to-bigquery.json"
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = "dataengineer-reumeproject"
client = bigquery.Client(
    project=project_id,
    credentials=credentials,
)
job_config = bigquery.LoadJobConfig(
    skip_leading_rows=1,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,
)

# Addressess
data = "addresses"
file_path = f"{DATA_FOLDER}/{data}.csv"
with open(file_path, "rb") as f:
    table_id = f"{project_id}.Testcode_from_deb3.{data}"
    job = client.load_table_from_file(f, table_id, job_config=job_config)
    job.result()

table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")