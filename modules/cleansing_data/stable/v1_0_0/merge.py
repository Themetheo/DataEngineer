import pandas as pd
import glob
import os
from tqdm import tqdm  

# Path file
path = "Data"  # หรือใส่ path เต็ม เช่น "D:/project/data/"
csv_files = glob.glob(os.path.join(path, "*.csv"))
print(csv_files)
csv_files.sort()

# progress bar show data concated
combined_df = pd.concat(
    [pd.read_csv(file) for file in tqdm(csv_files, desc="Reading CSV files")],
    ignore_index=True
)

# export file 
file_2path = "Warehouse/concated.csv"
with open(file_2path,  'w', encoding = 'utf=8') as file:
    combined_df.to_csv(file)
           
