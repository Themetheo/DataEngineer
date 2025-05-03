# import Funcleansing as fc

# fc.Cleandata("Data","2021Sep","Warehouse","01")

import json

filepath = "cx.json"  # Or the full path if needed

try:
    with open(filepath, 'r', encoding='utf-8') as f:  # Or the correct encoding if known
        x = json.load(f)
    print(x)
except FileNotFoundError:
    print(f"Error: File not found at '{filepath}'")
except json.JSONDecodeError as e:
    print(f"Error: Invalid JSON data in '{filepath}': {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

