import pandas as pd
import time

#Call file from data folder (Raw Data)

DATA_FOLDER = "Data"
data = "202109Sep"
file_path = f"{DATA_FOLDER}/{data}.csv"

df = pd.read_csv(file_path)

#Change Columns Name (Clean Lv.1)

df_renamecolumns = df.set_axis(['Date', 
                           'Timestamp ',
                           'ID_invoice', 
                           'INV_No',
                           'Code_Cash_drawer', 
                           'Menu', 
                           'Order_type',
                           'Numbers', 
                           'Price_per_unit',
                           'BD',
                           'Discount',
                           'Percent_Discount',
                           'Net_Price', 
                           'Tax_Type',
                           'Table', 
                           'Customer_Name', 
                           'Tel', 
                           'Payment_method', 
                           'Note',
                           'Group', 
                           'Category', 
                           'brach'], axis='columns')


df_drop = df_renamecolumns.drop(df_renamecolumns[df_renamecolumns['Date']=='Total'].index, 
                 inplace=False)



# Upload CSV-file to data Warehouse

print("loading to WAREHOUSE... ")

NEWDATA_FOLDER = "Warehouse"
data = "02test45"
file_2path = f"{NEWDATA_FOLDER}/{data}.csv"
with open(file_2path,  'w', encoding = 'utf=8') as file:
    df_drop.to_csv(file)

print("A nick of time ... ")
time.sleep(0.5)
print("Done!!!")