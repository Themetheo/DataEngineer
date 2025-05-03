import pandas as pd

# Function for 
# Import File  
# Clean Data Level 1
# Export File to csv

def Cleandata(DATA_FOLDER,data,NEWDATA_FOLDER,Newdata) :
   
   #Call file from data folder (Raw Data)
   
   file_path = f"{DATA_FOLDER}/{data}.csv"
   print(f"Load this file from {file_path}")
   df = pd.read_csv(file_path)
   
   #Cleansing Level 1
   print("Cleansing Level.1")
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

   df_cleanlv1 = df_renamecolumns.drop(df_renamecolumns[df_renamecolumns['Date']=='Total'].index, 
                 inplace=False)

   df_cleanlv1 = df_cleanlv1.drop(df_cleanlv1.loc[df_cleanlv1['Menu'].isin(['ไม่รับเครื่องปรุง','ไม่รับช้อนส้อมพลาสติก'])].index)
   
   df_export = df_cleanlv1 #Conector to export file to destination
   
   #Export Data to csv file
   
   file_2path = f"{NEWDATA_FOLDER}/{Newdata}.csv"
   print(f"Uploading DataFrame to csv flie with this path {file_2path}")
   with open(file_2path,  'w', encoding = 'utf=8') as file:
      df_export.to_csv(file)
      
   return print("Done")


