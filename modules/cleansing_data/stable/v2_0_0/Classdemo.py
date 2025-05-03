import pandas as pd
import logging 
import hashlib
import numpy as np
import datetime
import re
import json

x = datetime.datetime.now()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelno)s:%(funcName)s:%(message)s')

file_handler = logging.FileHandler('log/pipeline.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

class Cleandata :
    
    #init file path
    def __init__(self, DATAFOLDER,Data) :
        self.file = f"{DATAFOLDER}/{Data}.csv"
        logger.info('Initial Origin Path {} '.format(self.file))
        
    #init file path in final destination
    def finaldestination(self,NEWDATAFOLDER,Newdata) :
        self.Destinatonpath = f"{NEWDATAFOLDER}/Unhash/{Newdata}.csv"
        self.Destinatonpath_hash = f"{NEWDATAFOLDER}/Hash/{Newdata}.csv"
        logger.info('Destination path {}'.format(self.Destinatonpath))
    
    #Show path for make sure right destination
    def show_filepath(self,WDF) :
    
        if WDF == "Ori" :
            self.file_showpath = self.file
            a = print(f"Load this file from {self.file_showpath}")
            
        elif WDF == "Des" :
            self.file_showpath = self.Destinatonpath
            a = print(f"Ingest Data to file {self.file_showpath}")
        
        return a
            
    # Hash funtion
    def hash(self, col, num):
        df_hash = self.df
        df_hash_init = self.df

        df_hash = df_hash_init.loc[:, [df_hash_init.columns[num]]]

        arr = df_hash.to_numpy()
        hash_arr = []

        for stuff in arr:
            # Extract the string value from the numpy array
            string_value = stuff[0]  
            # Now you can encode and hash the string value
            if not isinstance(string_value, str):
                string_value = str(string_value)
                
            a = hashlib.sha224(string_value.encode('utf-8'))  
            b = a.hexdigest()
            hash_arr.append(b)

         
        new_column = f"{col}_hash"
        df_hash_init["Menu"] = hash_arr
        logger.info("Data was hashed" )
        self.df = df_hash_init #update the dataframe.
        
    #Load data into data frame
    def Loadfile(self):
        file_path = self.file
        self.df = pd.read_csv(file_path)
        logger.info('Read CSV-File with path : {}'.format(self.show_filepath) )      
        
    #Clean data LV.1 only reaname and drop some row
    def cleanlv1(self):
        df = self.df
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
        self.cleanlv1 = df_cleanlv1
        logger.info('Data was Clean by function CleanLV.1')   
        self.df = df_cleanlv1 #Conector to export file to destination
        
    
    #rearage order of columns 
    def rearrage(self,des):
        df = self.df
        column_names = list(df.columns)
        print(column_names)
        last_index = len(column_names) - 1
        a = column_names[last_index]
        column_names[des]
        column_names[last_index]= column_names[des]
        column_names[des] = a
        df = df[column_names]
        
    #Cast data type
    def castdtype(self) :
        self.df.columns = self.df.columns.str.strip()
        # Cast data type Date and time
        self.df['Date'] = pd.to_datetime(self.df['Date'], errors='coerce')
        self.df['Timestamp'] = pd.to_datetime(self.df['Timestamp'], errors='coerce') #แก้ด้วย

        # Cast data type
        self.df['ID_invoice'] = self.df['ID_invoice'].astype(str)
        self.df['INV_No'] = pd.to_numeric(self.df['INV_No'], errors='coerce')
        self.df['Code_Cash_drawer'] = self.df['Code_Cash_drawer'].astype(str)
        self.df['Menu'] = self.df['Menu'].astype(str)
        self.df['Order_type'] = self.df['Order_type'].astype(str)
        self.df['Numbers'] = pd.to_numeric(self.df['Numbers'], errors='coerce')
        self.df['Price_per_unit'] = pd.to_numeric(self.df['Price_per_unit'], errors='coerce')
        self.df['BD'] = pd.to_numeric(self.df['BD'], errors='coerce')  
        self.df['Percent_Discount'] = pd.to_numeric(self.df['Percent_Discount'], errors='coerce')
        self.df['Net_Price'] = pd.to_numeric(self.df['Net_Price'], errors='coerce')

        self.df['Tax_Type'] = self.df['Tax_Type'].astype(str)
        self.df['Table'] = self.df['Table'].astype(str)
        self.df['Customer_Name'] = self.df['Customer_Name'].astype(str)
        self.df['Tel'] = self.df['Tel'].astype(str)
        self.df['Payment_method'] = self.df['Payment_method'].astype(str)
        self.df['Note'] = self.df['Note'].astype(str)
        self.df['Group'] = self.df['Group'].astype(str)
        self.df['Category'] = self.df['Category'].astype(str)
        self.df['brach'] = self.df['brach'].fillna(-1).astype(int) # replaced NAN by -1
    
    def export_file(self):
        file_2path = self.Destinatonpath
        with open(file_2path,  'w', encoding = 'utf=8') as file:
            self.df_export = self.df
            self.df_export.to_csv(file)
           
        logger.info("Created file into {}".format(self.Destinatonpath))
    
    def end_point(self):
        logger.info("---------------------------------------------------------------------------------------------------")