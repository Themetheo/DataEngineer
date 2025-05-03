class TableSplitter:
    def __init__(self, df):
        self.df = df.copy()

    def split_fact_sales(self):
        fact_sales_columns = [
            'sales_id', 'ID_invoice', 'Date', 'Timestamp', 'INV_No', 'Code_Cash_drawer', 'product_id',
            'branch_id', 'Order_type', 'Numbers', 'Price_per_unit', 'BD', 'Discount',
            'Percent_Discount', 'Net_Price', 'Tax_Type', 'customer_id', 'payment_method_id', 'Note'
        ]
        return self.df[fact_sales_columns].rename(columns={'Numbers': 'quantity'}).reset_index(drop=True)

    def split_dim_product(self):
        return self.df[['product_id', 'Menu', 'Group', 'Category']].drop_duplicates().reset_index(drop=True)

    def split_dim_branch(self):
        return self.df[['branch_id', 'branch']].drop_duplicates().rename(columns={'branch': 'branch_name'}).reset_index(drop=True)

    def split_dim_customer(self):
        return self.df[['customer_id', 'Customer_Name', 'Table', 'Tel']].drop_duplicates().rename(columns={
            'Customer_Name': 'customer_name',
            'Table': 'table_name',
            'Tel': 'tel'
        }).reset_index(drop=True)

    def split_dim_payment_method(self):
        return self.df[['payment_method_id', 'Payment_method']].drop_duplicates().rename(columns={'Payment_method': 'payment_method'}).reset_index(drop=True)
