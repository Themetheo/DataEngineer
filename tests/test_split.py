
import pandas as pd
from modules.splittable.split import split_tables

def test_split_tables():
    df = pd.DataFrame({
        "Product ID": ["P1", "P2"],
        "customer_id": ["C1", "C2"],
        "branch_id": ["B1", "B2"],
        "payment_method_id": ["PM1", "PM2"],
        "Price": [100, 200],
        "Date": ["2024-01-01", "2024-01-02"]
    })
    tables = split_tables(df)

    assert isinstance(tables, dict)
    assert "fact_sale" in tables
    for key, table in tables.items():
        assert isinstance(table, pd.DataFrame)
