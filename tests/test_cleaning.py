
import pandas as pd
from modules.cleansing_data.stable.V3_0_0.Classclean import Cleandata

def test_cleanlv1():
    cleaner = Cleandata("Data", "202108Aug", "Warehouse", "06")
    df_clean = cleaner.cleanlv1()

    assert isinstance(df_clean, pd.DataFrame)
    assert "Menu" in df_clean.columns
    assert df_clean.shape[0] > 0
