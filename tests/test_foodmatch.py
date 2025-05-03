
import pandas as pd
from modules.Menumatcher.dev.Matchmenudemo2 import match_menu

def test_match_menu():
    df = pd.DataFrame({
        "Menu": ["ข้าวกะเพรา", "ข้าวไข่เจียว"]
    })
    df_matched = match_menu(df)

    assert "Product ID" in df_matched.columns
    assert len(df_matched) == 2

