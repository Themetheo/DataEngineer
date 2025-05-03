
import pandas as pd
import os
from utlis import export_file, hash_and_move_column, hash_column, setup_logger

def test_export_file(tmp_path):
    df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
    export_path = tmp_path / "test_output.csv"
    export_file(df, str(export_path))
    assert export_path.exists()
    df_loaded = pd.read_csv(export_path)
    assert df_loaded.equals(df)

def test_hash_and_move_column():
    df = pd.DataFrame({"name": ["Alice", "Bob"]})
    df_hashed = hash_and_move_column(df.copy(), cols=["name"])
    assert "name_hashed" in df_hashed.columns
    assert df_hashed["name_hashed"].nunique() == 2
    assert df_hashed["name_hashed"].dtype == "object"

def test_hash_column():
    df = pd.DataFrame({"text": ["a", "b", "c"]})
    df_hashed = hash_column(df.copy(), "text")
    assert "text_hashed" in df_hashed.columns
    assert df_hashed["text_hashed"].nunique() == 3

def test_setup_logger_creates_logger():
    logger = setup_logger("test_logger")
    assert logger.name == "test_logger"
    assert hasattr(logger, "info")
