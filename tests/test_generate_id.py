
import pandas as pd
from modules.gen_id.generate_id import generate_all_ids

def test_generate_all_ids():
    df = pd.DataFrame({
        "Customer name": ["Alice", "Bob"],
        "Branch": ["A", "B"],
        "Payment Method": ["cash", "credit"]
    })
    df_result = generate_all_ids(df)

    assert "customer_id" in df_result.columns
    assert "branch_id" in df_result.columns
    assert "payment_method_id" in df_result.columns
    assert len(df_result) == 2
