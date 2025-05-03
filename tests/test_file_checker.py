
from modules.file_checker.file_checker import run_checker

def test_run_checker():
    result = run_checker("Data/202108Aug.csv")
    assert result is True or result is None
