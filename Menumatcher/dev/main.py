import pandas as pd
import json
from Menumatcher.dev.Matcherwritethisfileonly import MenuMatcher

df = pd.read_csv("mockdata/mock1.csv")

with open("modules/Menumatcher/Data_menumatcher/menu_mapping.json", "r", encoding="utf-8") as f:
    menu_dict = json.load(f)

with open("modules/Menumatcher/Data_menumatcher/codemenu.json", "r", encoding="utf-8") as f:
    codemenu = json.load(f)


matcher = MenuMatcher(df, menu_dict, codemenu)
matcher.match_all()
