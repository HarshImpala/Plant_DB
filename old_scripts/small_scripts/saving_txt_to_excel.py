import pandas as pd

df = pd.read_csv(r"C:\Users\aron_\PycharmProjects\obsidian_app\MongoDB_Project\excel_files\acronyms_comma_separated.txt")
df.to_excel(r"C:\Users\aron_\PycharmProjects\obsidian_app\MongoDB_Project\excel_files\acronyms_comma_separated.xlsx", index=False)