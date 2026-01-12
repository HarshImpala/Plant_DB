import pandas as pd

df = pd.read_csv(r"/MongoDB_Project/excel_files/acronyms_space_separated.txt", sep="\s+", header=None)
df.columns = ["Short form", "Latin family name", "Hungarian family name"]
df.to_csv(r"C:\Users\aron_\PycharmProjects\obsidian_app\MongoDB_Project\excel_files\acronyms_comma_separated.txt", index=False)
