import re
import pandas as pd

# =========================
# CONFIG
# =========================
INFILE_XLSX = "plants_gbif_matched_plus_wfo.xlsx"   # change if your filename differs
OUT_XLSX = "plants_gbif_matched_plus_wfo_syn_diff.xlsx"
OUT_CSV = "plants_gbif_matched_plus_wfo_syn_diff.csv"

GBIF_COL = "gbif_synonyms"
WFO_COL = "wfo_synonyms"

OUT_GBIF_NOT_WFO = "synonyms_in_gbif_not_in_wfo"
OUT_WFO_NOT_GBIF = "synonyms_in_wfo_not_in_gbif"

SEP = " | "  # your delimiter


# =========================
# HELPERS
# =========================
def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", str(s)).strip()

def normalize_syn(s: str) -> str:
    """
    Normalize a synonym string for comparison:
      - collapse whitespace
      - unify × to x
      - remove some punctuation that varies between sources
      - casefold
    """
    s = normalize_spaces(s)
    s = s.replace("×", "x")
    s = re.sub(r"[“”\"']", "", s)
    s = re.sub(r"\s*([(),.;])\s*", r"\1", s)  # tighten common punctuation
    return s.casefold()

def split_synonyms(cell) -> list[str]:
    if cell is None or (isinstance(cell, float) and pd.isna(cell)):
        return []
    s = normalize_spaces(str(cell))
    if not s:
        return []
    parts = [normalize_spaces(p) for p in s.split(SEP)]
    return [p for p in parts if p]

def diff_synonyms(gbif_raw: str, wfo_raw: str) -> tuple[list[str], list[str]]:
    gbif_list = split_synonyms(gbif_raw)
    wfo_list = split_synonyms(wfo_raw)

    gbif_norm = {normalize_syn(x): x for x in gbif_list}  # keep a representative original
    wfo_norm = {normalize_syn(x): x for x in wfo_list}

    gbif_only_norm = [k for k in gbif_norm.keys() if k not in wfo_norm]
    wfo_only_norm = [k for k in wfo_norm.keys() if k not in gbif_norm]

    gbif_only = [gbif_norm[k] for k in gbif_only_norm]
    wfo_only = [wfo_norm[k] for k in wfo_only_norm]

    # stable ordering for readability
    gbif_only = sorted(gbif_only, key=lambda x: (len(x), x.lower()))
    wfo_only = sorted(wfo_only, key=lambda x: (len(x), x.lower()))

    return gbif_only, wfo_only


# =========================
# MAIN
# =========================
def main():
    df = pd.read_excel(INFILE_XLSX)
    df.columns = df.columns.str.strip()

    if GBIF_COL not in df.columns:
        raise KeyError(f"Missing column: {GBIF_COL}")
    if WFO_COL not in df.columns:
        raise KeyError(f"Missing column: {WFO_COL}")

    gbif_not_wfo_vals = []
    wfo_not_gbif_vals = []

    for _, row in df.iterrows():
        gbif_only, wfo_only = diff_synonyms(row.get(GBIF_COL, ""), row.get(WFO_COL, ""))
        gbif_not_wfo_vals.append(SEP.join(gbif_only))
        wfo_not_gbif_vals.append(SEP.join(wfo_only))

    df[OUT_GBIF_NOT_WFO] = gbif_not_wfo_vals
    df[OUT_WFO_NOT_GBIF] = wfo_not_gbif_vals

    df.to_excel(OUT_XLSX, index=False)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    print(f"Wrote: {OUT_XLSX}")
    print(f"Wrote: {OUT_CSV}")
    print(f"Added columns: {OUT_GBIF_NOT_WFO}, {OUT_WFO_NOT_GBIF}")

if __name__ == "__main__":
    main()
