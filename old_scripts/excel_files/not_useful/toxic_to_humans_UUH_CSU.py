import re
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

# =========================
# Inputs/outputs
# =========================
GBIF_MATCHED_PATH = r"/PostgreSQL_DB/large_scripts/plants_gbif_matched.csv"
OUT_XLSX = "toxicity_results_humans_utah.xlsx"

UTAH_ALL_PLANTS_URL = "https://poisoncontrol.utah.edu/plant-guide/all-plants"
UTAH_BASE = "https://poisoncontrol.utah.edu"
HEADERS = {"User-Agent": "plant-toxicity-check/1.0 (aron_serebrenik@yahoo.com)"}

UTAH_CACHE_CSV = Path("utah_poison_plants_cache.csv")

# =========================
# Normalization
# =========================
RANK_MARKERS = {"sp", "spp", "ssp", "subsp", "var", "forma", "f", "cv", "cultivar"}

def clean_text(s: str) -> str:
    if s is None or pd.isna(s):
        return ""
    s = str(s).strip().lower()
    s = s.replace("×", "x")
    s = re.sub(r"['\"][^'\"]+['\"]", " ", s)      # cultivar quotes
    s = re.sub(r"\bnon\b.*$", " ", s)            # "non Forssk." notes
    s = re.sub(r"[(),.;\[\]]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def clean_tokens(s: str) -> list[str]:
    s = clean_text(s)
    toks = s.split()
    toks = [t for t in toks if t not in RANK_MARKERS and t != "x"]
    return toks

def key_genus_species(s: str) -> str:
    toks = clean_tokens(s)
    return f"{toks[0]} {toks[1]}" if len(toks) >= 2 else (toks[0] if toks else "")

# =========================
# HTTP
# =========================
def fetch(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text

# =========================
# Parse Utah pages
# =========================
def extract_toxicity_from_detail(detail_html: str) -> str:
    """
    On detail pages, Toxicity appears as a heading and then a value,
    e.g. "#### Toxicity" then "Poisonous" / "Skin irritant" / "Stomach irritant"
    """
    soup = BeautifulSoup(detail_html, "html.parser")
    # Find heading that says "Toxicity"
    # Often it's an <h4> with text "Toxicity"
    tox_heading = None
    for tag in soup.find_all(["h2", "h3", "h4", "strong"]):
        if tag.get_text(strip=True).lower() == "toxicity":
            tox_heading = tag
            break
    if not tox_heading:
        return ""

    # The value is usually the next element's text (often a sibling)
    # We'll walk forward until we find meaningful text.
    for nxt in tox_heading.find_all_next():
        txt = nxt.get_text(" ", strip=True)
        if not txt:
            continue
        # stop if we hit another section heading
        if txt.lower() in {"berries/fruits", "flowers", "native", "houseplant", "cultivated"}:
            break
        # first real value after "Toxicity"
        # (avoid repeating the word "Toxicity" itself)
        if txt.lower() != "toxicity":
            return txt
    return ""

def parse_all_plants_page(all_html: str) -> list[dict]:
    """
    Extract entries from the All Plants page by:
    - finding every 'More About ...' link
    - climbing to the nearest preceding heading (h3) for the common name
    - taking the scientific name from the text directly after that heading
    """
    soup = BeautifulSoup(all_html, "html.parser")
    rows = []

    for a in soup.find_all("a", href=True):
        a_text = a.get_text(" ", strip=True)
        href = a["href"]

        if "/plant-guide/" not in href:
            continue
        if not a_text.lower().startswith("more about"):
            continue

        # absolute URL
        url = href if href.startswith("http") else UTAH_BASE + href

        # Find closest preceding h3 (common name heading)
        h3 = a.find_previous(["h3", "h4"])
        common = h3.get_text(" ", strip=True) if h3 else ""
        common = re.sub(r"\s+", " ", common).strip()

        # Scientific name is usually the first short line after the heading
        sci = ""
        if h3:
            # look at next siblings after h3 until we find a short "latin-ish" line
            for sib in h3.find_all_next():
                if sib == a:
                    break
                txt = sib.get_text(" ", strip=True)
                txt = re.sub(r"\s+", " ", txt).strip()
                if not txt:
                    continue
                # stop if we hit the next plant heading
                if sib.name in {"h3", "h4"}:
                    break
                # pick first plausible binomial line (e.g., "Actaea rubra" or "Zantedeschia species")
                if re.match(r"^[A-Z][a-z]+\s+[a-zA-Z\-]+", txt):
                    sci = txt
                    break

        rows.append({
            "utah_common_name": common,
            "utah_scientific_name": sci,
            "utah_url": url,
            "utah_key_gs": key_genus_species(sci),
        })

    # de-dup by URL (preserve order)
    seen = set()
    dedup = []
    for r in rows:
        if r["utah_url"] in seen:
            continue
        seen.add(r["utah_url"])
        dedup.append(r)

    return dedup

def build_utah_cache(sleep_s: float = 0.15) -> pd.DataFrame:
    all_html = fetch(UTAH_ALL_PLANTS_URL)
    base_rows = parse_all_plants_page(all_html)

    # Now fetch each detail page to get Toxicity field
    out = []
    for r in base_rows:
        tox = ""
        try:
            detail_html = fetch(r["utah_url"])
            tox = extract_toxicity_from_detail(detail_html)
        except Exception:
            tox = ""

        out.append({
            **r,
            "utah_toxicity": tox,
        })
        time.sleep(sleep_s)

    df = pd.DataFrame(out)

    # If some scientific names are empty, keep them anyway (still useful for manual click-through)
    return df

def load_or_build_utah_cache() -> pd.DataFrame:
    if UTAH_CACHE_CSV.exists():
        return pd.read_csv(UTAH_CACHE_CSV, encoding="utf-8-sig")
    df = build_utah_cache()
    df.to_csv(UTAH_CACHE_CSV, index=False, encoding="utf-8-sig")
    return df

# =========================
# Match your GBIF plants to Utah toxicity (humans)
# =========================
def main():
    gbif_df = pd.read_csv(GBIF_MATCHED_PATH)
    utah_df = load_or_build_utah_cache()

    # Build lookup by genus+species
    utah_lookup = (
        utah_df[utah_df["utah_key_gs"].astype(str).str.strip() != ""]
        .drop_duplicates(subset=["utah_key_gs"])
        .set_index("utah_key_gs", drop=False)
    )

    out_rows = []
    for _, r in gbif_df.iterrows():
        input_name = r.get("input_name")
        canon = r.get("gbif_canonicalName")
        gs = r.get("gbif_genus_species")

        target = canon if isinstance(canon, str) and canon.strip() else (gs if isinstance(gs, str) and gs.strip() else input_name)
        k = key_genus_species(target)

        hit = None
        if k and k in utah_lookup.index:
            hit = utah_lookup.loc[k]
            if isinstance(hit, pd.DataFrame):
                hit = hit.iloc[0]

        utah_tox = str(hit.get("utah_toxicity")) if hit is not None else ""
        human_toxic = "unknown"
        if utah_tox:
            # Utah "Toxicity" values vary: Poisonous, Skin irritant, Stomach irritant, etc.
            # We'll mark anything non-empty as "flagged" and keep raw label.
            human_toxic = "flagged"

        out_rows.append({
            "input_latin_name": input_name,
            "gbif_canonicalName": canon,
            "gbif_genus_species": gs,
            "utah_match_key_gs": k,

            "human_toxic": human_toxic,               # flagged/unknown
            "utah_toxicity_raw": utah_tox if utah_tox else None,

            "utah_common_name": hit.get("utah_common_name") if hit is not None else None,
            "utah_scientific_name": hit.get("utah_scientific_name") if hit is not None else None,
            "utah_url": hit.get("utah_url") if hit is not None else None,

            "source_humans": "UtahPoisonControl" if hit is not None else None,
            "needs_manual_review": True if hit is not None else False,  # human poisoning is always worth review
        })

    out = pd.DataFrame(out_rows)
    out.to_excel(OUT_XLSX, index=False)

    print("Utah cache rows:", len(utah_df))
    print("Utah cache with sci keys:", (utah_df["utah_key_gs"].astype(str).str.strip() != "").sum())
    print("Matched humans:", out["source_humans"].notna().sum(), "of", len(out))
    print(f"Wrote {OUT_XLSX}")

if __name__ == "__main__":
    main()
