import pandas as pd
import requests
import time
import json
from pathlib import Path

IN_XLSX = "toxicity_results_pets_gbif.xlsx"
OUT_XLSX = "toxicity_results_pets_gbif_plus_wikidata.xlsx"

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WDQS = "https://query.wikidata.org/sparql"

HEADERS = {
    "User-Agent": "plant-toxicity-check/1.0 (aron_serebrenik@yahoo.com)",
    "Accept": "application/sparql-results+json",
}

# Items
POISONOUS_PLANT_QID = "Q21028485"  # poisonous plant :contentReference[oaicite:2]{index=2}
POISON_QID = "Q40867"              # poison :contentReference[oaicite:3]{index=3}
HAS_CHARACTERISTIC_P = "P1552"     # has characteristic :contentReference[oaicite:4]{index=4}

CACHE_PATH = Path("wikidata_cache.json")

def load_cache() -> dict:
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    return {}

def save_cache(cache: dict) -> None:
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

def wikidata_search_entity(query: str, limit: int = 1):
    params = {
        "action": "wbsearchentities",
        "search": query,
        "language": "en",
        "format": "json",
        "limit": limit,
    }
    r = requests.get(WIKIDATA_API, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    hits = data.get("search", [])
    return hits[0] if hits else None

def wdqs_bool(query: str) -> bool:
    r = requests.get(WDQS, params={"query": query}, headers=HEADERS, timeout=60)
    r.raise_for_status()
    js = r.json()
    cnt = int(js["results"]["bindings"][0]["count"]["value"])
    return cnt > 0

def wdqs_is_poisonous_plant(qid: str) -> bool:
    """
    True if:
      - item is subclass of poisonous plant (P279*), OR
      - item is instance/subclass chain to poisonous plant (rare but keep), OR
      - item has characteristic poison (P1552 poison)
    """
    sparql = f"""
    SELECT (COUNT(*) AS ?count) WHERE {{
      {{
        wd:{qid} wdt:P279/wdt:P279* wd:{POISONOUS_PLANT_QID} .
      }}
      UNION
      {{
        wd:{qid} wdt:P31/wdt:P279* wd:{POISONOUS_PLANT_QID} .
      }}
      UNION
      {{
        wd:{qid} wdt:{HAS_CHARACTERISTIC_P} wd:{POISON_QID} .
      }}
    }}
    """
    return wdqs_bool(sparql)

df = pd.read_excel(IN_XLSX)

# enrich only unmatched (same logic you used)
mask_unmatched = df["source_pets"].isna() | (df["source_pets"].astype(str).str.strip() == "")
to_enrich = df[mask_unmatched].copy()
print("Unmatched to enrich with Wikidata:", len(to_enrich))

# output cols
for col in ["wikidata_qid","wikidata_label","wikidata_description","wikidata_match_score","wikidata_poisonous_signal"]:
    if col not in df.columns:
        df[col] = None

cache = load_cache()

for idx, row in to_enrich.iterrows():
    query = row.get("query_used")
    if not isinstance(query, str) or not query.strip():
        query = row.get("input_latin_name")
    query = ("" if pd.isna(query) else str(query)).strip()
    if not query:
        continue

    # cache by query string
    if query in cache:
        hit = cache[query]
    else:
        hit = wikidata_search_entity(query, limit=1)
        cache[query] = hit
        time.sleep(0.05)

    if not hit:
        df.at[idx, "wikidata_match_score"] = 0
        df.at[idx, "wikidata_poisonous_signal"] = False
        continue

    qid = hit.get("id")
    df.at[idx, "wikidata_qid"] = qid
    df.at[idx, "wikidata_label"] = hit.get("label")
    df.at[idx, "wikidata_description"] = hit.get("description")
    df.at[idx, "wikidata_match_score"] = hit.get("match", {}).get("score", None)

    try:
        df.at[idx, "wikidata_poisonous_signal"] = wdqs_is_poisonous_plant(qid)
    except Exception:
        df.at[idx, "wikidata_poisonous_signal"] = None

    time.sleep(0.1)

save_cache(cache)
df.to_excel(OUT_XLSX, index=False)

print(f"Wrote {OUT_XLSX}")
print("Wikidata poisonous_signal = True:", (df["wikidata_poisonous_signal"] == True).sum())

print("Test Atropa bella-donna (Q156091):", wdqs_is_poisonous_plant("Q156091"))

