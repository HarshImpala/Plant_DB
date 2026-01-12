"""
powo_native_enrichment_api.py
-----------------------------
Add POWO nativity to plants_with_native_plus_wfo.csv.

Key features:
- POWO API search (v1/v2) + robust HTML fallback
- HARD STOP at "Introduced into" (no contamination)
- WGSRPD expansion FIRST
- Wikidata fallback using SEARCH → QID → geographic filter
  (prevents Czech Republic / Netherlands false positives)
- Resume-only-failed via cache
- Progress reporting

Input:
- plants_with_native_plus_wfo.csv

Output:
- plants_with_native_plus_wfo_powo.csv
- plants_with_native_plus_wfo_powo.xlsx

Adds columns:
- powo_native
- powo_url
- powo_taxon_id
- powo_error
"""

from __future__ import annotations

import json
import time
import re
from pathlib import Path
from typing import Any, Iterable, Optional, List
from urllib.parse import quote

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

# ======================================================
# CONFIG
# ======================================================
INPUT_CSV = "plants_with_native_plus_wfo.csv"
OUTPUT_CSV = "plants_with_native_plus_wfo_powo.csv"
OUTPUT_XLSX = "plants_with_native_plus_wfo_powo.xlsx"

NAME_COL = "gbif_canonicalName"

CACHE_PATH = Path("powo_native_cache.json")
WGSRPD_CACHE_PATH = Path("wgsrpd_mapping_cache.json")
WIKIDATA_CACHE_PATH = Path("wikidata_country_cache.json")

SLEEP_S = 0.35
WIKIDATA_SLEEP_S = 0.25
PROGRESS_EVERY_N = 10

RESUME_ONLY_FAILED = True
RETRY_BLANK_NATIVE_TOO = True

POWO_BASE = "https://powo.science.kew.org"
API_V1_SEARCH = f"{POWO_BASE}/api/1/search"
API_V2_SEARCH = f"{POWO_BASE}/api/2/search"

WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"

HEADERS = {
    "User-Agent": "plant-powo-native/1.4 (contact: aron_serebrenik@yahoo.com)",
    "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
    "Referer": POWO_BASE + "/",
}

STOP_WORDS = (
    "introduced into",
    "introduced",
    "synonyms",
    "taxonomy",
    "images",
    "general information",
    "literature",
)

LABELS = (
    "native to:",
    "native range",
    "native:",
    "distribution",
    "occurs in",
)

# ======================================================
# Helpers
# ======================================================
def clean(s) -> str:
    if s is None:
        return ""
    if isinstance(s, float) and pd.isna(s):
        return ""
    return str(s).strip()

def clean_space(s) -> str:
    return re.sub(r"\s+", " ", clean(s)).strip()

def dedup_preserve(items: Iterable[str]) -> list[str]:
    seen = set()
    out = []
    for it in items:
        it = clean_space(it)
        if not it:
            continue
        k = it.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(it)
    return out

def load_json(path: Path) -> dict[str, Any]:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}

def save_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def powo_results_url(query: str) -> str:
    return f"{POWO_BASE}/results?q={quote(query, safe='')}"

def taxon_url_from_urn(urn: str) -> str:
    return f"{POWO_BASE}/taxon/{quote(urn, safe='')}"

# ======================================================
# WGSRPD mapping
# ======================================================
def load_wgsrpd_mapping() -> dict[str, Any]:
    return load_json(WGSRPD_CACHE_PATH)

def expand_wgsrpd(area: str, mapping: dict[str, Any]) -> list[str]:
    if not mapping:
        return []
    name_to_code = mapping.get("name_to_code", {})
    code_to_name = mapping.get("code_to_name", {})
    children = mapping.get("children", {})
    l4_codes = set(mapping.get("l4_codes", []))

    code = name_to_code.get(area.lower())
    if not code:
        return []

    if code in l4_codes:
        return [code_to_name.get(code, area)]

    out, queue, seen = [], [code], set()
    while queue:
        c = queue.pop(0)
        if c in seen:
            continue
        seen.add(c)
        for ch in children.get(c, []):
            if ch in l4_codes:
                out.append(code_to_name.get(ch, ch))
            else:
                queue.append(ch)

    return dedup_preserve(out)

# ======================================================
# Wikidata (SAFE geographic resolution)
# ======================================================
def wikidata_best_qid(label: str, cache: dict[str, Any]) -> str:
    key = f"qid::{label.lower()}"
    if key in cache:
        return cache[key]

    query = f"""
    SELECT ?item WHERE {{
      SERVICE wikibase:mwapi {{
        bd:serviceParam wikibase:api "EntitySearch" ;
                        wikibase:endpoint "www.wikidata.org" ;
                        mwapi:search "{label}" ;
                        mwapi:language "en" ;
                        mwapi:limit 1 .
        ?item wikibase:apiOutputItem mwapi:item .
      }}
    }}
    """

    try:
        r = requests.get(
            WIKIDATA_SPARQL_URL,
            params={"format": "json", "query": query},
            headers=HEADERS,
            timeout=30,
        )
        r.raise_for_status()
        rows = r.json().get("results", {}).get("bindings", [])
        uri = rows[0]["item"]["value"] if rows else ""
        qid = uri.rsplit("/", 1)[-1] if uri else ""
    except Exception:
        qid = ""

    cache[key] = qid
    time.sleep(WIKIDATA_SLEEP_S)
    return qid

def wikidata_countries_for_place(place: str, cache: dict[str, Any]) -> list[str]:
    key = place.lower()
    if key in cache:
        return cache[key]

    qid = wikidata_best_qid(place, cache)
    if not qid:
        cache[key] = []
        return []

    query = f"""
    SELECT DISTINCT ?countryLabel WHERE {{
      wd:{qid} wdt:P31 ?instance .
      FILTER(?instance != wd:Q4167410)  # not disambiguation
      FILTER EXISTS {{
        wd:{qid} wdt:P31/wdt:P279* wd:Q618123  # geographical object
      }}
      OPTIONAL {{ wd:{qid} wdt:P17 ?c1 . }}
      OPTIONAL {{ wd:{qid} wdt:P131* / wdt:P17 ?c2 . }}
      BIND(COALESCE(?c1, ?c2) AS ?country)
      FILTER(BOUND(?country))
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    """

    try:
        r = requests.get(
            WIKIDATA_SPARQL_URL,
            params={"format": "json", "query": query},
            headers=HEADERS,
            timeout=30,
        )
        r.raise_for_status()
        rows = r.json().get("results", {}).get("bindings", [])
        countries = dedup_preserve([row["countryLabel"]["value"] for row in rows])
    except Exception:
        countries = []

    cache[key] = countries
    time.sleep(WIKIDATA_SLEEP_S)
    return countries

# ======================================================
# POWO extraction (HTML, safe)
# ======================================================
def extract_native_from_taxon_html(urn: str) -> list[str]:
    try:
        r = requests.get(taxon_url_from_urn(urn), headers=HEADERS, timeout=30)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "html.parser")

        for label in LABELS:
            for t in soup.find_all(string=True):
                if label in t.lower():
                    out = []
                    for el in t.parent.next_elements:
                        if isinstance(el, str):
                            low = el.lower()
                            if "introduced into" in low or any(sw in low for sw in STOP_WORDS):
                                return dedup_preserve(out)
                            parts = re.split(r"[|\n,•·]+", el)
                            for p in parts:
                                p = clean_space(p)
                                if p and not any(sw in p.lower() for sw in STOP_WORDS):
                                    out.append(p)
                        elif isinstance(el, Tag) and el.name == "a":
                            txt = clean_space(el.get_text())
                            if txt and not any(sw in txt.lower() for sw in STOP_WORDS):
                                out.append(txt)
                    return dedup_preserve(out)
        return []
    except Exception:
        return []

# ======================================================
# POWO API search
# ======================================================
def powo_search_best_urn(name: str) -> Optional[str]:
    for api in (API_V1_SEARCH, API_V2_SEARCH):
        try:
            r = requests.get(api, headers=HEADERS, params={"q": name}, timeout=30)
            if r.status_code != 200:
                continue
            js = r.json()
            for k in ("results", "data"):
                hits = js.get(k)
                if isinstance(hits, list):
                    for h in hits:
                        urn = h.get("fqId") or h.get("id")
                        if isinstance(urn, str) and "urn:" in urn:
                            return urn
        except Exception:
            continue
    return None

# ======================================================
# Main
# ======================================================
def main():
    df = pd.read_csv(INPUT_CSV)
    if NAME_COL not in df.columns:
        raise KeyError(f"Missing column {NAME_COL}")

    powo_cache = load_json(CACHE_PATH)
    wgsrpd = load_wgsrpd_mapping()
    wikidata_cache = load_json(WIKIDATA_CACHE_PATH)

    powo_native, powo_urls, powo_ids, powo_errors = [], [], [], []

    total = len(df)
    start = time.time()

    for i, name in enumerate(df[NAME_COL].astype(str), start=1):
        key = name.lower()
        cached = powo_cache.get(key)

        if cached and cached.get("powo_native"):
            out = cached
        else:
            urn = powo_search_best_urn(name)
            if not urn:
                out = {
                    "powo_native": "",
                    "powo_url": powo_results_url(name),
                    "powo_taxon_id": "",
                    "powo_error": "not_found",
                }
            else:
                areas = extract_native_from_taxon_html(urn)
                countries = []
                for a in areas:
                    expanded = expand_wgsrpd(a, wgsrpd) or [a]
                    for e in expanded:
                        countries.extend(wikidata_countries_for_place(e, wikidata_cache))
                out = {
                    "powo_native": " | ".join(dedup_preserve(areas)),
                    "powo_url": taxon_url_from_urn(urn),
                    "powo_taxon_id": urn,
                    "powo_error": "" if areas else "no_native_data",
                }

            powo_cache[key] = out
            time.sleep(SLEEP_S)

        powo_native.append(out.get("powo_native", ""))
        powo_urls.append(out.get("powo_url", ""))
        powo_ids.append(out.get("powo_taxon_id", ""))
        powo_errors.append(out.get("powo_error", ""))

        if i == 1 or i % PROGRESS_EVERY_N == 0 or i == total:
            elapsed = int(time.time() - start)
            pct = (i / total) * 100
            print(f"[{i}/{total} | {pct:5.1f}%] elapsed={elapsed}s | {name}")

    df["powo_native"] = powo_native
    df["powo_url"] = powo_urls
    df["powo_taxon_id"] = powo_ids
    df["powo_error"] = powo_errors

    save_json(CACHE_PATH, powo_cache)
    save_json(WIKIDATA_CACHE_PATH, wikidata_cache)

    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    df.to_excel(OUTPUT_XLSX, index=False)

    print("Done.")
    print(f"Rows with POWO native non-empty: {(df['powo_native'].astype(str).str.strip() != '').sum()} of {len(df)}")

if __name__ == "__main__":
    main()
