"""
wfo_id_resolver.py
------------------
Resolve plant names to World Flora Online (WFO) portal taxon IDs (wfo-XXXXXXXXXX).

This version is robust:
1) Tries WFO Plant List Name Matching REST API:
   https://list.worldfloraonline.org/matching_rest.php?input_string=...
2) If that fails / returns non-JSON / no match, falls back to WFO Portal search:
   https://www.worldfloraonline.org/search?query=...

Input:
- plants_gbif_with_native_range.csv (must include gbif_canonicalName; optionally gbif_genus_species, input_name)

Output:
- plants_gbif_with_native_range_plus_wfo_id.csv
- plants_gbif_with_native_range_plus_wfo_id.xlsx

Requirements:
    pip install pandas requests beautifulsoup4 openpyxl
"""

from __future__ import annotations

import json
import time
import re
from pathlib import Path
from typing import Any, Optional, Tuple
from urllib.parse import quote_plus

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ======================================================
# CONFIG
# ======================================================
INPUT_CSV = "plants_gbif_with_native_range.csv"
OUTPUT_CSV = "plants_gbif_with_native_range_plus_wfo_id.csv"
OUTPUT_XLSX = "plants_gbif_with_native_range_plus_wfo_id.xlsx"

# Columns we can try (in order)
CANDIDATE_COLS = ["gbif_canonicalName", "gbif_genus_species", "input_name"]

# WFO endpoints
WFO_MATCHING_URL = "https://list.worldfloraonline.org/matching_rest.php"
WFO_PORTAL_SEARCH_URL = "https://www.worldfloraonline.org/search?query={q}"

HEADERS = {
    "User-Agent": "plant-wfo-id-resolver/1.1 (aron_serebrenik@yahoo.com)",
    "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
}

CACHE_PATH = Path("wfo_id_cache.json")

# Be polite
SLEEP_S = 0.8

# Matching knobs
FUZZY_NAMES = 2
FUZZY_AUTHORS = 0
ACCEPT_SINGLE = True

# Debug: print what happens for the first N rows
DEBUG_FIRST_N = 5


# ======================================================
# Cache helpers
# ======================================================
def load_cache() -> dict[str, Any]:
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    return {}

def save_cache(cache: dict[str, Any]) -> None:
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


# ======================================================
# Utilities
# ======================================================
WFO_ID_RE = re.compile(r"\b(wfo-\d{10})\b", re.IGNORECASE)

def normalize_name(s: str) -> str:
    """Light cleanup: collapse whitespace."""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def extract_wfo_id_from_text(text: str) -> str:
    m = WFO_ID_RE.search(text or "")
    return m.group(1).lower() if m else ""

def safe_get(url: str, params: Optional[dict] = None) -> Tuple[int, str, str]:
    """
    Returns (status_code, content_type, body_text)
    """
    r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    ct = (r.headers.get("Content-Type") or "").lower()
    return r.status_code, ct, r.text


# ======================================================
# Method 1: Plant List matching REST API
# ======================================================
def try_wfo_matching_api(name: str) -> Tuple[str, str]:
    """
    Returns (wfo_id, debug_note)
    wfo_id is the 10-digit portal ID wfo-XXXXXXXXXX (no edition suffix).
    """
    params = {"input_string": name}

    if FUZZY_NAMES and int(FUZZY_NAMES) > 0:
        params["fuzzy_names"] = int(FUZZY_NAMES)
    if FUZZY_AUTHORS and int(FUZZY_AUTHORS) > 0:
        params["fuzzy_authors"] = int(FUZZY_AUTHORS)
    if ACCEPT_SINGLE:
        params["accept_single_candidate"] = "true"

    status, ct, body = safe_get(WFO_MATCHING_URL, params=params)

    # Must be JSON-ish
    if "json" not in ct:
        # sometimes servers mislabel; try to parse anyway if it looks like JSON
        if not body.lstrip().startswith("{"):
            return "", f"matching_api non-json content-type={ct} status={status}"

    try:
        data = json.loads(body)
    except Exception:
        return "", f"matching_api json-parse-failed content-type={ct} status={status}"

    # According to the WFO matching API docs, response has 'match' and 'candidates'.
    # Keys may be 'wfo_id' (REST docs) or 'wfoId' (GraphQL style).
    def _get_id(obj: dict) -> str:
        if not isinstance(obj, dict):
            return ""
        # Try a few variants
        for k in ("wfo_id", "wfoId", "wfoID", "wfoid"):
            v = obj.get(k)
            if isinstance(v, str):
                # Could be "wfo-0000982612-2025-06" or "wfo-0000982612"
                base = extract_wfo_id_from_text(v)
                if base:
                    return base
        # Sometimes the full object string contains it
        as_text = json.dumps(obj)
        base = extract_wfo_id_from_text(as_text)
        return base

    m = data.get("match")
    wfo_id = _get_id(m)

    if not wfo_id:
        cands = data.get("candidates") or []
        if isinstance(cands, list) and len(cands) == 1 and ACCEPT_SINGLE:
            wfo_id = _get_id(cands[0])

    if wfo_id:
        return wfo_id, "matching_api"

    return "", "matching_api no-match"


# ======================================================
# Method 2: WFO portal search HTML fallback
# ======================================================
def try_wfo_portal_search(name: str) -> Tuple[str, str]:
    """
    Parses https://www.worldfloraonline.org/search?query=...
    and extracts the first /taxon/wfo-XXXXXXXXXX occurrence.
    Returns (wfo_id, debug_note)
    """
    url = WFO_PORTAL_SEARCH_URL.format(q=quote_plus(name))
    status, ct, body = safe_get(url)

    if status != 200:
        return "", f"portal_search status={status}"

    # Parse HTML and look for /taxon/wfo-##########
    soup = BeautifulSoup(body, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/taxon/" in href:
            wfo_id = extract_wfo_id_from_text(href)
            if wfo_id:
                return wfo_id, "portal_search"

    # Fallback: regex scan entire page
    wfo_id = extract_wfo_id_from_text(body)
    if wfo_id:
        return wfo_id, "portal_search(regex)"

    return "", "portal_search no-match"


# ======================================================
# Combined resolver
# ======================================================
def resolve_wfo_id(name: str, cache: dict[str, Any]) -> Tuple[str, str]:
    """
    Returns (wfo_id, method)
    Cached by normalized name.
    """
    name = normalize_name(name)
    if not name:
        return "", "empty"

    cache_key = name.lower()
    if cache_key in cache:
        v = cache[cache_key]
        if isinstance(v, dict):
            return v.get("wfo_id", ""), v.get("method", "cached")
        if isinstance(v, str):
            return v, "cached"
        return "", "cached"

    # 1) matching API
    wfo_id, method = try_wfo_matching_api(name)
    if wfo_id:
        cache[cache_key] = {"wfo_id": wfo_id, "method": method}
        return wfo_id, method

    # 2) portal search fallback
    wfo_id, method2 = try_wfo_portal_search(name)
    if wfo_id:
        cache[cache_key] = {"wfo_id": wfo_id, "method": method2}
        return wfo_id, method2

    cache[cache_key] = {"wfo_id": "", "method": "no-match"}
    return "", "no-match"


# ======================================================
# Main
# ======================================================
def main():
    df = pd.read_csv(INPUT_CSV)

    for col in CANDIDATE_COLS:
        if col not in df.columns:
            df[col] = ""

    cache = load_cache()

    out_ids: list[str] = []
    out_method: list[str] = []
    out_query: list[str] = []

    for i, r in df.iterrows():
        # Build candidate list from available columns
        candidates = []
        for col in CANDIDATE_COLS:
            v = r.get(col)
            if isinstance(v, str) and v.strip():
                candidates.append(v.strip())
        # De-dup preserve order
        seen = set()
        candidates = [c for c in candidates if not (c.lower() in seen or seen.add(c.lower()))]

        wfo_id = ""
        method = "no-match"
        used = ""

        for cand in candidates:
            wfo_id, method = resolve_wfo_id(cand, cache)
            used = cand
            if wfo_id:
                break

        out_ids.append(wfo_id)
        out_method.append(method)
        out_query.append(used)

        # Debug print for first few rows
        if i < DEBUG_FIRST_N:
            print(f"[DEBUG] row={i} query='{used}' -> wfo_id='{wfo_id}' method={method}")

        time.sleep(SLEEP_S)

    df["wfo_taxon_id"] = out_ids
    df["wfo_id_method"] = out_method
    df["wfo_query_used"] = out_query
    df["wfo_url"] = df["wfo_taxon_id"].apply(lambda x: f"https://www.worldfloraonline.org/taxon/{x}" if isinstance(x, str) and x.startswith("wfo-") else "")

    save_cache(cache)

    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    df.to_excel(OUTPUT_XLSX, index=False)

    resolved = (df["wfo_taxon_id"].astype(str).str.startswith("wfo-")).sum()

    print(f"Resolved WFO IDs: {resolved} of {len(df)}")
    print(f"Wrote: {OUTPUT_CSV}")
    print(f"Wrote: {OUTPUT_XLSX}")
    print(f"Cache: {CACHE_PATH.resolve()}")

if __name__ == "__main__":
    main()
