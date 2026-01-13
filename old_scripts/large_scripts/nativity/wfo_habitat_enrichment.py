"""
wfo_native_enrichment.py
------------------------
Enrich plants with Native geographic information from World Flora Online (WFO)
using WFO portal taxon IDs (wfo-XXXXXXXXXX).

Final output columns added:
- wfo_native_areas_found_in
- wfo_native_countries
- wfo_url

Columns REMOVED from output (if present):
- gbif_synonym_count
- gbif_english_name_count
- wfo_id_method
"""

from __future__ import annotations

import json
import time
import re
import warnings
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag, NavigableString
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# ======================================================
# CONFIG
# ======================================================
INPUT_CSV = "plants_gbif_with_native_range_plus_wfo_id.csv"
OUTPUT_CSV = "plants_with_native_plus_wfo.csv"
OUTPUT_XLSX = "plants_with_native_plus_wfo.xlsx"

WFO_BASE = "https://www.worldfloraonline.org"
HEADERS = {"User-Agent": "plant-wfo-native/2.4 (aron_serebrenik@yahoo.com)"}

CACHE_PATH = Path("wfo_native_cache.json")
WGSRPD_CACHE_PATH = Path("wgsrpd_mapping_cache.json")
WIKIDATA_CACHE_PATH = Path("wikidata_country_cache.json")

SLEEP_S = 1.0
WIKIDATA_SLEEP_S = 0.2
WIKIDATA_TIMEOUT_S = 30

PROGRESS_EVERY_N = 5

RESUME_ONLY_FAILED = True
RETRY_BLANK_COUNTRIES_TOO = True

warnings.simplefilter("ignore", InsecureRequestWarning)

LABEL_NATIVE_VARIANTS = [
    "Native distribution",
    "Native Distribution",
    "Native range",
    "Native Range",
]

STOP_PHRASES = (
    "introduced into",
    "references",
    "reference",
    "bibliography",
    "citation",
    "citations",
)

# ======================================================
# Cache helpers
# ======================================================
def load_json_cache(path: Path) -> dict[str, Any]:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}

def save_json_cache(path: Path, cache: dict[str, Any]) -> None:
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

# ======================================================
# Utilities
# ======================================================
def clean_space(s) -> str:
    if s is None:
        return ""
    if isinstance(s, float) and pd.isna(s):
        return ""
    if not isinstance(s, str):
        s = str(s)
    return re.sub(r"\s+", " ", s.replace("\u00a0", " ")).strip()

def norm(s: str) -> str:
    return clean_space(s).lower()

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

def format_eta(elapsed_s: float, done: int, total: int) -> str:
    if done <= 0:
        return "ETA: unknown"
    rate = elapsed_s / done
    remaining = rate * (total - done)
    m, s = divmod(int(remaining), 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"ETA: {h}h {m}m"
    if m > 0:
        return f"ETA: {m}m {s}s"
    return f"ETA: {s}s"

# ======================================================
# Robust WFO fetching
# ======================================================
def make_wfo_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=6,
        connect=6,
        read=6,
        status=6,
        backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    return s

WFO_SESSION = make_wfo_session()

def fetch_wfo(url: str) -> str:
    r = WFO_SESSION.get(url, timeout=(15, 75), verify=False)
    r.raise_for_status()
    return r.text

# ======================================================
# WFO extraction (robust Found-in)
# ======================================================
_REF_TOKEN_RE = re.compile(r"^\s*(\[\d+\]|\d+)\s*$")
_DOI_RE = re.compile(r"\bdoi\s*:\s*", re.IGNORECASE)

def _looks_like_reference_token(token: str) -> bool:
    t = clean_space(token)
    if not t:
        return True
    if _REF_TOKEN_RE.match(t):
        return True
    if _DOI_RE.search(t):
        return True
    if t.lower().startswith(("http://", "https://")):
        return True
    return False

def _looks_like_area_token(tok: str) -> bool:
    t = clean_space(tok)
    if not t or _looks_like_reference_token(t):
        return False
    if not re.search(r"[A-Za-z]", t):
        return False
    if len(t) < 3:
        return False
    if any(p in t.lower() for p in STOP_PHRASES):
        return False
    return True

def extract_native_found_in_areas_only(soup: BeautifulSoup) -> list[str]:
    started = False
    collected: list[str] = []

    for el in soup.descendants:
        if isinstance(el, NavigableString):
            t = norm(str(el))
            if not started and "found in" in t:
                started = True
                continue
            if started and any(p in t for p in STOP_PHRASES):
                break
            if started:
                raw = clean_space(str(el))
                if _looks_like_area_token(raw):
                    collected.append(raw)

        elif isinstance(el, Tag) and started:
            if el.name in {"a", "li"}:
                txt = clean_space(el.get_text(" ", strip=True))
                if _looks_like_area_token(txt):
                    collected.append(txt)

    return dedup_preserve(collected)

# ======================================================
# Wikidata (country mapping)
# ======================================================
def load_wikidata_cache() -> dict[str, Any]:
    return load_json_cache(WIKIDATA_CACHE_PATH)

def wikidata_country_for_place(place: str, cache: dict[str, Any]) -> list[str]:
    key = clean_space(place).lower()
    if key in cache:
        return cache[key]

    escaped = place.replace("\\", "\\\\").replace('"', '\\"')
    query = f"""
    SELECT ?countryLabel WHERE {{
      ?place rdfs:label "{escaped}"@en .
      OPTIONAL {{ ?place wdt:P17 ?c1 . }}
      OPTIONAL {{ ?place wdt:P131* / wdt:P17 ?c2 . }}
      BIND(COALESCE(?c1, ?c2) AS ?country)
      FILTER(BOUND(?country))
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }} LIMIT 5
    """

    try:
        r = requests.get(
            "https://query.wikidata.org/sparql",
            params={"format": "json", "query": query},
            timeout=WIKIDATA_TIMEOUT_S,
        )
        r.raise_for_status()
        rows = r.json()["results"]["bindings"]
        countries = dedup_preserve(row["countryLabel"]["value"] for row in rows)
    except Exception:
        countries = []

    cache[key] = countries
    time.sleep(WIKIDATA_SLEEP_S)
    return countries

# ======================================================
# Resume logic
# ======================================================
REQUIRED_CACHE_KEYS = {"wfo_native_areas_found_in", "wfo_native_countries", "wfo_url"}

def cache_entry_succeeded(entry: Any) -> bool:
    if not isinstance(entry, dict):
        return False
    if not REQUIRED_CACHE_KEYS.issubset(entry):
        return False
    if entry.get("wfo_error"):
        return False
    if RETRY_BLANK_COUNTRIES_TOO:
        return bool(clean_space(entry.get("wfo_native_countries")))
    return True

# ======================================================
# Main
# ======================================================
def main():
    df = pd.read_csv(INPUT_CSV)

    wfo_cache = load_json_cache(CACHE_PATH)
    wd_cache = load_wikidata_cache()

    areas_col, countries_col, urls = [], [], []

    total = len(df)
    start = time.time()

    for i, wfo_id in enumerate(df["wfo_taxon_id"].astype(str), start=1):
        wfo_id = wfo_id.strip().lower()
        cached = wfo_cache.get(wfo_id)

        if cached and cache_entry_succeeded(cached):
            out = cached
        else:
            url = f"{WFO_BASE}/taxon/{wfo_id}"
            try:
                soup = BeautifulSoup(fetch_wfo(url), "html.parser")
                areas = extract_native_found_in_areas_only(soup)
                countries = []
                for a in areas:
                    countries.extend(wikidata_country_for_place(a, wd_cache))
                out = {
                    "wfo_url": url,
                    "wfo_native_areas_found_in": " | ".join(areas),
                    "wfo_native_countries": " | ".join(dedup_preserve(countries)),
                }
            except Exception as e:
                out = {
                    "wfo_url": url,
                    "wfo_native_areas_found_in": "",
                    "wfo_native_countries": "",
                    "wfo_error": str(e),
                }
            wfo_cache[wfo_id] = out
            time.sleep(SLEEP_S)

        areas_col.append(out.get("wfo_native_areas_found_in", ""))
        countries_col.append(out.get("wfo_native_countries", ""))
        urls.append(out.get("wfo_url", ""))

        if i == 1 or i % PROGRESS_EVERY_N == 0 or i == total:
            elapsed = time.time() - start
            print(f"[{i}/{total} | {(i/total)*100:5.1f}%] elapsed={int(elapsed)}s | {format_eta(elapsed, i, total)}")

    # Attach output columns
    df["wfo_native_areas_found_in"] = areas_col
    df["wfo_native_countries"] = countries_col
    df["wfo_url"] = urls

    # 🔥 REMOVE UNWANTED COLUMNS (if present)
    df.drop(
        columns=[
            "gbif_synonym_count",
            "gbif_english_name_count",
            "wfo_id_method",
        ],
        errors="ignore",
        inplace=True,
    )

    save_json_cache(CACHE_PATH, wfo_cache)
    save_json_cache(WIKIDATA_CACHE_PATH, wd_cache)

    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    df.to_excel(OUTPUT_XLSX, index=False)

    print("Done.")
    print(f"Wrote: {OUTPUT_CSV}")
    print(f"Wrote: {OUTPUT_XLSX}")

if __name__ == "__main__":
    main()
