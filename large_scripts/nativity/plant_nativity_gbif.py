"""
gbif_native_range.py
--------------------
Standalone script to infer a plant's native range using GBIF occurrence records.

Features:
- Uses GBIF occurrence/search with establishmentMeans=native
- Exponential backoff on HTTP 429 (rate limiting)
- Caches results locally (JSON)
- Conservative sampling (avoids hammering GBIF)
- Outputs CSV + XLSX

Requirements:
    pip install pandas requests openpyxl
"""

from __future__ import annotations

import json
import time
import random
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.exceptions import HTTPError

# =========================
# CONFIG
# =========================
INPUT_GBIF_MATCHED_CSV = r"C:\Users\aron_\PycharmProjects\obsidian_app\PostgreSQL_DB\large_scripts\plants_gbif_matched.csv"
USAGEKEY_COL = "gbif_usageKey"

OUTPUT_CSV = "plants_gbif_with_native_range.csv"
OUTPUT_XLSX = "plants_gbif_with_native_range.xlsx"

CACHE_PATH = Path("../gbif_native_cache.json")

GBIF_OCCURRENCE_URL = "https://api.gbif.org/v1/occurrence/search"
HEADERS = {
    "User-Agent": "plant-native-range/1.0 (aron_serebrenik@yahoo.com)"
}

# ---- polite defaults ----
LIMIT = 300
MAX_PAGES = 3          # sample up to 900 records max
BASE_SLEEP = 0.5       # base backoff sleep
MAX_SLEEP = 30.0
INTER_REQUEST_SLEEP = 0.4   # delay between successful requests


# =========================
# Cache helpers
# =========================
def load_cache() -> dict[str, Any]:
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    return {}

def save_cache(cache: dict[str, Any]) -> None:
    CACHE_PATH.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# =========================
# GBIF-safe GET with backoff
# =========================
def gbif_get_with_backoff(
    url: str,
    params: dict,
    headers: dict,
    max_retries: int = 5,
):
    """
    Perform a GET request with exponential backoff on HTTP 429.
    """
    for attempt in range(max_retries):
        r = requests.get(url, params=params, headers=headers, timeout=30)
        try:
            r.raise_for_status()
            return r
        except HTTPError:
            if r.status_code == 429:
                sleep_s = min(
                    MAX_SLEEP,
                    BASE_SLEEP * (2 ** attempt) + random.uniform(0, 0.5),
                )
                print(
                    f"[GBIF 429] Sleeping {sleep_s:.1f}s "
                    f"(attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(sleep_s)
                continue
            raise
    raise RuntimeError("GBIF rate limit exceeded after retries")


# =========================
# Native range inference
# =========================
def gbif_native_range_cached(
    usage_key: Any,
    cache: dict[str, Any],
) -> dict[str, Any]:
    """
    Infer native range from GBIF occurrence records where establishmentMeans=native.

    Returns:
      {
        countries: list[str],
        country_codes: list[str],
        record_count_sampled: int,
        gbif_total_native_records: int | None
      }
    """
    if usage_key is None or pd.isna(usage_key) or str(usage_key).strip() == "":
        return {}

    try:
        uk = int(float(usage_key))
    except Exception:
        return {}

    cache_key = str(uk)
    if cache_key in cache:
        return cache[cache_key]

    countries: dict[str, set[str]] = {}
    offset = 0
    page = 0
    sampled = 0
    gbif_total = None

    while page < MAX_PAGES:
        params = {
            "taxonKey": uk,
            "establishmentMeans": "native",
            "limit": LIMIT,
            "offset": offset,
        }

        r = gbif_get_with_backoff(
            GBIF_OCCURRENCE_URL,
            params=params,
            headers=HEADERS,
        )
        data = r.json()

        if gbif_total is None:
            gbif_total = data.get("count")

            # Early exit for extremely widespread taxa
            if isinstance(gbif_total, int) and gbif_total > 2000:
                break

        results = data.get("results", []) or []
        sampled += len(results)

        for rec in results:
            country = rec.get("country")
            code = rec.get("countryCode")
            if country:
                countries.setdefault(country, set())
                if code:
                    countries[country].add(code)

        if offset + LIMIT >= (data.get("count") or 0):
            break

        offset += LIMIT
        page += 1
        time.sleep(INTER_REQUEST_SLEEP)

    out = {
        "countries": sorted(countries.keys()),
        "country_codes": sorted({cc for v in countries.values() for cc in v if cc}),
        "record_count_sampled": sampled,
        "gbif_total_native_records": gbif_total,
    }

    cache[cache_key] = out
    return out


def native_confidence(total_native_records: Any) -> str:
    """
    Conservative confidence heuristic based on GBIF total native record count.
    """
    try:
        n = int(total_native_records)
    except Exception:
        return "unknown"

    if n >= 200:
        return "high"
    if n >= 50:
        return "medium"
    if n >= 10:
        return "low"
    if n >= 1:
        return "very_low"
    return "none"


# =========================
# Main
# =========================
def main() -> None:
    df = pd.read_csv(INPUT_GBIF_MATCHED_CSV)

    if USAGEKEY_COL not in df.columns:
        raise KeyError(
            f"Missing column '{USAGEKEY_COL}' in input CSV. "
            f"Found columns: {list(df.columns)}"
        )

    cache = load_cache()

    native_countries = []
    native_codes = []
    native_total = []
    native_sampled = []
    native_conf = []

    for uk in df[USAGEKEY_COL]:
        native = gbif_native_range_cached(uk, cache)

        countries = native.get("countries", [])
        codes = native.get("country_codes", [])
        total = native.get("gbif_total_native_records", None)
        sampled = native.get("record_count_sampled", 0)

        native_countries.append(" | ".join(countries))
        native_codes.append(" | ".join(codes))
        native_total.append(total if total is not None else 0)
        native_sampled.append(sampled)
        native_conf.append(native_confidence(total))

    df["gbif_native_countries"] = native_countries
    df["gbif_native_country_codes"] = native_codes
    df["gbif_native_total_records"] = native_total
    df["gbif_native_sampled_records"] = native_sampled
    df["gbif_native_confidence"] = native_conf

    save_cache(cache)

    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    df.to_excel(OUTPUT_XLSX, index=False)

    found = (df["gbif_native_total_records"].fillna(0).astype(int) > 0).sum()

    print(f"Input rows: {len(df)}")
    print(f"Native range inferred: {found} rows")
    print(f"Wrote: {OUTPUT_CSV}")
    print(f"Wrote: {OUTPUT_XLSX}")
    print(f"Cache file: {CACHE_PATH.resolve()}")


if __name__ == "__main__":
    main()
