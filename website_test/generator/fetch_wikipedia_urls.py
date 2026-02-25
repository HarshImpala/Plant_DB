"""
Fetch Wikipedia URLs for plants using Wikidata API.

This script searches Wikidata for each plant by canonical name,
then retrieves the English Wikipedia URL from the sitelinks.
"""

import sqlite3
import requests
import time
import json
import re
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "plants.db"
CACHE_PATH = DATA_DIR / "wikipedia_cache.json"

WIKIDATA_API = "https://www.wikidata.org/w/api.php"

HEADERS = {
    "User-Agent": "plant-encyclopedia/1.0 (botanical garden project)",
    "Accept": "application/json",
}


def load_cache() -> dict:
    """Load cache from disk."""
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    return {}


def save_cache(cache: dict) -> None:
    """Save cache to disk."""
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def api_request_with_retry(params: dict, max_retries: int = 3) -> dict | None:
    """Make API request with retry on rate limit."""
    for attempt in range(max_retries):
        try:
            r = requests.get(WIKIDATA_API, params=params, headers=HEADERS, timeout=30)
            if r.status_code == 429:
                wait_time = 2 ** (attempt + 1)  # Exponential backoff: 2, 4, 8 seconds
                print(f"  Rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** (attempt + 1)
                print(f"  Request error, retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"  Request failed after {max_retries} attempts: {e}")
                return None
    return None


def ensure_columns(conn):
    """Ensure wikipedia URL columns exist and legacy column is migrated."""
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(plants)")
    columns = {row[1] for row in cursor.fetchall()}
    if "wikipedia_url" in columns and "wikipedia_url_english" not in columns:
        cursor.execute("ALTER TABLE plants RENAME COLUMN wikipedia_url TO wikipedia_url_english")
        columns.remove("wikipedia_url")
        columns.add("wikipedia_url_english")
    if "wikipedia_url_english" not in columns:
        cursor.execute("ALTER TABLE plants ADD COLUMN wikipedia_url_english TEXT")
    if "wikipedia_url_hungarian" not in columns:
        cursor.execute("ALTER TABLE plants ADD COLUMN wikipedia_url_hungarian TEXT")
    conn.commit()


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def search_wikidata(query: str, limit: int = 5) -> list[dict]:
    """Search Wikidata for entities by name."""
    params = {
        "action": "wbsearchentities",
        "search": query,
        "language": "en",
        "format": "json",
        "limit": max(1, min(int(limit), 20)),
    }
    data = api_request_with_retry(params)
    if not data:
        return []
    return data.get("search", []) or []


def get_wikipedia_urls(qid: str) -> tuple[str | None, str | None]:
    """Get English and Hungarian Wikipedia URLs for a Wikidata entity."""
    params = {
        "action": "wbgetentities",
        "ids": qid,
        "props": "sitelinks/urls",
        "format": "json",
    }
    data = api_request_with_retry(params)
    if data:
        entity = data.get("entities", {}).get(qid, {})
        sitelinks = entity.get("sitelinks", {})
        enwiki_url = sitelinks.get("enwiki", {}).get("url")
        huwiki_url = sitelinks.get("huwiki", {}).get("url")
        return enwiki_url, huwiki_url
    return None, None


def _score_hit(hit: dict, canonical_name: str, scientific_name: str, family: str | None, genus: str | None) -> int:
    """Heuristic score to choose a better Wikidata candidate."""
    score = 0
    label = _norm(hit.get("label", ""))
    description = _norm(hit.get("description", ""))
    canonical = _norm(canonical_name or "")
    scientific = _norm(scientific_name or "")

    plant_terms = ("plant", "species", "genus", "tree", "shrub", "flower", "flora", "botan")
    if any(term in description for term in plant_terms):
        score += 25
    if "disambiguation" in description:
        score -= 40

    if canonical and label == canonical:
        score += 60
    elif canonical and label.startswith(canonical):
        score += 30
    elif canonical and canonical in label:
        score += 15

    if scientific and label == scientific:
        score += 45
    elif scientific and scientific in label:
        score += 20

    if family and _norm(family) in description:
        score += 8
    if genus and _norm(genus) in description:
        score += 8

    return score


def pick_best_hit(hits: list[dict], canonical_name: str, scientific_name: str, family: str | None, genus: str | None) -> dict | None:
    if not hits:
        return None

    ranked = sorted(
        ((hit, _score_hit(hit, canonical_name, scientific_name, family, genus)) for hit in hits),
        key=lambda x: x[1],
        reverse=True,
    )
    best_hit, best_score = ranked[0]
    # Avoid weak/ambiguous matches.
    if best_score < 10:
        return None
    return best_hit


def main():
    """Main function to fetch Wikipedia URLs for all plants."""
    print("Fetching Wikipedia URLs for plants...")
    print(f"Database: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    ensure_columns(conn)

    # Get all plants
    cursor.execute("SELECT id, canonical_name, scientific_name, family, genus FROM plants")
    plants = cursor.fetchall()
    print(f"Found {len(plants)} plants")

    cache = load_cache()
    updated_count = 0
    found_count = 0

    for i, plant in enumerate(plants):
        plant_id = plant["id"]
        canonical_name = plant["canonical_name"] or ""
        scientific_name = plant["scientific_name"] or ""
        family = plant["family"]
        genus = plant["genus"]
        # Try canonical name first, then scientific name
        search_name = canonical_name or scientific_name

        if not search_name:
            continue

        # Check cache
        cache_key = search_name.lower().strip()
        if cache_key in cache:
            cached_value = cache[cache_key]
            if cached_value == "NOT_FOUND":
                wikipedia_url_english = None
                wikipedia_url_hungarian = None
            elif isinstance(cached_value, dict):
                wikipedia_url_english = cached_value.get("en")
                wikipedia_url_hungarian = cached_value.get("hu")
            else:
                wikipedia_url_english = cached_value
                wikipedia_url_hungarian = None
        else:
            wikipedia_url_english = None
            wikipedia_url_hungarian = None

            query_candidates = []
            for q in (
                canonical_name,
                scientific_name,
                f"{canonical_name} plant" if canonical_name else "",
                f"{scientific_name} plant" if scientific_name else "",
            ):
                q = (q or "").strip()
                if q and q not in query_candidates:
                    query_candidates.append(q)

            for query in query_candidates:
                hits = search_wikidata(query, limit=7)
                hit = pick_best_hit(hits, canonical_name, scientific_name, family, genus)
                if not hit:
                    continue
                qid = hit.get("id")
                if not qid:
                    continue
                wikipedia_url_english, wikipedia_url_hungarian = get_wikipedia_urls(qid)
                if wikipedia_url_english or wikipedia_url_hungarian:
                    break

            if wikipedia_url_english or wikipedia_url_hungarian:
                cache[cache_key] = {
                    "en": wikipedia_url_english,
                    "hu": wikipedia_url_hungarian,
                }
            else:
                cache[cache_key] = "NOT_FOUND"

            time.sleep(0.5)  # Rate limiting - be gentle with the API

        if wikipedia_url_english or wikipedia_url_hungarian:
            cursor.execute(
                """
                UPDATE plants
                SET wikipedia_url_english = ?, wikipedia_url_hungarian = ?
                WHERE id = ?
                """,
                (wikipedia_url_english, wikipedia_url_hungarian, plant_id),
            )
            found_count += 1

        updated_count += 1

        if (i + 1) % 20 == 0:
            print(f"  Processed {i + 1}/{len(plants)} plants ({found_count} with Wikipedia)...")
            save_cache(cache)
            conn.commit()

    save_cache(cache)
    conn.commit()
    conn.close()

    print(f"\n=== Complete ===")
    print(f"Processed: {updated_count} plants")
    print(f"Wikipedia URLs found: {found_count}")


if __name__ == "__main__":
    main()
