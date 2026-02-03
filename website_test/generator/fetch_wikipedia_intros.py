"""
Fetch Wikipedia introductions for plants.

This script fetches the first paragraph from Wikipedia for each plant
that has a Wikipedia URL and stores it in the description field.
"""

import sqlite3
import requests
import time
import json
import re
from pathlib import Path
from urllib.parse import unquote, urlparse

# Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "plants.db"
CACHE_PATH = DATA_DIR / "wikipedia_intro_cache.json"

WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"

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


def get_page_title_from_url(wikipedia_url: str) -> str | None:
    """Extract the page title from a Wikipedia URL."""
    parsed = urlparse(wikipedia_url)
    if '/wiki/' in parsed.path:
        title = parsed.path.split('/wiki/')[-1]
        return unquote(title)
    return None


def api_request_with_retry(params: dict, max_retries: int = 3) -> dict | None:
    """Make API request with retry on rate limit."""
    for attempt in range(max_retries):
        try:
            r = requests.get(WIKIPEDIA_API, params=params, headers=HEADERS, timeout=30)
            if r.status_code == 429:
                wait_time = 2 ** (attempt + 1)
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
                print(f"  Request failed: {e}")
                return None
    return None


def clean_text(text: str) -> str:
    """Clean Wikipedia text by removing references and extra whitespace."""
    # Remove reference markers like [1], [2], etc.
    text = re.sub(r'\[\d+\]', '', text)
    # Remove pronunciation guides in parentheses at the start
    text = re.sub(r'^\s*\([^)]*pronunciation[^)]*\)\s*', '', text, flags=re.IGNORECASE)
    # Clean up multiple spaces
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def get_page_intro(page_title: str) -> str | None:
    """Get the introduction/first paragraph from a Wikipedia page."""
    params = {
        "action": "query",
        "titles": page_title,
        "prop": "extracts",
        "exintro": True,  # Only get intro section
        "explaintext": True,  # Plain text, no HTML
        "exsectionformat": "plain",
        "format": "json",
    }

    data = api_request_with_retry(params)
    if not data:
        return None

    pages = data.get("query", {}).get("pages", {})
    for page_id, page_data in pages.items():
        if page_id == "-1":
            return None
        extract = page_data.get("extract", "")
        if extract:
            # Clean up the text
            extract = clean_text(extract)
            # Get first paragraph (up to first double newline or reasonable length)
            paragraphs = extract.split('\n\n')
            if paragraphs:
                first_para = paragraphs[0].strip()
                # If first paragraph is too short, try to include more
                if len(first_para) < 100 and len(paragraphs) > 1:
                    first_para = '\n\n'.join(paragraphs[:2]).strip()
                return first_para
        return None

    return None


def main():
    """Main function to fetch Wikipedia introductions for all plants."""
    print("Fetching Wikipedia introductions for plants...")
    print(f"Database: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get plants with Wikipedia URLs
    cursor.execute("""
        SELECT id, canonical_name, scientific_name, wikipedia_url, description
        FROM plants
        WHERE wikipedia_url IS NOT NULL
    """)
    plants = cursor.fetchall()
    print(f"Found {len(plants)} plants with Wikipedia URLs")

    cache = load_cache()
    fetched_count = 0
    skipped_count = 0
    failed_count = 0

    for i, plant in enumerate(plants):
        plant_id = plant["id"]
        canonical_name = plant["canonical_name"] or plant["scientific_name"]
        wikipedia_url = plant["wikipedia_url"]
        existing_desc = plant["description"]

        # Skip if already has a description
        if existing_desc:
            skipped_count += 1
            continue

        # Get page title from URL
        page_title = get_page_title_from_url(wikipedia_url)
        if not page_title:
            print(f"  Could not extract title from: {wikipedia_url}")
            failed_count += 1
            continue

        cache_key = page_title.lower()

        # Check cache
        if cache_key in cache:
            intro = cache[cache_key]
            if intro == "NO_INTRO":
                skipped_count += 1
                continue
        else:
            # Fetch from Wikipedia
            intro = get_page_intro(page_title)

            if intro:
                cache[cache_key] = intro
            else:
                cache[cache_key] = "NO_INTRO"
                skipped_count += 1
                time.sleep(0.3)
                continue

            time.sleep(0.5)  # Rate limiting

        if intro and intro != "NO_INTRO":
            # Update database
            cursor.execute(
                "UPDATE plants SET description = ? WHERE id = ?",
                (intro, plant_id)
            )
            fetched_count += 1
            print(f"  [{i+1}/{len(plants)}] Fetched: {canonical_name}")

        # Save progress periodically
        if (i + 1) % 20 == 0:
            save_cache(cache)
            conn.commit()
            print(f"  Progress: {i + 1}/{len(plants)} ({fetched_count} fetched)")

    save_cache(cache)
    conn.commit()
    conn.close()

    print(f"\n=== Complete ===")
    print(f"Fetched: {fetched_count}")
    print(f"Skipped (no intro/already had): {skipped_count}")
    print(f"Failed: {failed_count}")


if __name__ == "__main__":
    main()
