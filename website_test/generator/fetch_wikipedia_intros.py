"""
Fetch English and Hungarian Wikipedia introductions for plants.

If no Hungarian page intro is available, this script attempts to machine
translate the English intro and marks it with a translation flag.
"""

import hashlib
import json
import sqlite3
import time
from pathlib import Path
import re
from urllib.parse import unquote, urlparse

import requests

# Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "plants.db"
CACHE_PATH = DATA_DIR / "wikipedia_intro_cache.json"

WIKIPEDIA_API_EN = "https://en.wikipedia.org/w/api.php"
WIKIPEDIA_API_HU = "https://hu.wikipedia.org/w/api.php"
MYMEMORY_API = "https://api.mymemory.translated.net/get"
TRANSLATION_MAX_CHARS = 450

HEADERS = {
    "User-Agent": "plant-encyclopedia/1.0 (botanical garden project)",
    "Accept": "application/json",
}


def is_invalid_translation_text(candidate: str) -> bool:
    candidate_lc = (candidate or "").strip().lower()
    if not candidate_lc:
        return True
    error_markers = (
        "query length limit exceeded",
        "max allowed query",
        "too many requests",
        "invalid language pair",
        "null",
    )
    return any(marker in candidate_lc for marker in error_markers)


def load_cache() -> dict:
    """Load cache from disk."""
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    return {}


def save_cache(cache: dict) -> None:
    """Save cache to disk."""
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def ensure_columns(conn):
    """Ensure intro columns exist and legacy column is migrated."""
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(plants)")
    columns = {row[1] for row in cursor.fetchall()}

    if "description" in columns and "description_english" not in columns:
        cursor.execute("ALTER TABLE plants RENAME COLUMN description TO description_english")
        columns.remove("description")
        columns.add("description_english")

    if "wikipedia_url" in columns and "wikipedia_url_english" not in columns:
        cursor.execute("ALTER TABLE plants RENAME COLUMN wikipedia_url TO wikipedia_url_english")
        columns.remove("wikipedia_url")
        columns.add("wikipedia_url_english")

    add_columns = [
        ("wikipedia_url_english", "TEXT"),
        ("wikipedia_url_hungarian", "TEXT"),
        ("description_english", "TEXT"),
        ("description_hungarian", "TEXT"),
        ("description_hungarian_is_translated", "INTEGER DEFAULT 0"),
    ]
    for column_name, definition in add_columns:
        if column_name not in columns:
            cursor.execute(f"ALTER TABLE plants ADD COLUMN {column_name} {definition}")

    cursor.execute(
        "UPDATE plants SET description_hungarian_is_translated = 0 "
        "WHERE description_hungarian_is_translated IS NULL"
    )
    conn.commit()


def get_page_title_from_url(wikipedia_url: str) -> str | None:
    """Extract the page title from a Wikipedia URL."""
    parsed = urlparse(wikipedia_url)
    if "/wiki/" in parsed.path:
        title = parsed.path.split("/wiki/")[-1]
        return unquote(title)
    return None


def api_request_with_retry(url: str, params: dict, max_retries: int = 3) -> dict | None:
    """Make API request with retry on rate limit."""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if response.status_code == 429:
                wait_time = 2 ** (attempt + 1)
                print(f"  Rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as exc:
            if attempt < max_retries - 1:
                wait_time = 2 ** (attempt + 1)
                print(f"  Request error, retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"  Request failed: {exc}")
                return None
    return None


def clean_text(text: str) -> str:
    """Clean Wikipedia text by removing references and extra whitespace."""
    text = re.sub(r"\[\d+\]", "", text)
    text = re.sub(r"^\s*\([^)]*pronunciation[^)]*\)\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def get_page_intro(page_title: str, lang: str) -> str | None:
    """Get the introduction/first paragraph from a Wikipedia page."""
    api_url = WIKIPEDIA_API_HU if lang == "hu" else WIKIPEDIA_API_EN
    params = {
        "action": "query",
        "titles": page_title,
        "prop": "extracts",
        "exintro": True,
        "explaintext": True,
        "exsectionformat": "plain",
        "format": "json",
    }

    data = api_request_with_retry(api_url, params)
    if not data:
        return None

    pages = data.get("query", {}).get("pages", {})
    for page_id, page_data in pages.items():
        if page_id == "-1":
            return None
        extract = page_data.get("extract", "")
        if extract:
            extract = clean_text(extract)
            paragraphs = extract.split("\n\n")
            if paragraphs:
                first_para = paragraphs[0].strip()
                if len(first_para) < 100 and len(paragraphs) > 1:
                    first_para = "\n\n".join(paragraphs[:2]).strip()
                return first_para
        return None
    return None


def translate_en_to_hu(text: str) -> str | None:
    """Translate English text to Hungarian using MyMemory public API."""
    def split_into_chunks(value: str, max_len: int) -> list[str]:
        normalized = re.sub(r"\s+", " ", value).strip()
        if not normalized:
            return []
        if len(normalized) <= max_len:
            return [normalized]

        sentence_parts = re.split(r"(?<=[.!?])\s+", normalized)
        chunks = []
        current = ""
        for sentence in sentence_parts:
            sentence = sentence.strip()
            if not sentence:
                continue
            if len(sentence) > max_len:
                words = sentence.split()
                buffer = ""
                for word in words:
                    trial = f"{buffer} {word}".strip()
                    if len(trial) <= max_len:
                        buffer = trial
                    else:
                        if buffer:
                            chunks.append(buffer)
                        buffer = word
                if buffer:
                    if current:
                        chunks.append(current)
                        current = ""
                    chunks.append(buffer)
                continue

            trial = f"{current} {sentence}".strip()
            if len(trial) <= max_len:
                current = trial
            else:
                if current:
                    chunks.append(current)
                current = sentence
        if current:
            chunks.append(current)
        return chunks

    chunks = split_into_chunks(text, TRANSLATION_MAX_CHARS)
    if not chunks:
        return None

    translated_chunks = []
    for chunk in chunks:
        params = {
            "q": chunk,
            "langpair": "en|hu",
        }
        data = api_request_with_retry(MYMEMORY_API, params, max_retries=2)
        if not data:
            return None
        response = data.get("responseData", {})
        translated = (response.get("translatedText") or "").strip()
        if is_invalid_translation_text(translated):
            return None
        translated_chunks.append(translated)
        time.sleep(0.15)

    final_translation = " ".join(translated_chunks).strip()
    if is_invalid_translation_text(final_translation):
        return None
    if final_translation.lower() == text.strip().lower():
        return None
    return final_translation


def translation_cache_key(text: str) -> str:
    digest = hashlib.sha1(text.encode("utf-8")).hexdigest()
    return f"tr:en-hu:{digest}"


def main():
    """Main function to fetch Wikipedia introductions for all plants."""
    print("Fetching Wikipedia introductions for plants (EN + HU)...")
    print(f"Database: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    ensure_columns(conn)

    cursor.execute(
        """
        SELECT
            id,
            canonical_name,
            scientific_name,
            wikipedia_url_english,
            wikipedia_url_hungarian,
            description_english,
            description_hungarian,
            description_hungarian_is_translated
        FROM plants
        WHERE wikipedia_url_english IS NOT NULL OR wikipedia_url_hungarian IS NOT NULL
        """
    )
    plants = cursor.fetchall()
    print(f"Found {len(plants)} plants with at least one Wikipedia URL")

    cache = load_cache()
    en_fetched_count = 0
    hu_fetched_count = 0
    hu_translated_count = 0
    skipped_count = 0
    failed_count = 0

    for i, plant in enumerate(plants):
        plant_id = plant["id"]
        canonical_name = plant["canonical_name"] or plant["scientific_name"] or f"#{plant_id}"
        en_url = plant["wikipedia_url_english"]
        hu_url = plant["wikipedia_url_hungarian"]
        description_en = (plant["description_english"] or "").strip()
        description_hu = (plant["description_hungarian"] or "").strip()
        hu_is_translated = int(plant["description_hungarian_is_translated"] or 0)

        updates = {}
        touched = False

        if not description_en and en_url:
            en_title = get_page_title_from_url(en_url)
            if en_title:
                cache_key = f"en:{en_title.lower()}"
                if cache_key in cache:
                    intro_en = None if cache[cache_key] == "NO_INTRO" else cache[cache_key]
                else:
                    intro_en = get_page_intro(en_title, "en")
                    cache[cache_key] = intro_en if intro_en else "NO_INTRO"
                    time.sleep(0.4)

                if intro_en:
                    updates["description_english"] = intro_en
                    description_en = intro_en
                    en_fetched_count += 1
                    touched = True
            else:
                failed_count += 1

        if (not description_hu or hu_is_translated == 1) and hu_url:
            hu_title = get_page_title_from_url(hu_url)
            if hu_title:
                cache_key = f"hu:{hu_title.lower()}"
                if cache_key in cache:
                    intro_hu = None if cache[cache_key] == "NO_INTRO" else cache[cache_key]
                else:
                    intro_hu = get_page_intro(hu_title, "hu")
                    cache[cache_key] = intro_hu if intro_hu else "NO_INTRO"
                    time.sleep(0.4)

                if intro_hu:
                    updates["description_hungarian"] = intro_hu
                    updates["description_hungarian_is_translated"] = 0
                    description_hu = intro_hu
                    hu_is_translated = 0
                    hu_fetched_count += 1
                    touched = True

        if not description_hu and description_en:
            tr_key = translation_cache_key(description_en)
            if tr_key in cache:
                cached_translation = cache[tr_key]
                if cached_translation == "NO_TRANSLATION" or is_invalid_translation_text(cached_translation):
                    translated_hu = None
                    cache[tr_key] = "NO_TRANSLATION"
                else:
                    translated_hu = cached_translation
            else:
                translated_hu = translate_en_to_hu(description_en)
                cache[tr_key] = translated_hu if translated_hu else "NO_TRANSLATION"
                time.sleep(0.3)

            if translated_hu:
                updates["description_hungarian"] = translated_hu
                updates["description_hungarian_is_translated"] = 1
                description_hu = translated_hu
                hu_is_translated = 1
                hu_translated_count += 1
                touched = True

        if touched:
            set_parts = [f"{col} = ?" for col in updates.keys()]
            values = list(updates.values()) + [plant_id]
            cursor.execute(f"UPDATE plants SET {', '.join(set_parts)} WHERE id = ?", values)
            print(
                f"  [{i + 1}/{len(plants)}] Updated: {canonical_name} "
                f"(en={'description_english' in updates}, hu={'description_hungarian' in updates}, "
                f"translated={updates.get('description_hungarian_is_translated', hu_is_translated) == 1})"
            )
        else:
            skipped_count += 1

        if (i + 1) % 20 == 0:
            save_cache(cache)
            conn.commit()
            print(
                f"  Progress: {i + 1}/{len(plants)} "
                f"(en={en_fetched_count}, hu={hu_fetched_count}, hu_translated={hu_translated_count})"
            )

    save_cache(cache)
    conn.commit()
    conn.close()

    print("\n=== Complete ===")
    print(f"English intros fetched: {en_fetched_count}")
    print(f"Hungarian intros fetched: {hu_fetched_count}")
    print(f"Hungarian intros translated: {hu_translated_count}")
    print(f"Skipped (already had/no source): {skipped_count}")
    print(f"Failed: {failed_count}")


if __name__ == "__main__":
    main()
