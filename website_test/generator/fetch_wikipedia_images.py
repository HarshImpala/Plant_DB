"""
Fetch Wikipedia images for plants.

This script fetches the main image from Wikipedia for each plant
that has a Wikipedia URL, downloads it, and updates the database.
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
IMAGES_DIR = BASE_DIR / "static" / "images" / "plants"
CACHE_PATH = DATA_DIR / "wikipedia_images_cache.json"

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


def slugify(text: str) -> str:
    """Convert text to a URL-safe slug."""
    text = text.lower()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)
    return text.strip('-')


def get_page_title_from_url(wikipedia_url: str) -> str | None:
    """Extract the page title from a Wikipedia URL."""
    # URL format: https://en.wikipedia.org/wiki/Page_Title
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


def get_page_image_url(page_title: str, thumb_width: int = 800) -> str | None:
    """Get the thumbnail image URL for a Wikipedia page using pageimages API."""
    params = {
        "action": "query",
        "titles": page_title,
        "prop": "pageimages",
        "piprop": "thumbnail",
        "pithumbsize": thumb_width,  # Request thumbnail at specified width
        "format": "json",
    }

    data = api_request_with_retry(params)
    if not data:
        return None

    pages = data.get("query", {}).get("pages", {})
    for page_id, page_data in pages.items():
        if page_id == "-1":
            return None
        thumbnail = page_data.get("thumbnail", {})
        return thumbnail.get("source")

    return None


def download_image(image_url: str, save_path: Path, max_retries: int = 3) -> bool:
    """Download an image and save it to disk with retry logic."""
    for attempt in range(max_retries):
        try:
            r = requests.get(image_url, headers=HEADERS, timeout=60, stream=True)

            if r.status_code == 429:
                wait_time = 5 * (attempt + 1)  # 5, 10, 15 seconds
                print(f"  Rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
                continue

            r.raise_for_status()

            # Check content type
            content_type = r.headers.get('content-type', '')
            if not content_type.startswith('image/'):
                print(f"  Not an image: {content_type}")
                return False

            # Save the image
            with open(save_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

            return True
        except requests.exceptions.HTTPError as e:
            if '429' in str(e) and attempt < max_retries - 1:
                wait_time = 5 * (attempt + 1)
                print(f"  Rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            print(f"  Download failed: {e}")
            return False
        except Exception as e:
            print(f"  Download failed: {e}")
            return False

    return False


def get_image_extension(image_url: str) -> str:
    """Get the file extension from an image URL."""
    parsed = urlparse(image_url)
    path = parsed.path.lower()

    if path.endswith('.jpg') or path.endswith('.jpeg'):
        return '.jpg'
    elif path.endswith('.png'):
        return '.png'
    elif path.endswith('.gif'):
        return '.gif'
    elif path.endswith('.svg'):
        return '.svg'
    elif path.endswith('.webp'):
        return '.webp'
    else:
        # Default to jpg
        return '.jpg'


def main():
    """Main function to fetch Wikipedia images for all plants."""
    print("Fetching Wikipedia images for plants...")
    print(f"Database: {DB_PATH}")
    print(f"Images directory: {IMAGES_DIR}")

    # Ensure images directory exists
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get plants with Wikipedia URLs
    cursor.execute("""
        SELECT id, canonical_name, scientific_name, wikipedia_url, image_filename
        FROM plants
        WHERE wikipedia_url IS NOT NULL
    """)
    plants = cursor.fetchall()
    print(f"Found {len(plants)} plants with Wikipedia URLs")

    cache = load_cache()
    downloaded_count = 0
    skipped_count = 0
    failed_count = 0

    for i, plant in enumerate(plants):
        plant_id = plant["id"]
        canonical_name = plant["canonical_name"] or plant["scientific_name"]
        wikipedia_url = plant["wikipedia_url"]
        existing_image = plant["image_filename"]

        # Skip if already has an image
        if existing_image:
            skipped_count += 1
            continue

        # Create slug for filename
        slug = slugify(canonical_name)
        cache_key = slug

        # Check cache
        if cache_key in cache:
            cached = cache[cache_key]
            if cached == "NO_IMAGE":
                skipped_count += 1
                continue
            elif cached.startswith("DOWNLOADED:"):
                # Already downloaded, just update DB if needed
                filename = cached.replace("DOWNLOADED:", "")
                cursor.execute(
                    "UPDATE plants SET image_filename = ? WHERE id = ?",
                    (filename, plant_id)
                )
                downloaded_count += 1
                continue

        # Get page title from URL
        page_title = get_page_title_from_url(wikipedia_url)
        if not page_title:
            print(f"  [{i+1}] Could not extract title from: {wikipedia_url}")
            cache[cache_key] = "NO_IMAGE"
            failed_count += 1
            continue

        # Get image URL from Wikipedia
        image_url = get_page_image_url(page_title)

        if not image_url:
            cache[cache_key] = "NO_IMAGE"
            skipped_count += 1
            time.sleep(0.3)
            continue

        # Skip SVG images (they don't display well as plant photos)
        if image_url.lower().endswith('.svg'):
            cache[cache_key] = "NO_IMAGE"
            skipped_count += 1
            time.sleep(0.3)
            continue

        # Determine extension and filename
        ext = get_image_extension(image_url)
        filename = f"{slug}{ext}"
        save_path = IMAGES_DIR / filename

        # Download the image
        print(f"  [{i+1}/{len(plants)}] Downloading: {canonical_name}")
        if download_image(image_url, save_path):
            # Update database with filename and source
            cursor.execute(
                "UPDATE plants SET image_filename = ?, image_source = 'wikipedia' WHERE id = ?",
                (filename, plant_id)
            )
            cache[cache_key] = f"DOWNLOADED:{filename}"
            downloaded_count += 1
        else:
            cache[cache_key] = "NO_IMAGE"
            failed_count += 1

        time.sleep(1.5)  # Rate limiting - be gentle with Wikipedia

        # Save progress periodically
        if (i + 1) % 10 == 0:
            save_cache(cache)
            conn.commit()
            print(f"  Progress: {i + 1}/{len(plants)} ({downloaded_count} downloaded)")

    save_cache(cache)
    conn.commit()
    conn.close()

    print(f"\n=== Complete ===")
    print(f"Downloaded: {downloaded_count}")
    print(f"Skipped (no image/already had): {skipped_count}")
    print(f"Failed: {failed_count}")


if __name__ == "__main__":
    main()
