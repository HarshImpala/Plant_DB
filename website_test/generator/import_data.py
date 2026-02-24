"""
Excel to SQLite Importer for Plant Database

This script imports plant data from Excel files into a SQLite database.
Designed to be extensible - new data sources can be added easily.
"""

import sqlite3
import pandas as pd
from pathlib import Path
import argparse
import json
import re
from difflib import SequenceMatcher
from itertools import combinations


# Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "plants.db"

# Source Excel files
EXCEL_DIR = Path(__file__).parent.parent.parent / "new_scripts_WFO_main_source"
TAXONOMY_FILE = EXCEL_DIR / "taxonomy" / "plants_gbif_matched_plus_wfo_syn_diff_and_taxonomy.xlsx"
LOCATION_FILE = EXCEL_DIR / "location" / "plants_gbif_with_native_plus_wfo.xlsx"


def create_database():
    """Create the SQLite database with all tables."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Main plants table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS plants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            input_name TEXT UNIQUE NOT NULL,
            scientific_name TEXT,
            canonical_name TEXT,
            common_name TEXT,
            family TEXT,
            genus TEXT,
            wfo_id TEXT,
            wfo_url TEXT,
            gbif_usage_key TEXT,
            gbif_url TEXT,
            wikipedia_url TEXT,
            native_countries TEXT,
            native_regions TEXT,
            native_confidence TEXT,
            -- Placeholder columns for future data
            toxicity_info TEXT,
            garden_location TEXT,
            image_filename TEXT,
            image_source TEXT,
            description TEXT,
            curator_comments TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Synonyms table (one-to-many)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS plant_synonyms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plant_id INTEGER NOT NULL,
            synonym_name TEXT NOT NULL,
            source TEXT,  -- 'gbif' or 'wfo'
            FOREIGN KEY (plant_id) REFERENCES plants(id),
            UNIQUE(plant_id, synonym_name)
        )
    """)

    # Common names table (one-to-many)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS plant_common_names (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plant_id INTEGER NOT NULL,
            common_name TEXT NOT NULL,
            language TEXT DEFAULT 'en',
            FOREIGN KEY (plant_id) REFERENCES plants(id),
            UNIQUE(plant_id, common_name)
        )
    """)

    # Native regions table (one-to-many) - for detailed location data
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS plant_native_regions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plant_id INTEGER NOT NULL,
            country TEXT,
            region TEXT,
            source TEXT,  -- 'gbif' or 'wfo'
            FOREIGN KEY (plant_id) REFERENCES plants(id)
        )
    """)

    # Categories table (for organizing plants)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            category_type TEXT,  -- 'family', 'genus', 'custom'
            description TEXT
        )
    """)

    # Plant-category relationship (many-to-many)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS plant_categories (
            plant_id INTEGER NOT NULL,
            category_id INTEGER NOT NULL,
            FOREIGN KEY (plant_id) REFERENCES plants(id),
            FOREIGN KEY (category_id) REFERENCES categories(id),
            PRIMARY KEY (plant_id, category_id)
        )
    """)

    # Collections table (seeded from data/collections.json during build)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS collections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            name_en TEXT NOT NULL,
            name_hu TEXT,
            description_en TEXT,
            description_hu TEXT,
            image_filename TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create indexes for faster lookups
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_plants_canonical ON plants(canonical_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_plants_family ON plants(family)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_plants_genus ON plants(genus)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_synonyms_plant ON plant_synonyms(plant_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_common_names_plant ON plant_common_names(plant_id)")

    conn.commit()
    return conn


def parse_pipe_separated(value):
    """Parse pipe-separated values into a list."""
    if pd.isna(value) or not value:
        return []
    return [item.strip() for item in str(value).split('|') if item.strip()]


def import_taxonomy_data(conn, df_taxonomy):
    """Import data from the taxonomy Excel file."""
    cursor = conn.cursor()

    for _, row in df_taxonomy.iterrows():
        input_name = row.get('input_name')
        if not input_name or pd.isna(input_name):
            continue

        # Get GBIF usage key and construct URL
        gbif_usage_key = row.get('gbif_accepted_usageKey')
        if pd.isna(gbif_usage_key):
            gbif_usage_key = None
            gbif_url = None
        else:
            gbif_usage_key = str(int(gbif_usage_key))  # Convert to string, remove decimal
            gbif_url = f"https://www.gbif.org/species/{gbif_usage_key}"

        # Upsert main plant record while preserving existing plant ID.
        cursor.execute("""
            INSERT INTO plants (
                input_name, scientific_name, canonical_name, common_name,
                family, genus, wfo_id, gbif_usage_key, gbif_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(input_name) DO UPDATE SET
                scientific_name=excluded.scientific_name,
                canonical_name=excluded.canonical_name,
                common_name=excluded.common_name,
                family=excluded.family,
                genus=excluded.genus,
                wfo_id=excluded.wfo_id,
                gbif_usage_key=excluded.gbif_usage_key,
                gbif_url=excluded.gbif_url,
                updated_at=CURRENT_TIMESTAMP
        """, (
            input_name,
            row.get('gbif_scientificName') if not pd.isna(row.get('gbif_scientificName')) else None,
            row.get('gbif_canonicalName') if not pd.isna(row.get('gbif_canonicalName')) else None,
            row.get('gbif_english_name') if not pd.isna(row.get('gbif_english_name')) else None,
            row.get('wfo_family') if not pd.isna(row.get('wfo_family')) else None,
            row.get('wfo_genus') if not pd.isna(row.get('wfo_genus')) else None,
            row.get('wfo_match_wfo_id') if not pd.isna(row.get('wfo_match_wfo_id')) else None,
            gbif_usage_key,
            gbif_url,
        ))

        cursor.execute("SELECT id FROM plants WHERE input_name = ?", (input_name,))
        plant_id = cursor.fetchone()[0]

        # Replace relationship records so incremental imports stay consistent.
        cursor.execute("DELETE FROM plant_synonyms WHERE plant_id = ?", (plant_id,))
        cursor.execute("DELETE FROM plant_common_names WHERE plant_id = ?", (plant_id,))
        cursor.execute("DELETE FROM plant_categories WHERE plant_id = ?", (plant_id,))

        # Import synonyms from GBIF
        synonyms = parse_pipe_separated(row.get('gbif_synonyms'))
        for synonym in synonyms:
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO plant_synonyms (plant_id, synonym_name, source)
                    VALUES (?, ?, 'gbif')
                """, (plant_id, synonym))
            except sqlite3.IntegrityError:
                pass

        # Import synonyms from WFO
        wfo_synonyms = parse_pipe_separated(row.get('wfo_synonyms'))
        for synonym in wfo_synonyms:
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO plant_synonyms (plant_id, synonym_name, source)
                    VALUES (?, ?, 'wfo')
                """, (plant_id, synonym))
            except sqlite3.IntegrityError:
                pass

        # Import common names
        common_names = parse_pipe_separated(row.get('gbif_english_names'))
        for name in common_names:
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO plant_common_names (plant_id, common_name, language)
                    VALUES (?, ?, 'en')
                """, (plant_id, name))
            except sqlite3.IntegrityError:
                pass

        # Create/link family category
        family = row.get('wfo_family')
        if family and not pd.isna(family):
            cursor.execute("""
                INSERT OR IGNORE INTO categories (name, category_type)
                VALUES (?, 'family')
            """, (family,))
            cursor.execute("SELECT id FROM categories WHERE name = ?", (family,))
            cat_id = cursor.fetchone()[0]
            cursor.execute("""
                INSERT OR IGNORE INTO plant_categories (plant_id, category_id)
                VALUES (?, ?)
            """, (plant_id, cat_id))

        # Create/link genus category
        genus = row.get('wfo_genus')
        if genus and not pd.isna(genus):
            cursor.execute("""
                INSERT OR IGNORE INTO categories (name, category_type)
                VALUES (?, 'genus')
            """, (genus,))
            cursor.execute("SELECT id FROM categories WHERE name = ?", (genus,))
            cat_id = cursor.fetchone()[0]
            cursor.execute("""
                INSERT OR IGNORE INTO plant_categories (plant_id, category_id)
                VALUES (?, ?)
            """, (plant_id, cat_id))

    conn.commit()
    print(f"Imported {len(df_taxonomy)} plants from taxonomy file")


def import_location_data(conn, df_location):
    """Import data from the location Excel file."""
    cursor = conn.cursor()

    for _, row in df_location.iterrows():
        input_name = row.get('input_name')
        if not input_name or pd.isna(input_name):
            continue

        # Update plant record with location data
        wfo_url = row.get('wfo_url') if not pd.isna(row.get('wfo_url')) else None
        native_countries = row.get('wfo_native_countries') if not pd.isna(row.get('wfo_native_countries')) else None
        native_regions = row.get('wfo_native_areas_found_in') if not pd.isna(row.get('wfo_native_areas_found_in')) else None
        native_confidence = row.get('gbif_native_confidence') if not pd.isna(row.get('gbif_native_confidence')) else None

        cursor.execute("""
            UPDATE plants SET
                wfo_url = ?,
                native_countries = ?,
                native_regions = ?,
                native_confidence = ?
            WHERE input_name = ?
        """, (wfo_url, native_countries, native_regions, native_confidence, input_name))

        # Get plant_id for detailed region import
        cursor.execute("SELECT id FROM plants WHERE input_name = ?", (input_name,))
        result = cursor.fetchone()
        if not result:
            continue
        plant_id = result[0]

        # Replace location records for this plant on each import.
        cursor.execute("DELETE FROM plant_native_regions WHERE plant_id = ?", (plant_id,))

        # Import individual countries from WFO
        countries = row.get('wfo_native_countries')
        if countries and not pd.isna(countries):
            for country in str(countries).split('|'):
                country = country.strip()
                if country:
                    cursor.execute("""
                        INSERT INTO plant_native_regions (plant_id, country, source)
                        VALUES (?, ?, 'wfo')
                    """, (plant_id, country))

    conn.commit()
    print(f"Updated location data for {len(df_location)} plants")


CURATOR_DATA_FILE = DATA_DIR / "curator_data.csv"

CURATOR_FIELDS = ["toxicity_info", "garden_location", "curator_comments", "image_source"]
DUPLICATE_REPORT_PATH = DATA_DIR / "duplicate_review_report.json"


def import_curator_data(conn):
    """Merge curator_data.csv into the plants table (survives full re-imports)."""
    if not CURATOR_DATA_FILE.exists():
        print("No curator_data.csv found — skipping.")
        return

    import csv
    cursor = conn.cursor()
    updated = 0
    skipped = 0

    with open(CURATOR_DATA_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            input_name = row.get('input_name', '').strip()
            if not input_name:
                continue

            # Build SET clause for non-empty fields only
            updates = {field: row[field].strip()
                       for field in CURATOR_FIELDS
                       if field in row and row[field].strip()}

            if not updates:
                skipped += 1
                continue

            set_clause = ', '.join(f"{field} = ?" for field in updates)
            values = list(updates.values()) + [input_name]
            cursor.execute(
                f"UPDATE plants SET {set_clause} WHERE input_name = ?",
                values
            )
            if cursor.rowcount:
                updated += 1
            else:
                print(f"  Warning: no plant found for input_name '{input_name}'")

    conn.commit()
    print(f"Curator data: {updated} plants updated, {skipped} rows skipped (empty).")


def _norm_name(value):
    if not value:
        return ""
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def generate_duplicate_review_report(conn):
    """Generate a duplicate-candidate review report for curator review."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, input_name, canonical_name, scientific_name, family, genus
        FROM plants
        ORDER BY canonical_name, scientific_name, input_name
    """)
    rows = [dict(zip([c[0] for c in cursor.description], r)) for r in cursor.fetchall()]

    canonical_groups = {}
    scientific_groups = {}
    family_genus_groups = {}
    for row in rows:
        canonical_key = _norm_name(row.get("canonical_name"))
        scientific_key = _norm_name(row.get("scientific_name"))
        fg_key = (row.get("family") or "", row.get("genus") or "")
        if canonical_key:
            canonical_groups.setdefault(canonical_key, []).append(row)
        if scientific_key:
            scientific_groups.setdefault(scientific_key, []).append(row)
        family_genus_groups.setdefault(fg_key, []).append(row)

    exact_canonical_duplicates = [
        {
            "normalized_name": key,
            "count": len(group),
            "plants": group,
        }
        for key, group in canonical_groups.items()
        if len(group) > 1
    ]
    exact_scientific_duplicates = [
        {
            "normalized_name": key,
            "count": len(group),
            "plants": group,
        }
        for key, group in scientific_groups.items()
        if len(group) > 1
    ]

    similar_name_candidates = []
    for (family, genus), group in family_genus_groups.items():
        if len(group) < 2 or len(group) > 25:
            continue
        for a, b in combinations(group, 2):
            a_name = _norm_name(a.get("canonical_name") or a.get("scientific_name") or a.get("input_name"))
            b_name = _norm_name(b.get("canonical_name") or b.get("scientific_name") or b.get("input_name"))
            if not a_name or not b_name or a_name == b_name:
                continue
            ratio = SequenceMatcher(None, a_name, b_name).ratio()
            if ratio >= 0.92:
                similar_name_candidates.append({
                    "family": family,
                    "genus": genus,
                    "similarity": round(ratio, 3),
                    "a": a,
                    "b": b,
                })

    report = {
        "summary": {
            "plant_count": len(rows),
            "exact_canonical_duplicate_groups": len(exact_canonical_duplicates),
            "exact_scientific_duplicate_groups": len(exact_scientific_duplicates),
            "similar_name_candidate_pairs": len(similar_name_candidates),
        },
        "exact_canonical_duplicates": sorted(exact_canonical_duplicates, key=lambda x: x["count"], reverse=True),
        "exact_scientific_duplicates": sorted(exact_scientific_duplicates, key=lambda x: x["count"], reverse=True),
        "same_family_genus_similar_name": sorted(similar_name_candidates, key=lambda x: x["similarity"], reverse=True),
    }
    DUPLICATE_REPORT_PATH.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(
        "Duplicate review report written: "
        f"{DUPLICATE_REPORT_PATH} "
        f"(exact canonical groups={report['summary']['exact_canonical_duplicate_groups']}, "
        f"exact scientific groups={report['summary']['exact_scientific_duplicate_groups']}, "
        f"similar pairs={report['summary']['similar_name_candidate_pairs']})"
    )


def main():
    """Main import function."""
    parser = argparse.ArgumentParser(description="Import plant data into SQLite database.")
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="Delete and recreate the database before importing (destructive).",
    )
    args = parser.parse_args()

    print("Starting data import...")
    print(f"Database path: {DB_PATH}")

    # Ensure data directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Full rebuild is explicit. Default behavior is incremental update.
    if args.full_rebuild and DB_PATH.exists():
        DB_PATH.unlink()
        print("Removed existing database (--full-rebuild)")

    conn = create_database()
    if args.full_rebuild:
        print("Created database schema (full rebuild)")
    else:
        print("Ensured database schema (incremental mode)")

    # Import taxonomy data
    print(f"\nReading taxonomy file: {TAXONOMY_FILE}")
    df_taxonomy = pd.read_excel(TAXONOMY_FILE)
    import_taxonomy_data(conn, df_taxonomy)

    # Import location data
    print(f"\nReading location file: {LOCATION_FILE}")
    df_location = pd.read_excel(LOCATION_FILE)
    import_location_data(conn, df_location)

    # Merge curator data (toxicity, garden location, comments, image source)
    print(f"\nMerging curator data: {CURATOR_DATA_FILE}")
    import_curator_data(conn)

    # Print summary
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM plants")
    plant_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM plant_synonyms")
    synonym_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM plant_common_names")
    common_name_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM categories")
    category_count = cursor.fetchone()[0]

    print(f"\n=== Import Complete ===")
    print(f"Plants: {plant_count}")
    print(f"Synonyms: {synonym_count}")
    print(f"Common names: {common_name_count}")
    print(f"Categories: {category_count}")
    generate_duplicate_review_report(conn)

    conn.close()


if __name__ == "__main__":
    main()
