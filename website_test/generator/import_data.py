"""
Excel to SQLite Importer for Plant Database

This script imports plant data from Excel files into a SQLite database.
Designed to be extensible - new data sources can be added easily.
"""

import sqlite3
import pandas as pd
from pathlib import Path


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

        # Insert main plant record
        cursor.execute("""
            INSERT OR REPLACE INTO plants (
                input_name, scientific_name, canonical_name, common_name,
                family, genus, wfo_id, gbif_usage_key, gbif_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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

        plant_id = cursor.lastrowid

        # If this was a replace, get the actual ID
        if plant_id == 0:
            cursor.execute("SELECT id FROM plants WHERE input_name = ?", (input_name,))
            plant_id = cursor.fetchone()[0]

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


def main():
    """Main import function."""
    print("Starting data import...")
    print(f"Database path: {DB_PATH}")

    # Ensure data directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Create fresh database
    if DB_PATH.exists():
        DB_PATH.unlink()
        print("Removed existing database")

    conn = create_database()
    print("Created database schema")

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

    conn.close()


if __name__ == "__main__":
    main()
