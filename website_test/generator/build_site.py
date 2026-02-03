"""
Static Site Generator for Plant Encyclopedia

Generates all HTML pages from the SQLite database.
Run this script after importing data to rebuild the website.
"""

import sqlite3
import json
import re
import shutil
from pathlib import Path
from collections import defaultdict
from jinja2 import Environment, FileSystemLoader


# Paths
BASE_DIR = Path(__file__).parent.parent
TEMPLATE_DIR = BASE_DIR / "templates"
OUTPUT_DIR = BASE_DIR / "output"
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "plants.db"


def slugify(text):
    """Convert text to URL-friendly slug."""
    if not text:
        return ""
    # Remove author citations in parentheses and trailing author names
    text = re.sub(r'\s+\([^)]+\)\s*$', '', text)
    text = re.sub(r'\s+[A-Z][a-z]*\.?\s*$', '', text)
    # Convert to lowercase and replace spaces/special chars
    text = text.lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)
    return text


def setup_jinja_env():
    """Set up Jinja2 environment with custom filters."""
    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))
    env.filters['slugify'] = slugify
    return env


def get_db_connection():
    """Get database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_all_plants(conn):
    """Get all plants with basic info."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM plants ORDER BY canonical_name, scientific_name
    """)
    plants = [dict(row) for row in cursor.fetchall()]

    # Add slug to each plant
    for plant in plants:
        name = plant['canonical_name'] or plant['scientific_name'] or plant['input_name']
        plant['slug'] = slugify(name)

    return plants


def get_plant_synonyms(conn, plant_id):
    """Get synonyms for a plant."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT synonym_name FROM plant_synonyms
        WHERE plant_id = ? ORDER BY synonym_name
    """, (plant_id,))
    return [row['synonym_name'] for row in cursor.fetchall()]


def get_plant_common_names(conn, plant_id):
    """Get common names for a plant."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT common_name FROM plant_common_names
        WHERE plant_id = ? ORDER BY common_name
    """, (plant_id,))
    return [row['common_name'] for row in cursor.fetchall()]


def get_categories(conn, category_type):
    """Get all categories of a specific type with plant counts."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.*, COUNT(pc.plant_id) as plant_count
        FROM categories c
        LEFT JOIN plant_categories pc ON c.id = pc.category_id
        WHERE c.category_type = ?
        GROUP BY c.id
        HAVING plant_count > 0
        ORDER BY c.name
    """, (category_type,))
    categories = [dict(row) for row in cursor.fetchall()]

    for cat in categories:
        cat['slug'] = slugify(cat['name'])

    return categories


def get_plants_in_category(conn, category_id):
    """Get all plants in a category."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT p.* FROM plants p
        JOIN plant_categories pc ON p.id = pc.plant_id
        WHERE pc.category_id = ?
        ORDER BY p.canonical_name, p.scientific_name
    """, (category_id,))
    plants = [dict(row) for row in cursor.fetchall()]

    for plant in plants:
        name = plant['canonical_name'] or plant['scientific_name'] or plant['input_name']
        plant['slug'] = slugify(name)

    return plants


def group_by_letter(items, key='name'):
    """Group items by first letter."""
    letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    grouped = {letter: [] for letter in letters}

    for item in items:
        value = item.get(key) or item.get('canonical_name') or item.get('scientific_name') or ''
        first_letter = value[0].upper() if value else '#'
        if first_letter in grouped:
            grouped[first_letter].append(item)

    return grouped


def build_search_data(plants, conn):
    """Build JSON search data for client-side search."""
    search_data = []

    for plant in plants:
        common_names = get_plant_common_names(conn, plant['id'])
        synonyms = get_plant_synonyms(conn, plant['id'])

        search_data.append({
            'id': plant['id'],
            'slug': plant['slug'],
            'canonical_name': plant['canonical_name'],
            'scientific_name': plant['scientific_name'],
            'common_name': plant['common_name'],
            'common_names': common_names[:10],  # Limit for file size
            'synonyms': synonyms[:10],  # Limit for file size
        })

    return search_data


def clear_output_dir():
    """Clear and recreate output directory."""
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True)


def copy_static_files():
    """Copy static files to output directory."""
    output_static = OUTPUT_DIR / "static"
    if output_static.exists():
        shutil.rmtree(output_static)
    shutil.copytree(STATIC_DIR, output_static)


def build_site():
    """Main build function."""
    print("Building Plant Encyclopedia...")

    # Setup
    env = setup_jinja_env()
    conn = get_db_connection()

    # Clear and prepare output
    clear_output_dir()
    copy_static_files()

    # Create output subdirectories
    (OUTPUT_DIR / "plant").mkdir()
    (OUTPUT_DIR / "family").mkdir()
    (OUTPUT_DIR / "genus").mkdir()

    # Get all data
    plants = get_all_plants(conn)
    families = get_categories(conn, 'family')
    genera = get_categories(conn, 'genus')

    print(f"Found {len(plants)} plants, {len(families)} families, {len(genera)} genera")

    # Common template context
    base_context = {
        'base_url': '.',
    }

    # === Build Homepage ===
    print("Building homepage...")
    template = env.get_template('index.html')
    html = template.render(
        **base_context,
        plant_count=len(plants),
        family_count=len(families),
        genus_count=len(genera),
        featured_plants=plants[:8],  # First 8 as featured
    )
    (OUTPUT_DIR / "index.html").write_text(html, encoding='utf-8')

    # === Build A-Z Index ===
    print("Building A-Z index...")
    template = env.get_template('az_index.html')
    plants_by_letter = group_by_letter(plants, key='canonical_name')
    used_letters = {letter for letter, items in plants_by_letter.items() if items}

    html = template.render(
        **base_context,
        plant_count=len(plants),
        letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ',
        used_letters=used_letters,
        plants_by_letter=plants_by_letter,
    )
    (OUTPUT_DIR / "az-index.html").write_text(html, encoding='utf-8')

    # === Build Families List ===
    print("Building families list...")
    template = env.get_template('category_list.html')
    families_by_letter = group_by_letter(families)
    used_letters = {letter for letter, items in families_by_letter.items() if items}

    html = template.render(
        **base_context,
        title="Plant Families",
        description=f"Browse plants organized by {len(families)} botanical families.",
        category_type="family",
        letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ',
        used_letters=used_letters,
        categories_by_letter=families_by_letter,
    )
    (OUTPUT_DIR / "families.html").write_text(html, encoding='utf-8')

    # === Build Genera List ===
    print("Building genera list...")
    genera_by_letter = group_by_letter(genera)
    used_letters = {letter for letter, items in genera_by_letter.items() if items}

    html = template.render(
        **base_context,
        title="Plant Genera",
        description=f"Browse plants organized by {len(genera)} genera.",
        category_type="genus",
        letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ',
        used_letters=used_letters,
        categories_by_letter=genera_by_letter,
    )
    (OUTPUT_DIR / "genera.html").write_text(html, encoding='utf-8')

    # === Build Individual Family Pages ===
    print("Building family pages...")
    template = env.get_template('category.html')

    for family in families:
        family_plants = get_plants_in_category(conn, family['id'])
        html = template.render(
            base_url='..',
            category=family,
            category_type='family',
            category_type_plural='families',
            category_type_title='Family',
            plants=family_plants,
        )
        (OUTPUT_DIR / "family" / f"{family['slug']}.html").write_text(html, encoding='utf-8')

    # === Build Individual Genus Pages ===
    print("Building genus pages...")
    for genus in genera:
        genus_plants = get_plants_in_category(conn, genus['id'])
        html = template.render(
            base_url='..',
            category=genus,
            category_type='genus',
            category_type_plural='genera',
            category_type_title='Genus',
            plants=genus_plants,
        )
        (OUTPUT_DIR / "genus" / f"{genus['slug']}.html").write_text(html, encoding='utf-8')

    # === Build Individual Plant Pages ===
    print("Building plant pages...")
    template = env.get_template('plant.html')

    for i, plant in enumerate(plants):
        synonyms = get_plant_synonyms(conn, plant['id'])
        common_names = get_plant_common_names(conn, plant['id'])

        html = template.render(
            base_url='..',
            plant=plant,
            synonyms=synonyms,
            common_names=common_names,
        )
        (OUTPUT_DIR / "plant" / f"{plant['slug']}.html").write_text(html, encoding='utf-8')

        if (i + 1) % 50 == 0:
            print(f"  Built {i + 1}/{len(plants)} plant pages...")

    # === Build Search Data ===
    print("Building search data...")
    search_data = build_search_data(plants, conn)
    search_json_path = OUTPUT_DIR / "static" / "js" / "search-data.json"
    search_json_path.write_text(json.dumps(search_data, ensure_ascii=False), encoding='utf-8')

    conn.close()

    print(f"\n=== Build Complete ===")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Total pages generated: {1 + 1 + 1 + 1 + len(families) + len(genera) + len(plants)}")
    print(f"\nTo view the site, open: {OUTPUT_DIR / 'index.html'}")


if __name__ == "__main__":
    build_site()
