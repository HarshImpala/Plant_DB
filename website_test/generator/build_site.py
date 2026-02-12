"""
Static Site Generator for Plant Encyclopedia

Generates all HTML pages from the SQLite database.
Run this script after importing data to rebuild the website.
"""

import os
import sqlite3
import json
import re
import shutil
from pathlib import Path
from collections import defaultdict
from jinja2 import Environment, FileSystemLoader


# Site base URL — set via SITE_BASE_URL env var or edit here before deploying
SITE_BASE_URL = os.environ.get("SITE_BASE_URL", "https://example.com").rstrip("/")

# When PLACEHOLDER_IMAGES=1, skip all plant images (saves space on CI/GitHub Pages)
PLACEHOLDER_IMAGES = os.environ.get("PLACEHOLDER_IMAGES", "").lower() in ("1", "true", "yes")

# Paths
BASE_DIR = Path(__file__).parent.parent
TEMPLATE_DIR = BASE_DIR / "templates"
OUTPUT_DIR = BASE_DIR / "output"
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "plants.db"
COLLECTIONS_PATH = DATA_DIR / "collections.json"


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


def load_collections(plants):
    """Load collections from JSON, resolve plant references, and seed the DB."""
    if not COLLECTIONS_PATH.exists():
        return [], {}

    with open(COLLECTIONS_PATH, encoding='utf-8') as f:
        raw = json.load(f)

    # Build a lookup: canonical_name (lowercase) → plant dict
    name_lookup = {}
    for p in plants:
        key = (p.get('canonical_name') or '').strip().lower()
        if key:
            name_lookup[key] = p

    # Build plant → collection lookup (canonical_name → collection dict)
    plant_to_collection = {}

    collections = []
    for col in raw:
        slug = col.get('slug') or slugify(col.get('name_en') or col.get('name', ''))
        name_en = col.get('name_en') or col.get('name', '')
        name_hu = col.get('name_hu', '')
        desc_en = col.get('description_en') or col.get('description', '')
        desc_hu = col.get('description_hu', '')

        matched_plants = []
        for pname in col.get('plants', []):
            p = name_lookup.get(pname.strip().lower())
            if p:
                matched_plants.append(p)
                plant_to_collection[p['canonical_name']] = {
                    'slug': slug,
                    'name_en': name_en,
                    'name_hu': name_hu,
                }

        collections.append({
            'name_en': name_en,
            'name_hu': name_hu,
            'slug': slug,
            'description_en': desc_en,
            'description_hu': desc_hu,
            'image': col.get('image'),
            'plant_count': len(matched_plants),
            'plants': matched_plants,
        })

    return collections, plant_to_collection


def seed_collections_db(conn, collections):
    """Seed the collections table in the DB from the loaded collections list."""
    cursor = conn.cursor()
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
    cursor.execute("DELETE FROM collections")
    for col in collections:
        cursor.execute("""
            INSERT INTO collections (slug, name_en, name_hu, description_en, description_hu, image_filename)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (col['slug'], col['name_en'], col['name_hu'],
              col['description_en'], col['description_hu'], col.get('image')))
    conn.commit()


def clear_output_dir():
    """Clear output directory contents (tolerates the folder itself being locked on Windows)."""
    if OUTPUT_DIR.exists():
        for child in OUTPUT_DIR.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
    else:
        OUTPUT_DIR.mkdir(parents=True)


def copy_static_files():
    """Copy static files to output directory."""
    output_static = OUTPUT_DIR / "static"
    if output_static.exists():
        shutil.rmtree(output_static)

    if PLACEHOLDER_IMAGES:
        # Copy everything except static/images/plants/ to save space
        def ignore_plant_images(src, names):
            src_path = Path(src)
            if src_path == STATIC_DIR / "images" / "plants":
                return names  # skip all files in this folder
            return []
        shutil.copytree(STATIC_DIR, output_static, ignore=ignore_plant_images)
        (output_static / "images" / "plants").mkdir(parents=True, exist_ok=True)
        print("  (placeholder mode: plant images skipped)")
    else:
        shutil.copytree(STATIC_DIR, output_static)


def build_site():
    """Main build function."""
    import time
    print("Building Plant Encyclopedia...")

    # Setup
    env = setup_jinja_env()
    conn = get_db_connection()
    build_version = str(int(time.time()))

    # Clear and prepare output
    clear_output_dir()
    copy_static_files()

    # Create output subdirectories
    (OUTPUT_DIR / "plant").mkdir()
    (OUTPUT_DIR / "family").mkdir()
    (OUTPUT_DIR / "genus").mkdir()
    (OUTPUT_DIR / "collection").mkdir()

    # Get all data
    plants = get_all_plants(conn)
    if PLACEHOLDER_IMAGES:
        for p in plants:
            p['image_filename'] = None
    families = get_categories(conn, 'family')
    genera = get_categories(conn, 'genus')
    collections, plant_to_collection = load_collections(plants)
    seed_collections_db(conn, collections)
    print(f"Loaded {len(collections)} collections, seeded to DB")

    print(f"Found {len(plants)} plants, {len(families)} families, {len(genera)} genera")

    # Common template context
    base_context = {
        'base_url': '.',
        'build_version': build_version,
    }

    # === Build Homepage ===
    print("Building homepage...")
    template = env.get_template('index.html')
    plants_with_images = [p for p in plants if p.get('image_filename')]
    step = max(1, len(plants_with_images) // 8)
    featured_plants = plants_with_images[::step][:8]
    html = template.render(
        **base_context,
        plant_count=len(plants),
        family_count=len(families),
        genus_count=len(genera),
        featured_plants=featured_plants,
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
            base_url='..', build_version=build_version,
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
            base_url='..', build_version=build_version,
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

    # Pre-build genus/family lookup for related plants
    from collections import defaultdict
    genus_map = defaultdict(list)
    family_map = defaultdict(list)
    for p in plants:
        if p.get('genus'):
            genus_map[p['genus']].append(p)
        if p.get('family'):
            family_map[p['family']].append(p)

    for i, plant in enumerate(plants):
        synonyms = get_plant_synonyms(conn, plant['id'])
        common_names = get_plant_common_names(conn, plant['id'])
        prev_plant = plants[i - 1] if i > 0 else None
        next_plant = plants[i + 1] if i < len(plants) - 1 else None

        # Related plants: prefer same genus, fall back to same family
        related = [p for p in genus_map.get(plant.get('genus', ''), [])
                   if p['id'] != plant['id']]
        if len(related) < 2:
            related = [p for p in family_map.get(plant.get('family', ''), [])
                       if p['id'] != plant['id']]
        related_plants = related[:6]

        plant_collection = plant_to_collection.get(plant.get('canonical_name'))
        html = template.render(
            base_url='..', build_version=build_version,
            plant=plant,
            synonyms=synonyms,
            common_names=common_names,
            prev_plant=prev_plant,
            next_plant=next_plant,
            related_plants=related_plants,
            plant_collection=plant_collection,
        )
        (OUTPUT_DIR / "plant" / f"{plant['slug']}.html").write_text(html, encoding='utf-8')

        if (i + 1) % 50 == 0:
            print(f"  Built {i + 1}/{len(plants)} plant pages...")

    # === Build Search Data ===
    print("Building search data...")
    search_data = build_search_data(plants, conn)
    search_data_dir = OUTPUT_DIR / "static" / "data"
    search_data_dir.mkdir(parents=True, exist_ok=True)
    (search_data_dir / "search-data.json").write_text(json.dumps(search_data, ensure_ascii=False), encoding='utf-8')

    # === Build Stats Page ===
    print("Building stats page...")
    template = env.get_template('stats.html')
    top_families = sorted(families, key=lambda f: f['plant_count'], reverse=True)[:15]
    html = template.render(
        **base_context,
        total_plants=len(plants),
        plants_with_images=sum(1 for p in plants if p.get('image_filename')),
        plants_with_descriptions=sum(1 for p in plants if p.get('description')),
        plants_with_distribution=sum(1 for p in plants if p.get('native_countries')),
        total_families=len(families),
        total_genera=len(genera),
        top_families=top_families,
    )
    (OUTPUT_DIR / "stats.html").write_text(html, encoding='utf-8')

    # === Build Collections List Page ===
    print("Building collections list page...")
    template = env.get_template('collections.html')
    html = template.render(**base_context, collections=collections)
    (OUTPUT_DIR / "collections.html").write_text(html, encoding='utf-8')

    # === Build Individual Collection Pages ===
    print(f"Building {len(collections)} collection pages...")
    template = env.get_template('collection.html')
    for col in collections:
        html = template.render(
            base_url='..', build_version=build_version,
            collection=col,
            plants=col['plants'],
        )
        (OUTPUT_DIR / "collection" / f"{col['slug']}.html").write_text(html, encoding='utf-8')

    # === Build Map Page ===
    print("Building map page...")
    template = env.get_template('map.html')
    html = template.render(**base_context)
    (OUTPUT_DIR / "map.html").write_text(html, encoding='utf-8')

    # === Build 404 Page ===
    print("Building 404 page...")
    template = env.get_template('404.html')
    html = template.render(**base_context)
    (OUTPUT_DIR / "404.html").write_text(html, encoding='utf-8')

    # === Build Sitemap ===
    print("Building sitemap...")
    urls = [
        f"{SITE_BASE_URL}/index.html",
        f"{SITE_BASE_URL}/az-index.html",
        f"{SITE_BASE_URL}/families.html",
        f"{SITE_BASE_URL}/genera.html",
        f"{SITE_BASE_URL}/stats.html",
        f"{SITE_BASE_URL}/map.html",
        f"{SITE_BASE_URL}/collections.html",
    ]
    for col in collections:
        urls.append(f"{SITE_BASE_URL}/collection/{col['slug']}.html")
    for plant in plants:
        urls.append(f"{SITE_BASE_URL}/plant/{plant['slug']}.html")
    for fam in families:
        urls.append(f"{SITE_BASE_URL}/family/{fam['slug']}.html")
    for genus in genera:
        urls.append(f"{SITE_BASE_URL}/genus/{genus['slug']}.html")
    sitemap_lines = ['<?xml version="1.0" encoding="UTF-8"?>',
                     '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for url in urls:
        sitemap_lines.append(f'  <url><loc>{url}</loc></url>')
    sitemap_lines.append('</urlset>')
    (OUTPUT_DIR / "sitemap.xml").write_text('\n'.join(sitemap_lines), encoding='utf-8')

    conn.close()

    print(f"\n=== Build Complete ===")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Total pages generated: {1 + 1 + 1 + 1 + len(families) + len(genera) + len(plants)}")
    print(f"\nTo view the site, open: {OUTPUT_DIR / 'index.html'}")


if __name__ == "__main__":
    build_site()
