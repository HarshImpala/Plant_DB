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
from urllib.parse import quote
from jinja2 import Environment, FileSystemLoader
from build_content import attach_toxicity_statuses, toxicity_bucket_for_plant, compute_quality_metrics, build_quality_queue_rows, write_build_diff_report, write_api_exports, build_plant_jsonld, load_collections, seed_collections_db


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
BUILD_SNAPSHOT_PATH = DATA_DIR / "build_snapshot.json"
BUILD_DIFF_REPORT_PATH = DATA_DIR / "build_diff_report.json"
PLANT_IMAGES_DIR = STATIC_DIR / "images" / "plants"


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
    env.filters['urlencode'] = lambda value: quote((value or '').strip())
    return env


def get_db_connection():
    """Get database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def clean_native_regions(value):
    """Strip source metadata tail from native region text."""
    text = (value or '').strip()
    if not text:
        return ''
    text = re.sub(r'\s*\|\s*Provided by:.*$', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*Provided by:.*$', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*\|\s*$', '', text)
    return text.strip()


def normalize_common_name(value):
    """Normalize display capitalization for common names."""
    text = (value or '').strip()
    if not text:
        return ''
    return text.title()


def normalize_external_url(value):
    """Return only valid absolute HTTP(S) URLs, otherwise None."""
    text = (value or '').strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered.startswith('http://') or lowered.startswith('https://'):
        return text
    return None


def normalize_image_filename(value):
    """Return image filename only if it exists in static/images/plants."""
    filename = (value or '').strip()
    if not filename:
        return None
    if Path(filename).name != filename:
        return None
    image_path = PLANT_IMAGES_DIR / filename
    if not image_path.exists() or not image_path.is_file():
        return None
    return filename


def normalize_plant_display_fields(plant):
    """Attach consistent display-name fields used across templates."""
    plant['wfo_url'] = normalize_external_url(plant.get('wfo_url'))
    plant['gbif_url'] = normalize_external_url(plant.get('gbif_url'))
    plant['wikipedia_url_english'] = normalize_external_url(
        plant.get('wikipedia_url_english') or plant.get('wikipedia_url')
    )
    plant['wikipedia_url_hungarian'] = normalize_external_url(plant.get('wikipedia_url_hungarian'))
    plant['image_filename'] = normalize_image_filename(plant.get('image_filename'))
    plant['description_english'] = (
        plant.get('description_english') or plant.get('description')
    )
    plant['description_hungarian'] = plant.get('description_hungarian')
    plant['description_hungarian_is_translated'] = int(plant.get('description_hungarian_is_translated') or 0)

    # Compatibility aliases used by existing templates/metrics.
    plant['wikipedia_url'] = plant['wikipedia_url_english']
    plant['description'] = plant['description_english']

    canonical = (plant.get('canonical_name') or '').strip()
    scientific = (plant.get('scientific_name') or '').strip()
    input_name = (plant.get('input_name') or '').strip()
    common_en = normalize_common_name(plant.get('common_name'))
    common_hu = normalize_common_name(plant.get('common_name_hungarian'))
    if common_en and common_hu:
        if common_en.lower() == common_hu.lower():
            common_combined = common_en
        else:
            common_combined = f"{common_en} / {common_hu}"
    else:
        common_combined = common_en or common_hu
    plant['display_name'] = canonical or scientific or input_name
    plant['display_scientific'] = scientific or canonical or input_name
    plant['display_common_en'] = common_en
    plant['display_common_hu'] = common_hu
    plant['display_common'] = common_combined
    plant['display_common_combined'] = common_combined
    plant['native_regions_display'] = clean_native_regions(plant.get('native_regions'))
    plant['native_regions_display_hungarian'] = clean_native_regions(plant.get('native_regions_hungarian'))
    plant['native_countries_list_en'] = split_list_field(plant.get('native_countries'))
    plant['native_countries_list_hu'] = split_list_field(plant.get('native_countries_hungarian'))
    return plant


def get_all_plants(conn):
    """Get all plants with basic info."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM plants ORDER BY canonical_name, scientific_name
    """)
    plants = [dict(row) for row in cursor.fetchall()]
    for plant in plants:
        normalize_plant_display_fields(plant)

    # Add deterministic unique slugs to each plant.
    # If a base slug appears more than once, suffix all variants as -1..-N.
    base_counts = defaultdict(int)
    for plant in plants:
        name = plant['canonical_name'] or plant['scientific_name'] or plant['input_name']
        base = slugify(name) or f"plant-{plant['id']}"
        plant['_base_slug'] = base
        plant['base_slug'] = base
        base_counts[base] += 1

    base_seen = defaultdict(int)
    for plant in plants:
        base = plant['_base_slug']
        if base_counts[base] == 1:
            plant['slug'] = base
        else:
            base_seen[base] += 1
            plant['slug'] = f"{base}-{base_seen[base]}"
        plant.pop('_base_slug', None)

    return plants


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


def build_plant_slug_map(plants):
    """Create a fast lookup of plant ID to precomputed unique slug."""
    return {plant['id']: plant['slug'] for plant in plants}


def build_legacy_slug_redirects(plants):
    """Map legacy unsuffixed slugs to the first canonical duplicate page."""
    grouped = defaultdict(list)
    for plant in plants:
        grouped[plant.get('base_slug')].append(plant['slug'])

    redirects = {}
    for base_slug, slugs in grouped.items():
        if not base_slug or len(slugs) <= 1:
            continue
        sorted_slugs = sorted(slugs, key=str.lower)
        if base_slug not in sorted_slugs:
            redirects[base_slug] = sorted_slugs[0]
    return redirects


def render_redirect_page(target_href):
    """Return a tiny HTML redirect page."""
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="0; url={target_href}">
  <link rel="canonical" href="{target_href}">
  <title>Redirecting...</title>
  <script>window.location.replace({json.dumps(target_href)});</script>
</head>
<body>
  <p>Redirecting to <a href="{target_href}">{target_href}</a>...</p>
</body>
</html>
"""


def preload_plant_synonyms(conn):
    """Load synonyms for all plants in one query."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT plant_id, synonym_name
        FROM plant_synonyms
        ORDER BY plant_id, synonym_name
    """)
    grouped = defaultdict(list)
    for plant_id, synonym_name in cursor.fetchall():
        grouped[plant_id].append(synonym_name)
    return grouped


def preload_plant_common_names(conn):
    """Load common names for all plants in one query."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT plant_id, common_name
        FROM plant_common_names
        ORDER BY plant_id, common_name
    """)
    grouped = defaultdict(list)
    for plant_id, common_name in cursor.fetchall():
        grouped[plant_id].append(normalize_common_name(common_name))
    return grouped


def preload_plants_by_category(conn, slug_by_plant_id):
    """Load plants grouped by category ID in one query."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            pc.category_id,
            p.*
        FROM plant_categories pc
        JOIN plants p ON p.id = pc.plant_id
        ORDER BY pc.category_id, p.canonical_name, p.scientific_name
    """)
    grouped = defaultdict(list)
    seen_by_category = defaultdict(set)
    for row in cursor.fetchall():
        d = dict(row)
        normalize_plant_display_fields(d)
        if d['id'] in seen_by_category[d['category_id']]:
            continue
        seen_by_category[d['category_id']].add(d['id'])
        d['slug'] = slug_by_plant_id.get(d['id'], f"plant-{d['id']}")
        grouped[d['category_id']].append(d)
    return grouped


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


def split_list_field(value):
    """Split comma/semicolon separated text into normalized unique tokens."""
    if not value:
        return []
    parts = re.split(r'[;,]', value)
    cleaned = []
    seen = set()
    for part in parts:
        token = part.strip()
        token_key = token.lower()
        if not token or token_key in seen:
            continue
        seen.add(token_key)
        cleaned.append(token)
    return cleaned


def build_search_data(plants, synonyms_by_plant, common_names_by_plant):
    """Build JSON search data for client-side search."""
    search_data = []

    for plant in plants:
        common_names = common_names_by_plant.get(plant['id'], [])
        synonyms = synonyms_by_plant.get(plant['id'], [])
        merged_common_names = []
        seen_common = set()

        def add_common(name):
            normalized = normalize_common_name(name)
            if not normalized:
                return
            key = normalized.lower()
            if key in seen_common:
                return
            seen_common.add(key)
            merged_common_names.append(normalized)

        add_common(plant.get('common_name'))
        add_common(plant.get('common_name_hungarian'))
        add_common(plant.get('display_common_en'))
        add_common(plant.get('display_common_hu'))
        for name in common_names:
            add_common(name)

        search_data.append({
            'id': plant['id'],
            'slug': plant['slug'],
            'display_name': plant['display_name'],
            'display_scientific': plant['display_scientific'],
            'display_common': plant['display_common'],
            'display_common_hu': plant.get('display_common_hu'),
            'canonical_name': plant['canonical_name'],
            'scientific_name': plant['scientific_name'],
            'common_name': plant['common_name'],
            'common_name_hungarian': plant.get('common_name_hungarian'),
            'common_names': merged_common_names[:40],
            'synonyms': synonyms[:10],  # Limit for file size
        })

    return search_data


def write_search_shards(search_data, search_data_dir):
    """Write prefix-sharded search files for lighter client fetches."""
    buckets = defaultdict(list)

    def shard_key_for_text(text):
        candidate = (text or '').strip().lower()
        first = candidate[:1]
        return first if first.isalpha() else None

    for item in search_data:
        shard_keys = set()

        for text in (
            item.get('display_name'),
            item.get('canonical_name'),
            item.get('scientific_name'),
            item.get('common_name'),
            item.get('common_name_hungarian'),
            item.get('display_common'),
            item.get('display_common_hu'),
        ):
            key = shard_key_for_text(text)
            if key:
                shard_keys.add(key)

        for text in item.get('common_names', []):
            key = shard_key_for_text(text)
            if key:
                shard_keys.add(key)

        for text in item.get('synonyms', []):
            key = shard_key_for_text(text)
            if key:
                shard_keys.add(key)

        if not shard_keys:
            shard_keys.add('_')

        for key in shard_keys:
            buckets[key].append(item)

    index = {
        'version': 1,
        'bucket_keys': sorted(buckets.keys()),
        'total_items': len(search_data),
    }
    (search_data_dir / "search-index.json").write_text(json.dumps(index, ensure_ascii=False), encoding='utf-8')
    for key, items in buckets.items():
        (search_data_dir / f"search-shard-{key}.json").write_text(json.dumps(items, ensure_ascii=False), encoding='utf-8')


def preload_garden_location_map(conn):
    """Load normalized garden location key/display_name by plant ID."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT pgl.plant_id, gl.location_key, gl.display_name
            FROM plant_garden_locations pgl
            JOIN garden_locations gl ON gl.id = pgl.location_id
        """)
    except sqlite3.OperationalError:
        return {}
    return {
        plant_id: {
            'garden_location_key': location_key,
            'garden_location_display': display_name,
        }
        for plant_id, location_key, display_name in cursor.fetchall()
    }


def build_map_locations(plants):
    """Build location -> plants mapping for the map page."""
    grouped = defaultdict(list)
    for plant in plants:
        location_key = (plant.get('garden_location_key') or '').strip()
        location_display = (plant.get('garden_location_display') or plant.get('garden_location') or '').strip()
        if not location_key or not location_display:
            continue
        grouped[(location_key, location_display)].append({
            'slug': plant['slug'],
            'display_name': plant.get('display_name'),
            'display_common': plant.get('display_common'),
            'family': plant.get('family'),
            'genus': plant.get('genus'),
        })

    locations = []
    for location_key, location_display in sorted(grouped.keys(), key=lambda x: x[1].lower()):
        plants_at_location = sorted(grouped[(location_key, location_display)], key=lambda p: (p['display_name'] or '').lower())
        locations.append({
            'location': location_display,
            'location_key': location_key,
            'slug': slugify(location_display) or 'garden-location',
            'plant_count': len(plants_at_location),
            'plants': plants_at_location,
        })
    return locations


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
    (OUTPUT_DIR / "toxicity").mkdir()
    (OUTPUT_DIR / "toxicity" / "toxic").mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "toxicity" / "possibly-toxic").mkdir(parents=True, exist_ok=True)

    # Get all data
    plants = get_all_plants(conn)
    attach_toxicity_statuses(plants)
    if PLACEHOLDER_IMAGES:
        for p in plants:
            p['image_filename'] = None
    garden_locations_by_plant = preload_garden_location_map(conn)
    for plant in plants:
        location_info = garden_locations_by_plant.get(plant['id'])
        if location_info:
            plant.update(location_info)
        else:
            plant['garden_location_key'] = None
            plant['garden_location_display'] = (plant.get('garden_location') or '').strip() or None
    families = get_categories(conn, 'family')
    genera = get_categories(conn, 'genus')
    family_slugs = {f['slug'] for f in families if f.get('slug')}
    genus_slugs = {g['slug'] for g in genera if g.get('slug')}
    for plant in plants:
        family_slug = slugify(plant.get('family'))
        genus_slug = slugify(plant.get('genus'))
        plant['family_slug'] = family_slug if family_slug in family_slugs else None
        plant['genus_slug'] = genus_slug if genus_slug in genus_slugs else None
    synonyms_by_plant = preload_plant_synonyms(conn)
    common_names_by_plant = preload_plant_common_names(conn)
    slug_by_plant_id = build_plant_slug_map(plants)
    plants_by_category = preload_plants_by_category(conn, slug_by_plant_id)
    collections, plant_to_collection = load_collections(plants)
    map_locations = build_map_locations(plants)
    build_diff = write_build_diff_report(plants)
    seed_collections_db(conn, collections)
    print(f"Loaded {len(collections)} collections, seeded to DB")
    print(
        "Build diff: "
        f"added={build_diff['summary']['added']}, "
        f"removed={build_diff['summary']['removed']}, "
        f"changed={build_diff['summary']['changed']}"
    )

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
    featured_source = plants_with_images if plants_with_images else plants
    step = max(1, len(featured_source) // 8)
    featured_plants = featured_source[::step][:8]
    if not featured_plants:
        featured_plants = plants[:8]
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
    facet_families = sorted({p['family'] for p in plants if p.get('family')}, key=str.lower)
    facet_genera = sorted({p['genus'] for p in plants if p.get('genus')}, key=str.lower)
    facet_regions = sorted(
        {region for p in plants for region in split_list_field(p.get('native_regions_display'))},
        key=str.lower,
    )
    facet_regions_hu = sorted(
        {region for p in plants for region in split_list_field(p.get('native_regions_display_hungarian'))},
        key=str.lower,
    )

    html = template.render(
        **base_context,
        plant_count=len(plants),
        letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ',
        used_letters=used_letters,
        plants_by_letter=plants_by_letter,
        facet_families=facet_families,
        facet_genera=facet_genera,
        facet_regions=facet_regions,
        facet_regions_hu=facet_regions_hu,
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
        family_plants = plants_by_category.get(family['id'], [])
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
        genus_plants = plants_by_category.get(genus['id'], [])
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
        synonyms = synonyms_by_plant.get(plant['id'], [])
        common_names = common_names_by_plant.get(plant['id'], [])
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
        plant_jsonld = build_plant_jsonld(plant, common_names, synonyms)
        html = template.render(
            base_url='..', build_version=build_version,
            plant=plant,
            synonyms=synonyms,
            common_names=common_names,
            prev_plant=prev_plant,
            next_plant=next_plant,
            related_plants=related_plants,
            plant_collection=plant_collection,
            plant_jsonld=plant_jsonld,
        )
        (OUTPUT_DIR / "plant" / f"{plant['slug']}.html").write_text(html, encoding='utf-8')

        if (i + 1) % 50 == 0:
            print(f"  Built {i + 1}/{len(plants)} plant pages...")

    # Legacy compatibility: keep old unsuffixed plant URLs working.
    legacy_redirects = build_legacy_slug_redirects(plants)
    for legacy_slug, target_slug in legacy_redirects.items():
        target_href = f"./{target_slug}.html"
        redirect_html = render_redirect_page(target_href)
        (OUTPUT_DIR / "plant" / f"{legacy_slug}.html").write_text(redirect_html, encoding='utf-8')

    # === Build Search Data ===
    print("Building search data...")
    search_data = build_search_data(plants, synonyms_by_plant, common_names_by_plant)
    search_data_dir = OUTPUT_DIR / "static" / "data"
    search_data_dir.mkdir(parents=True, exist_ok=True)
    (search_data_dir / "search-data.json").write_text(json.dumps(search_data, ensure_ascii=False), encoding='utf-8')
    write_search_shards(search_data, search_data_dir)

    # === Build API Exports ===
    print("Building API exports...")
    write_api_exports(build_version, plants, families, genera, collections, map_locations)

    # === Build Stats Page ===
    print("Building stats page...")
    template = env.get_template('stats.html')
    top_families = sorted(families, key=lambda f: f['plant_count'], reverse=True)[:15]
    quality_metrics = compute_quality_metrics(plants)
    html = template.render(
        **base_context,
        total_plants=len(plants),
        plants_with_images=sum(1 for p in plants if p.get('image_filename')),
        plants_with_descriptions=sum(1 for p in plants if p.get('description')),
        plants_with_distribution=sum(1 for p in plants if p.get('native_countries')),
        total_families=len(families),
        total_genera=len(genera),
        top_families=top_families,
        coverage_rows=quality_metrics['coverage_rows'],
        overall_completeness=quality_metrics['overall_completeness'],
    )
    (OUTPUT_DIR / "stats.html").write_text(html, encoding='utf-8')

    # === Build Content Quality Queue ===
    print("Building quality queue page...")
    template = env.get_template('quality_queue.html')
    quality_rows = build_quality_queue_rows(plants)
    html = template.render(
        **base_context,
        queue_rows=quality_rows,
        queue_count=len(quality_rows),
    )
    (OUTPUT_DIR / "quality-queue.html").write_text(html, encoding='utf-8')

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
    html = template.render(
        **base_context,
        map_locations=map_locations,
        mapped_plant_count=sum(location['plant_count'] for location in map_locations),
    )
    (OUTPUT_DIR / "map.html").write_text(html, encoding='utf-8')

    # === Build Design Demo Pages ===
    print("Building design demo pages...")
    template = env.get_template('demo_magazine.html')
    html = template.render(**base_context)
    (OUTPUT_DIR / "demo-magazine.html").write_text(html, encoding='utf-8')

    template = env.get_template('demo_museum.html')
    html = template.render(**base_context)
    (OUTPUT_DIR / "demo-museum.html").write_text(html, encoding='utf-8')

    template = env.get_template('demo_herbarium.html')
    html = template.render(**base_context)
    (OUTPUT_DIR / "demo-herbarium.html").write_text(html, encoding='utf-8')

    template = env.get_template('demo_conservatory.html')
    html = template.render(**base_context)
    (OUTPUT_DIR / "demo-conservatory.html").write_text(html, encoding='utf-8')

    template = env.get_template('demo_minimal.html')
    html = template.render(**base_context)
    (OUTPUT_DIR / "demo-minimal.html").write_text(html, encoding='utf-8')

    template = env.get_template('demo_field_journal.html')
    html = template.render(**base_context)
    (OUTPUT_DIR / "demo-field-journal.html").write_text(html, encoding='utf-8')

    # === Build Toxicity Pages ===
    print("Building toxicity pages...")
    template = env.get_template('toxicity_list.html')
    toxic_plants = [p for p in plants if toxicity_bucket_for_plant(p) == 'toxic']
    possibly_toxic_plants = [p for p in plants if toxicity_bucket_for_plant(p) == 'possibly-toxic']
    toxicity_index_html = template.render(
        base_url='..',
        build_version=build_version,
        title='Toxicity Overview',
        description='Browse plants flagged as toxic or possibly toxic from structured toxicity consensus.',
        plants=sorted(toxic_plants + possibly_toxic_plants, key=lambda p: (p.get('display_name') or '').lower()),
        total_plants=len(plants),
        toxic_count=len(toxic_plants),
        possibly_toxic_count=len(possibly_toxic_plants),
        current_scope='all',
    )
    (OUTPUT_DIR / "toxicity" / "index.html").write_text(toxicity_index_html, encoding='utf-8')

    toxicity_toxic_html = template.render(
        base_url='../..',
        build_version=build_version,
        title='Toxic Plants',
        description='Plants with toxic status from weighted toxicity consensus.',
        plants=sorted(toxic_plants, key=lambda p: (p.get('display_name') or '').lower()),
        total_plants=len(plants),
        toxic_count=len(toxic_plants),
        possibly_toxic_count=len(possibly_toxic_plants),
        current_scope='toxic',
    )
    (OUTPUT_DIR / "toxicity" / "toxic" / "index.html").write_text(toxicity_toxic_html, encoding='utf-8')

    toxicity_possibly_html = template.render(
        base_url='../..',
        build_version=build_version,
        title='Possibly Toxic Plants',
        description='Plants marked possibly toxic (including family-level inference) from weighted toxicity consensus.',
        plants=sorted(possibly_toxic_plants, key=lambda p: (p.get('display_name') or '').lower()),
        total_plants=len(plants),
        toxic_count=len(toxic_plants),
        possibly_toxic_count=len(possibly_toxic_plants),
        current_scope='possibly-toxic',
    )
    (OUTPUT_DIR / "toxicity" / "possibly-toxic" / "index.html").write_text(toxicity_possibly_html, encoding='utf-8')

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
        f"{SITE_BASE_URL}/quality-queue.html",
        f"{SITE_BASE_URL}/map.html",
        f"{SITE_BASE_URL}/collections.html",
        f"{SITE_BASE_URL}/demo-magazine.html",
        f"{SITE_BASE_URL}/demo-museum.html",
        f"{SITE_BASE_URL}/demo-herbarium.html",
        f"{SITE_BASE_URL}/demo-conservatory.html",
        f"{SITE_BASE_URL}/demo-minimal.html",
        f"{SITE_BASE_URL}/demo-field-journal.html",
        f"{SITE_BASE_URL}/toxicity/index.html",
        f"{SITE_BASE_URL}/toxicity/toxic/index.html",
        f"{SITE_BASE_URL}/toxicity/possibly-toxic/index.html",
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
