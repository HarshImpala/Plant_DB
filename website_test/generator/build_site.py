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


def normalize_plant_display_fields(plant):
    """Attach consistent display-name fields used across templates."""
    plant['wikipedia_url_english'] = (
        plant.get('wikipedia_url_english') or plant.get('wikipedia_url')
    )
    plant['wikipedia_url_hungarian'] = plant.get('wikipedia_url_hungarian')
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
            'common_names': common_names[:10],  # Limit for file size
            'synonyms': synonyms[:10],  # Limit for file size
        })

    return search_data


def write_search_shards(search_data, search_data_dir):
    """Write prefix-sharded search files for lighter client fetches."""
    buckets = defaultdict(list)
    for item in search_data:
        candidate = (item.get('display_name') or item.get('canonical_name') or item.get('scientific_name') or '').strip().lower()
        first = candidate[:1]
        key = first if first.isalpha() else '_'
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


def _toxicity_flags_from_text(text):
    """Extract simple toxicity flags from free-text toxicity_info."""
    t = (text or "").strip().lower()
    if not t:
        return {
            'pets_toxic': False,
            'pets_not_toxic': False,
            'humans_toxic': False,
            'humans_not_toxic': False,
        }

    pets_toxic = False
    pets_not_toxic = False
    humans_toxic = False
    humans_not_toxic = False

    has_toxic_word = 'toxic' in t
    has_not_toxic_phrase = 'non-toxic' in t or 'not toxic' in t

    has_pets = any(word in t for word in ('dog', 'dogs', 'cat', 'cats', 'pet', 'pets', 'horse', 'horses'))
    has_humans = any(word in t for word in ('human', 'humans', 'people', 'person'))

    if has_pets and has_toxic_word and not has_not_toxic_phrase:
        pets_toxic = True
    if has_pets and has_not_toxic_phrase:
        pets_not_toxic = True

    if has_humans and has_toxic_word and not has_not_toxic_phrase:
        humans_toxic = True
    if has_humans and has_not_toxic_phrase:
        humans_not_toxic = True

    return {
        'pets_toxic': pets_toxic,
        'pets_not_toxic': pets_not_toxic,
        'humans_toxic': humans_toxic,
        'humans_not_toxic': humans_not_toxic,
    }


def attach_toxicity_statuses(plants):
    """Attach normalized toxicity statuses to each plant for UI display."""
    hu_status_map = {
        'toxic': 'mergezo',
        'not toxic': 'nem mergezo',
        'unknown': 'ismeretlen',
        'family known toxic': 'csaladban ismerten mergezo',
    }
    family_toxic_counts = defaultdict(int)

    for plant in plants:
        flags = _toxicity_flags_from_text(plant.get('toxicity_info'))
        plant['_tox_flags'] = flags
        family = (plant.get('family') or '').strip().lower()
        if family and (flags['pets_toxic'] or flags['humans_toxic']):
            family_toxic_counts[family] += 1

    for plant in plants:
        flags = plant.get('_tox_flags', {})
        family = (plant.get('family') or '').strip().lower()
        family_known_toxic = bool(family and family_toxic_counts.get(family, 0) >= 2)

        if flags.get('humans_toxic'):
            human_status = 'toxic'
        elif flags.get('humans_not_toxic'):
            human_status = 'not toxic'
        elif family_known_toxic:
            human_status = 'family known toxic'
        else:
            human_status = 'unknown'

        if flags.get('pets_toxic'):
            pets_status = 'toxic'
        elif flags.get('pets_not_toxic'):
            pets_status = 'not toxic'
        elif family_known_toxic:
            pets_status = 'family known toxic'
        else:
            pets_status = 'unknown'

        plant['toxicity_humans_status'] = human_status
        plant['toxicity_pets_status'] = pets_status
        plant['toxicity_humans_status_en'] = human_status
        plant['toxicity_pets_status_en'] = pets_status
        plant['toxicity_humans_status_hu'] = hu_status_map[human_status]
        plant['toxicity_pets_status_hu'] = hu_status_map[pets_status]
        plant.pop('_tox_flags', None)


def compute_quality_metrics(plants):
    """Compute coverage/completeness metrics for the stats dashboard."""
    total = len(plants) or 1
    tracked_fields = [
        ('Image', 'quality_image', 'image_filename'),
        ('Description', 'quality_description', 'description'),
        ('Distribution', 'quality_distribution', 'native_countries'),
        ('Wikipedia URL', 'quality_wikipedia_url', 'wikipedia_url'),
        ('WFO Link', 'quality_wfo_link', 'wfo_url'),
        ('Garden Location', 'quality_garden_location', 'garden_location'),
        ('Toxicity Info', 'quality_toxicity_info', 'toxicity_info'),
    ]

    coverage_rows = []
    completeness_points = 0
    max_points = len(tracked_fields) * len(plants)

    for label, label_key, key in tracked_fields:
        count = sum(1 for plant in plants if plant.get(key))
        completeness_points += count
        coverage_rows.append({
            'label': label,
            'label_key': label_key,
            'count': count,
            'missing': len(plants) - count,
            'pct': round((count / total) * 100, 1),
        })

    coverage_rows.sort(key=lambda row: row['pct'], reverse=True)
    overall_completeness = round((completeness_points / max_points) * 100, 1) if max_points else 0.0

    return {
        'coverage_rows': coverage_rows,
        'overall_completeness': overall_completeness,
    }


def build_quality_queue_rows(plants):
    """Build curator queue rows for plants missing key content fields."""
    checks = [
        ('image_filename', 'missing_image'),
        ('description', 'missing_description'),
        ('native_countries', 'missing_distribution'),
        ('wikipedia_url', 'missing_wikipedia'),
        ('toxicity_info', 'missing_toxicity'),
    ]
    rows = []
    for plant in plants:
        missing = [label for key, label in checks if not plant.get(key)]
        if not missing:
            continue
        rows.append({
            'slug': plant['slug'],
            'display_name': plant.get('display_name'),
            'family': plant.get('family'),
            'genus': plant.get('genus'),
            'missing': missing,
            'missing_count': len(missing),
        })
    rows.sort(key=lambda r: (-r['missing_count'], (r['display_name'] or '').lower()))
    return rows


def build_snapshot(plants):
    """Create a compact snapshot used for build-to-build diff reporting."""
    snapshot = {}
    for plant in plants:
        snapshot[plant['slug']] = {
            'id': plant['id'],
            'input_name': plant.get('input_name'),
            'display_name': plant.get('display_name'),
            'family': plant.get('family'),
            'genus': plant.get('genus'),
            'has_image': bool(plant.get('image_filename')),
            'has_description': bool(plant.get('description')),
            'has_distribution': bool(plant.get('native_countries')),
        }
    return snapshot


def write_build_diff_report(plants):
    """Compare current snapshot against previous build and write a diff report."""
    current = build_snapshot(plants)
    if BUILD_SNAPSHOT_PATH.exists():
        try:
            previous = json.loads(BUILD_SNAPSHOT_PATH.read_text(encoding='utf-8'))
        except json.JSONDecodeError:
            previous = {}
    else:
        previous = {}

    prev_slugs = set(previous.keys())
    curr_slugs = set(current.keys())
    added = sorted(curr_slugs - prev_slugs)
    removed = sorted(prev_slugs - curr_slugs)
    common = sorted(curr_slugs & prev_slugs)

    changed = []
    for slug in common:
        before = previous.get(slug, {})
        after = current.get(slug, {})
        changed_fields = [key for key in after.keys() if before.get(key) != after.get(key)]
        if changed_fields:
            changed.append({
                'slug': slug,
                'changed_fields': changed_fields,
                'before': {k: before.get(k) for k in changed_fields},
                'after': {k: after.get(k) for k in changed_fields},
            })

    report = {
        'summary': {
            'previous_count': len(previous),
            'current_count': len(current),
            'added': len(added),
            'removed': len(removed),
            'changed': len(changed),
        },
        'added': [{'slug': slug, **current[slug]} for slug in added],
        'removed': [{'slug': slug, **previous[slug]} for slug in removed],
        'changed': changed,
    }

    BUILD_DIFF_REPORT_PATH.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding='utf-8')
    BUILD_SNAPSHOT_PATH.write_text(json.dumps(current, indent=2, ensure_ascii=False), encoding='utf-8')
    return report


def write_api_exports(build_version, plants, families, genera, collections, map_locations):
    """Write versioned JSON exports for future API-driven UI features."""
    api_dir = OUTPUT_DIR / "static" / "api" / "v1"
    api_dir.mkdir(parents=True, exist_ok=True)

    plants_export = []
    for plant in plants:
        plants_export.append({
            'id': plant['id'],
            'slug': plant['slug'],
            'display_name': plant.get('display_name'),
            'display_scientific': plant.get('display_scientific'),
            'display_common': plant.get('display_common'),
            'family': plant.get('family'),
            'genus': plant.get('genus'),
            'garden_location_key': plant.get('garden_location_key'),
            'garden_location_display': plant.get('garden_location_display'),
            'has_image': bool(plant.get('image_filename')),
            'has_description': bool(plant.get('description')),
            'has_distribution': bool(plant.get('native_countries')),
        })

    families_export = [{'name': f['name'], 'slug': f['slug'], 'plant_count': f['plant_count']} for f in families]
    genera_export = [{'name': g['name'], 'slug': g['slug'], 'plant_count': g['plant_count']} for g in genera]
    collections_export = [
        {'slug': c['slug'], 'name_en': c['name_en'], 'name_hu': c['name_hu'], 'plant_count': c['plant_count']}
        for c in collections
    ]
    locations_export = [
        {'location_key': l['location_key'], 'location': l['location'], 'plant_count': l['plant_count']}
        for l in map_locations
    ]

    manifest = {
        'version': 'v1',
        'build_version': build_version,
        'generated_at_unix': int(build_version),
        'counts': {
            'plants': len(plants_export),
            'families': len(families_export),
            'genera': len(genera_export),
            'collections': len(collections_export),
            'locations': len(locations_export),
        },
        'files': {
            'plants': 'plants.json',
            'families': 'families.json',
            'genera': 'genera.json',
            'collections': 'collections.json',
            'locations': 'locations.json',
        },
    }

    (api_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    (api_dir / "plants.json").write_text(json.dumps(plants_export, ensure_ascii=False), encoding='utf-8')
    (api_dir / "families.json").write_text(json.dumps(families_export, ensure_ascii=False), encoding='utf-8')
    (api_dir / "genera.json").write_text(json.dumps(genera_export, ensure_ascii=False), encoding='utf-8')
    (api_dir / "collections.json").write_text(json.dumps(collections_export, ensure_ascii=False), encoding='utf-8')
    (api_dir / "locations.json").write_text(json.dumps(locations_export, ensure_ascii=False), encoding='utf-8')


def build_plant_jsonld(plant, common_names, synonyms):
    """Build JSON-LD metadata for a plant page."""
    display_name = plant.get('canonical_name') or plant.get('scientific_name') or plant.get('input_name')
    page_url = f"{SITE_BASE_URL}/plant/{plant['slug']}.html"
    jsonld = {
        "@context": "https://schema.org",
        "@type": "Taxon",
        "name": display_name,
        "url": page_url,
    }

    if plant.get('description'):
        jsonld["description"] = plant['description']

    alternate_names = []
    scientific_name = plant.get('scientific_name')
    if scientific_name and scientific_name != display_name:
        alternate_names.append(scientific_name)
    if plant.get('common_name'):
        alternate_names.append(plant['common_name'])
    alternate_names.extend(common_names[:8])
    alternate_names.extend(synonyms[:8])
    if alternate_names:
        seen = set()
        unique_names = []
        for name in alternate_names:
            key = name.strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            unique_names.append(name)
        if unique_names:
            jsonld["alternateName"] = unique_names

    if plant.get('image_filename'):
        jsonld["image"] = f"{SITE_BASE_URL}/static/images/plants/{plant['image_filename']}"

    if plant.get('wfo_id'):
        jsonld["identifier"] = f"WFO:{plant['wfo_id']}"

    same_as = [url for url in (plant.get('wfo_url'), plant.get('gbif_url'), plant.get('wikipedia_url')) if url]
    if same_as:
        jsonld["sameAs"] = same_as

    parent_taxa = []
    if plant.get('genus'):
        parent_taxa.append(plant['genus'])
    if plant.get('family'):
        parent_taxa.append(plant['family'])
    if parent_taxa:
        jsonld["parentTaxon"] = " > ".join(parent_taxa)

    return json.dumps(jsonld, ensure_ascii=False, separators=(',', ':'))


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
    facet_families = sorted({p['family'] for p in plants if p.get('family')}, key=str.lower)
    facet_genera = sorted({p['genus'] for p in plants if p.get('genus')}, key=str.lower)
    facet_regions = sorted(
        {region for p in plants for region in split_list_field(p.get('native_regions_display'))},
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
