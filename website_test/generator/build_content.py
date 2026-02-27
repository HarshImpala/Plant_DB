"""Supporting content/build utilities extracted from build_site.py."""

import os
import json
import re
from pathlib import Path
from collections import defaultdict

# Local constants mirror build_site.py paths
BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / "output"
DATA_DIR = BASE_DIR / "data"
COLLECTIONS_PATH = DATA_DIR / "collections.json"
BUILD_SNAPSHOT_PATH = DATA_DIR / "build_snapshot.json"
BUILD_DIFF_REPORT_PATH = DATA_DIR / "build_diff_report.json"
SITE_BASE_URL = os.environ.get("SITE_BASE_URL", "https://example.com").rstrip("/")

def slugify(text):
    """Convert text to URL-friendly slug."""
    if not text:
        return ""
    text = re.sub(r'\s+\([^)]+\)\s*$', '', text)
    text = re.sub(r'\s+[A-Z][a-z]*\.?\s*$', '', text)
    text = text.lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)
    return text

def toxicity_bucket_for_plant(plant):
    """Map plant toxicity info to a page bucket: toxic / possibly-toxic / other."""
    status = _normalize_toxicity_status(plant.get('toxicity_status_overall'))
    if status == 'toxic':
        return 'toxic'
    if status in ('possibly_toxic', 'family_known_toxic'):
        return 'possibly-toxic'

    humans = (plant.get('toxicity_humans_status_en') or '').strip().lower()
    pets = (plant.get('toxicity_pets_status_en') or '').strip().lower()
    if 'toxic' == humans or 'toxic' == pets:
        return 'toxic'
    if 'family known toxic' in (humans, pets):
        return 'possibly-toxic'
    return 'other'


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


def _extract_toxicity_source(text):
    """Extract a source citation from free-text toxicity info."""
    raw = (text or "").strip()
    if not raw:
        return None

    # Typical forms:
    # "... (Source: ASPCA)"
    # "... Source: ASPCA"
    m = re.search(r"\(source:\s*([^)]+)\)", raw, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()

    m = re.search(r"source:\s*(.+)$", raw, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip().strip("().")

    return None


def _normalize_toxicity_status(value):
    raw = (value or '').strip().lower().replace('-', '_').replace(' ', '_')
    if raw in ('toxic', 'possibly_toxic', 'unknown', 'not_toxic', 'family_known_toxic'):
        return raw
    return None


def _display_toxicity_status(status_key):
    mapping = {
        'toxic': 'toxic',
        'possibly_toxic': 'toxic',
        'unknown': 'unknown',
        'not_toxic': 'not toxic',
        'family_known_toxic': 'family known toxic',
    }
    return mapping.get(status_key, 'unknown')


def _combine_pet_status(cats_key, dogs_key):
    severity = {
        'not_toxic': 0,
        'unknown': 1,
        'possibly_toxic': 2,
        'family_known_toxic': 2.2,
        'toxic': 3,
    }
    cats_key = cats_key or 'unknown'
    dogs_key = dogs_key or 'unknown'
    return cats_key if severity[cats_key] >= severity[dogs_key] else dogs_key


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
        db_humans = _normalize_toxicity_status(plant.get('toxicity_status_humans'))
        db_cats = _normalize_toxicity_status(plant.get('toxicity_status_cats'))
        db_dogs = _normalize_toxicity_status(plant.get('toxicity_status_dogs'))
        db_family_inference = bool((plant.get('toxicity_status_family_inference') or '').strip())
        has_structured = any([db_humans, db_cats, db_dogs])

        if has_structured:
            humans_key = db_humans or ('family_known_toxic' if db_family_inference else 'unknown')
            cats_key = db_cats or ('family_known_toxic' if db_family_inference else 'unknown')
            dogs_key = db_dogs or ('family_known_toxic' if db_family_inference else 'unknown')
            pets_key = _combine_pet_status(cats_key, dogs_key)
            human_status = _display_toxicity_status(humans_key)
            pets_status = _display_toxicity_status(pets_key)
            toxicity_source = plant.get('toxicity_status_source') or _extract_toxicity_source(plant.get('toxicity_info'))
        else:
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
            toxicity_source = _extract_toxicity_source(plant.get('toxicity_info'))

        plant['toxicity_humans_status'] = human_status
        plant['toxicity_pets_status'] = pets_status
        plant['toxicity_humans_status_en'] = human_status
        plant['toxicity_pets_status_en'] = pets_status
        plant['toxicity_humans_status_hu'] = hu_status_map[human_status]
        plant['toxicity_pets_status_hu'] = hu_status_map[pets_status]
        plant['toxicity_source'] = toxicity_source
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


