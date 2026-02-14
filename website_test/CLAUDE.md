# Plant Encyclopedia - Static Site Generator

A Python-based static site generator that creates a comprehensive plant encyclopedia website from botanical data sourced from GBIF, World Flora Online (WFO), and Wikipedia.

## Tech Stack

- **Data Processing**: Python 3.x with pandas for Excel import
- **Database**: SQLite for structured plant data
- **Templating**: Jinja2 for HTML generation
- **APIs**: Wikidata/Wikipedia API for enrichment
- **Output**: Static HTML/CSS/JS website

## Project Structure

```
PostgreSQL_DB/
├── website_test/                    # Main website project
│   ├── generator/                   # Build scripts
│   │   ├── import_data.py          # Import Excel → SQLite
│   │   ├── build_site.py           # Generate static site
│   │   ├── fetch_wikipedia_urls.py # Find Wikipedia pages
│   │   ├── fetch_wikipedia_intros.py # Get plant descriptions
│   │   └── fetch_wikipedia_images.py # Download plant images
│   ├── templates/                   # Jinja2 templates
│   │   ├── base.html               # Base layout
│   │   ├── index.html              # Homepage
│   │   ├── az_index.html           # A-Z plant listing
│   │   ├── plant.html              # Individual plant page
│   │   ├── category.html           # Family/genus page
│   │   └── category_list.html      # List of families/genera
│   ├── static/                      # CSS, JS, images
│   ├── data/                        # Database and cache files
│   │   ├── plants.db               # SQLite database
│   │   └── wikipedia_cache.json    # API response cache
│   └── output/                      # Generated static site
└── new_scripts_WFO_main_source/    # Data source scripts
    ├── taxonomy/                    # Plant taxonomy data
    ├── location/                    # Native region data
    ├── toxicity/                    # Toxicity information
    └── wikipedia_info_pull/         # Wikipedia fetchers
```

## Database Schema

Located in `generator/import_data.py`:

### Core Tables
- **plants**: Main plant records with taxonomy, IDs, and URLs
- **plant_synonyms**: Alternative scientific names (one-to-many)
- **plant_common_names**: Common names in various languages (one-to-many)
- **plant_native_regions**: Native countries and regions (one-to-many)
- **categories**: Families and genera organization
- **plant_categories**: Plant-category relationships (many-to-many)

### Key Fields in plants Table
- `input_name`: Original plant name from source data
- `scientific_name`: Full scientific name with author
- `canonical_name`: Simple scientific name without author
- `common_name`: Primary common name
- `family`, `genus`: Taxonomic classification
- `wfo_id`, `wfo_url`: World Flora Online identifiers
- `gbif_usage_key`, `gbif_url`: GBIF species identifiers
- `wikipedia_url`: English Wikipedia page URL
- `native_countries`, `native_regions`: Native distribution
- `toxicity_info`, `description`: (Placeholder for future data)

## Build Pipeline

### 1. Import Data (`import_data.py`)
```bash
python generator/import_data.py
```

**What it does:**
- Reads Excel files from `new_scripts_WFO_main_source/`
- Creates/recreates SQLite database at `data/plants.db`
- Imports taxonomy data (scientific names, synonyms, families)
- Imports location data (native countries/regions)
- Creates category hierarchies (families, genera)
- Builds indexes for performance

**Source Files:**
- `taxonomy/plants_gbif_matched_plus_wfo_syn_diff_and_taxonomy.xlsx`
- `location/plants_gbif_with_native_plus_wfo.xlsx`

### 2. Fetch Wikipedia URLs (`fetch_wikipedia_urls.py`)
```bash
python generator/fetch_wikipedia_urls.py
```

**What it does:**
- Queries Wikidata API for each plant by canonical name
- Retrieves English Wikipedia URL from sitelinks
- Caches results in `data/wikipedia_cache.json`
- Updates `plants.wikipedia_url` field
- Rate-limited with 0.5s delay between requests
- Retries on rate limits with exponential backoff

### 3. Fetch Wikipedia Intros (`fetch_wikipedia_intros.py`)
```bash
python generator/fetch_wikipedia_intros.py
```

**What it does:**
- Fetches first paragraph from Wikipedia for plants with URLs
- Updates `plants.description` field
- Uses Wikipedia API extract endpoint

### 4. Fetch Wikipedia Images (`fetch_wikipedia_images.py`)
```bash
python generator/fetch_wikipedia_images.py
```

**What it does:**
- Downloads main image from Wikipedia pages
- Saves to `static/images/plants/`
- Updates `plants.image_filename` field
- Handles image resizing and optimization

### 5. Build Site (`build_site.py`)
```bash
python generator/build_site.py
```

**What it does:**
- Clears `output/` directory
- Copies `static/` files to `output/static/`
- Generates homepage with featured plants
- Creates A-Z index with letter navigation
- Builds family and genus list pages
- Generates individual pages for:
  - Each plant (`output/plant/{slug}.html`)
  - Each family (`output/family/{slug}.html`)
  - Each genus (`output/genus/{slug}.html`)
- Creates `search-data.json` for client-side search

**Output:**
- Fully static website in `output/` directory
- Open `output/index.html` to view

## Data Sources

### GBIF (Global Biodiversity Information Facility)
- Scientific names and taxonomic verification
- Synonyms and common names
- Usage keys for cross-referencing
- Native region confidence levels

### WFO (World Flora Online)
- Taxonomic hierarchy (family, genus)
- WFO IDs for standardized identification
- Additional synonyms
- Native country/region data
- Habitat information

### Wikipedia/Wikidata
- Plant descriptions (first paragraph)
- Images
- Common names in multiple languages

## Key Features

### Taxonomic Organization
- Browse by botanical family (Araceae, Orchidaceae, etc.)
- Browse by genus (Monstera, Anthurium, etc.)
- Proper taxonomic hierarchy

### Search & Navigation
- A-Z index with letter shortcuts
- Client-side search (searches names, synonyms, common names)
- Category filtering

### Plant Pages Include
- Scientific name with author
- Common names
- Synonyms
- Taxonomic family and genus
- Native regions/countries
- Wikipedia description
- External links to GBIF, WFO, Wikipedia
- Images (when available)

### URL Structure
- Homepage: `/index.html`
- A-Z Index: `/az-index.html`
- Families: `/families.html`
- Genera: `/genera.html`
- Individual plant: `/plant/{slug}.html`
- Family page: `/family/{slug}.html`
- Genus page: `/genus/{slug}.html`

## Slugification

Implemented in `build_site.py:slugify()`:
- Removes author citations (text in parentheses)
- Removes trailing author names
- Converts to lowercase
- Replaces spaces/special chars with hyphens
- Used for all URLs and file names

## Development Workflow

### Full Rebuild
```bash
cd PostgreSQL_DB/website_test/generator
python import_data.py              # Import Excel data
python fetch_wikipedia_urls.py     # Get Wikipedia links
python fetch_wikipedia_intros.py   # Get descriptions
python fetch_wikipedia_images.py   # Download images
python build_site.py               # Generate site
```

### Quick Rebuild (after template changes)
```bash
python generator/build_site.py
```

### View Site
Open `output/index.html` in a web browser

## Important Patterns

### Pipe-Separated Values
Excel files use `|` to separate multiple values in single cells:
- Synonyms: `Monstera deliciosa var. borsigiana | Philodendron pertusum`
- Common names: `Swiss cheese plant | Monstera | Split-leaf philodendron`
- Native countries: `Mexico | Guatemala | Panama`

Parsed by `import_data.py:parse_pipe_separated()`

### Caching Strategy
- Wikipedia API responses cached in `wikipedia_cache.json`
- Prevents re-fetching data during rebuilds
- Cache persists across runs
- "NOT_FOUND" cached for plants without Wikipedia pages

### Error Handling
- Excel import skips rows with missing `input_name`
- Wikipedia fetchers use exponential backoff on rate limits
- Failed API requests logged but don't stop the build
- Database uses `INSERT OR IGNORE` to prevent duplicates

### Template Context
All templates receive `base_url` for relative linking:
- Homepage/lists: `base_url = '.'`
- Plant/category pages: `base_url = '..'`

## Database Queries

Common operations:

```sql
-- Get all plants in a family
SELECT p.* FROM plants p
JOIN plant_categories pc ON p.id = pc.plant_id
JOIN categories c ON pc.category_id = c.id
WHERE c.name = 'Araceae' AND c.category_type = 'family';

-- Get synonyms for a plant
SELECT synonym_name FROM plant_synonyms
WHERE plant_id = ? ORDER BY synonym_name;

-- Count plants per family
SELECT c.name, COUNT(pc.plant_id) as count
FROM categories c
LEFT JOIN plant_categories pc ON c.id = pc.category_id
WHERE c.category_type = 'family'
GROUP BY c.id;
```

## Future Enhancements (Placeholder Fields)

- `toxicity_info`: Pet/human toxicity data
- `garden_location`: Physical location in botanical garden
- `curator_comments`: Internal notes
- Additional language support for common names

## Notes

- Currently no dynamic backend - fully static site
- Search is client-side JavaScript (loads `search-data.json`)
- Images stored locally in `static/images/plants/`
- Excel files must exist at specified paths in `import_data.py`
- Database is recreated on each import (destructive)
