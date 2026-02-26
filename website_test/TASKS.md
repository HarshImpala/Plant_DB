# Website Test - Tasks

## Priority Tasks

- [x] Fix slug collisions in generated URLs/pages (deterministic unique slug strategy in `generator/build_site.py`)
  - Progress (2026-02-24): added deterministic duplicate handling where collisions are emitted as `slug-1..slug-N` instead of overwriting pages.
  - Progress (2026-02-24): rebuilt static output so duplicate canonical names now produce distinct `output/plant/*.html` files.
- [x] Make import deterministic and non-destructive (incremental mode + explicit full rebuild in `generator/import_data.py`)
  - Progress (2026-02-24): switched plant writes to SQLite upsert (`ON CONFLICT(input_name) DO UPDATE`) to preserve stable IDs.
  - Progress (2026-02-24): added explicit `--full-rebuild` destructive mode; default run now preserves DB and updates in place.
  - Progress (2026-02-24): relationship/location child records are replaced per plant during import to avoid stale duplicates.
- [x] Add a pre-build validation step (duplicate slugs, missing critical fields, broken links) and fail CI on critical issues
  - Progress (2026-02-24): added `generator/validate_data.py` with checks for final slug uniqueness, missing key name fields, URL format, and missing image file references.
  - Progress (2026-02-24): validator writes `data/validation_report.json` and exits non-zero on critical failures for CI gating.
  - Progress (2026-02-24): wired validation into deploy workflow before build in `.github/workflows/deploy.yml`.
- [x] Improve Wikipedia matching quality (disambiguation checks + fallback query strategy in `generator/fetch_wikipedia_urls.py`)
  - Progress (2026-02-24): upgraded Wikidata search to fetch multiple candidates and rank them with botanical relevance scoring.
  - Progress (2026-02-24): added fallback query strategy (`canonical`, `scientific`, and `... plant` variants) before declaring `NOT_FOUND`.
  - Progress (2026-02-24): added ambiguity protection to skip weak/disambiguation-like matches.
- [x] Optimize build performance by removing N+1 DB query patterns in `generator/build_site.py`
  - Progress (2026-02-24): switched plant page/search/category data hydration to shared preloaded maps instead of per-plant/per-category DB lookups.
  - Progress (2026-02-24): category preload now reuses canonical generated slugs (`slug_by_plant_id`) and deduplicates repeated plant/category rows.

## UX and Discovery

- [x] Add faceted browsing/filtering (family, genus, native region, toxicity, has image/description)
  - Progress (2026-02-24): implemented facet controls on `az-index.html` for family, genus, native region, toxicity info, image presence, and description presence.
  - Progress (2026-02-24): added client-side filtering with live visible-count updates and dynamic letter navigation disabling for filtered-out sections.
- [x] Add structured metadata (JSON-LD) for plant pages in `templates/plant.html`
  - Progress (2026-02-24): added build-time JSON-LD generation (`@type: Taxon`) with canonical URL, alternate names, parent taxonomy, identifiers, image, and source links.
  - Progress (2026-02-24): injected metadata into plant page `<head>` as `application/ld+json`.
- [x] Integrate `garden_location` with map/plant pages for "find in garden" workflows
  - Progress (2026-02-24): added map-page location index grouped from `garden_location` with per-location plant links and quick filtering.
  - Progress (2026-02-24): added deep links from plant pages to `map.html?location=...` with auto-focus/highlight on the map page.

## Do Later

- [x] Expand stats page into data quality dashboard (coverage + completeness metrics)
  - Progress (2026-02-24): added generator-side quality metric computation (field coverage + overall completeness score).
  - Progress (2026-02-24): expanded `stats.html` with a Data Quality Dashboard section and coverage bars for key content fields.

## Platform and Delivery

- [x] Refine GitHub Pages workflow trigger strategy (build from main, deploy artifact) in `.github/workflows/deploy.yml`
  - Progress (2026-02-24): updated workflow triggers to `main` for `push` and `pull_request`, with path filters to avoid unnecessary builds.
  - Progress (2026-02-24): added `actions/configure-pages@v5` and gated deploy job to push/manual runs while preserving artifact-based deployment.

## Next Improvements

- [x] Add automated regression checks for generated pages (`generator/smoke_test.py`)
  - Progress (2026-02-24): added smoke checks for broken internal links/assets and invalid/empty JSON-LD in generated HTML.
  - Progress (2026-02-24): wired smoke test into CI workflow after site build so deploy is blocked on failures.
- [ ] Improve mobile layout and usability (navigation, filters, cards, map, and search)
- [x] Normalize taxonomy/name display rules across templates and cards
  - Progress (2026-02-24): introduced normalized display fields (`display_name`, `display_scientific`, `display_common`) in generator data shaping.
  - Progress (2026-02-24): updated cards/lists/search/plant templates to use the same display fields consistently.
- [x] Add duplicate-detection review report in import pipeline
  - Progress (2026-02-24): added post-import duplicate analysis in `generator/import_data.py`.
  - Progress (2026-02-24): writes `data/duplicate_review_report.json` with exact duplicate groups and same-family/genus fuzzy candidates.
- [x] Normalize `garden_location` into stable location IDs
  - Progress (2026-02-24): added normalized location tables (`garden_locations`, `plant_garden_locations`) and deterministic keys (`loc-...`) in import pipeline.
  - Progress (2026-02-24): updated build/map/plant flow to use stable location keys for deep links and location indexing.
- [x] Add "changed since last build" diff artifacts
  - Progress (2026-02-24): added snapshot/diff generation in `generator/build_site.py`.
  - Progress (2026-02-24): build now writes `data/build_snapshot.json` and `data/build_diff_report.json` with added/removed/changed plant records.
- [x] Generate versioned API-ready JSON exports
  - Progress (2026-02-24): build now emits `output/static/api/v1/*.json` entity exports and `manifest.json`.
  - Progress (2026-02-24): includes plants/families/genera/collections/locations with build version and counts.
- [x] Add editor-facing content quality queue page
  - Progress (2026-02-24): added generated `quality-queue.html` with per-plant missing-field list and direct links for curation.
  - Progress (2026-02-24): linked quality queue from footer and sitemap.
- [x] Performance pass for larger datasets (search split/gzip/precompute)
  - Progress (2026-02-24): sharded search dataset by prefix (`search-shard-*.json`) with lightweight `search-index.json`.
  - Progress (2026-02-24): updated client search to load only needed shard for current query, with full-data fallback.

## Toxicity Roadmap

- [ ] Add source-level confidence weighting for toxicity evidence
- [ ] Add structured `toxicity_status` fields in DB and write consensus back to `plants`
- [ ] Build toxicity pages in site (`/toxicity`, `/toxicity/toxic`, `/toxicity/possibly-toxic`)
- [ ] Add manual override layer for curator-reviewed toxicity decisions
