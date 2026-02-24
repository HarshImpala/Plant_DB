# Website Test - Tasks

## Priority Tasks

- [x] Fix slug collisions in generated URLs/pages (deterministic unique slug strategy in `generator/build_site.py`)
  - Progress (2026-02-24): added deterministic duplicate handling where collisions are emitted as `slug-1..slug-N` instead of overwriting pages.
  - Progress (2026-02-24): rebuilt static output so duplicate canonical names now produce distinct `output/plant/*.html` files.
- [x] Make import deterministic and non-destructive (incremental mode + explicit full rebuild in `generator/import_data.py`)
  - Progress (2026-02-24): switched plant writes to SQLite upsert (`ON CONFLICT(input_name) DO UPDATE`) to preserve stable IDs.
  - Progress (2026-02-24): added explicit `--full-rebuild` destructive mode; default run now preserves DB and updates in place.
  - Progress (2026-02-24): relationship/location child records are replaced per plant during import to avoid stale duplicates.
- [ ] Add a pre-build validation step (duplicate slugs, missing critical fields, broken links) and fail CI on critical issues
- [ ] Improve Wikipedia matching quality (disambiguation checks + fallback query strategy in `generator/fetch_wikipedia_urls.py`)
- [ ] Optimize build performance by removing N+1 DB query patterns in `generator/build_site.py`

## UX and Discovery

- [ ] Add faceted browsing/filtering (family, genus, native region, toxicity, has image/description)
- [ ] Add structured metadata (JSON-LD) for plant pages in `templates/plant.html`
- [ ] Integrate `garden_location` with map/plant pages for "find in garden" workflows

## Do Later

- [ ] Expand stats page into data quality dashboard (coverage + completeness metrics)

## Platform and Delivery

- [ ] Refine GitHub Pages workflow trigger strategy (build from main, deploy artifact) in `.github/workflows/deploy.yml`
