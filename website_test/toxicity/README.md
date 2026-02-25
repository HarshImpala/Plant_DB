# Toxicity Workflow

This folder supports toxicity triage and external-source evidence consolidation.

## 1) Base triage from existing DB notes

Run:

```powershell
..\..\venv\Scripts\python.exe toxicity/classify_toxicity.py
```

Outputs:

- `toxicity_all_classified.csv`
- `toxicity_toxic.csv`
- `toxicity_possibly_toxic.csv`
- `toxicity_unknown.csv`

## 2) Build multi-source review queue

Run:

```powershell
..\..\venv\Scripts\python.exe toxicity/build_external_review_queue.py
```

Output:

- `review_queue_external_sources.csv`

This file includes one row per plant, search links for multiple external sources, and blank columns for:

- `external_humans_status`
- `external_cats_status`
- `external_dogs_status`
- `confidence`
- `source_urls_used`
- `evidence_notes`

Allowed status values: `toxic`, `possibly_toxic`, `unknown`, `not_toxic`.

## 3) Optional: auto-enrich from 2 external domains

This attempts automated evidence pull using Bing RSS site-search + page keyword scan for:

- `aspca.org`
- `petpoisonhelpline.com`
- `vetmed.ucdavis.edu`
- `poison.org`
- `plants.ces.ncsu.edu`
- `merckvetmanual.com`

Run:

```powershell
..\..\venv\Scripts\python.exe toxicity/auto_enrich_external_evidence.py
```

Output:

- `external_evidence_auto.csv`

Notes:

- This is heuristic and lower confidence (`confidence = low_auto`).
- Keep manual review as source of truth.

## 4) Consolidate consensus

After filling manual evidence (and optionally generating auto evidence), run:

```powershell
..\..\venv\Scripts\python.exe toxicity/consolidate_external_evidence.py
```

Outputs:

- `toxicity_consensus_all.csv`
- `toxicity_consensus_toxic.csv`
- `toxicity_consensus_possibly_toxic.csv`

Consensus is conservative:

`toxic` > `possibly_toxic` > `unknown` > `not_toxic`

Family inference rule:

- If a plant has no direct toxicity signal and belongs to a family with at least 2 distinct toxic plants,
  it is marked `possibly_toxic` with `family_inference = family known toxic`.

## External references

See `external_sources.md` for source shortlist (ASPCA, Pet Poison Helpline, Merck Vet Manual, FDA Plantox, NC State, etc.).
