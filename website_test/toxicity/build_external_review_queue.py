import csv
import sqlite3
from pathlib import Path
from urllib.parse import quote_plus


BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "plants.db"
OUT_DIR = BASE_DIR / "toxicity"
OUT_PATH = OUT_DIR / "review_queue_external_sources.csv"


def load_plants():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, input_name, canonical_name, scientific_name, common_name, toxicity_info
        FROM plants
        ORDER BY canonical_name, scientific_name
        """
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def search_url(base, query):
    return f"{base}{quote_plus(query)}"


def build_row(plant):
    canonical = plant.get("canonical_name") or plant.get("scientific_name") or plant.get("input_name") or ""
    scientific = plant.get("scientific_name") or canonical
    query = scientific.strip() or canonical.strip()
    return {
        "id": plant["id"],
        "canonical_name": canonical,
        "scientific_name": scientific,
        "common_name": plant.get("common_name") or "",
        "toxicity_info_current": plant.get("toxicity_info") or "",
        "aspca_search": search_url(
            "https://www.google.com/search?q=site%3Aaspca.org+toxic+non-toxic+plants+",
            query,
        ),
        "petpoison_search": search_url(
            "https://www.google.com/search?q=site%3Apetpoisonhelpline.com+plant+toxicity+",
            query,
        ),
        "merckvet_search": search_url(
            "https://www.google.com/search?q=site%3Amerckvetmanual.com+plant+poisoning+",
            query,
        ),
        "fda_plantox_search": search_url(
            "https://www.google.com/search?q=site%3Aaccessdata.fda.gov+plantox+",
            query,
        ),
        "nc_state_search": search_url(
            "https://www.google.com/search?q=site%3Aplants.ces.ncsu.edu+",
            query,
        ),
        "poisonorg_search": search_url(
            "https://www.google.com/search?q=site%3Apoison.org+plant+",
            query,
        ),
        "external_humans_status": "",
        "external_cats_status": "",
        "external_dogs_status": "",
        "confidence": "",
        "source_urls_used": "",
        "evidence_notes": "",
        "reviewed_by": "",
        "reviewed_at": "",
    }


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    plants = load_plants()
    rows = [build_row(p) for p in plants]

    with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote review queue: {OUT_PATH}")
    print(f"Rows: {len(rows)}")


if __name__ == "__main__":
    main()
