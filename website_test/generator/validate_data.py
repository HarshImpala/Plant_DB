"""
Pre-build data validation for Plant Encyclopedia.

Fails with non-zero exit code on critical issues so CI can stop early.
"""

import json
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path


BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "plants.db"
IMAGES_DIR = BASE_DIR / "static" / "images" / "plants"
REPORT_PATH = DATA_DIR / "validation_report.json"


def slugify(text):
    if not text:
        return ""
    text = re.sub(r"\s+\([^)]+\)\s*$", "", text)
    text = re.sub(r"\s+[A-Z][a-z]*\.?\s*$", "", text)
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "-", text)
    return text


def _is_http_url(value):
    if not value:
        return True
    return isinstance(value, str) and value.startswith(("http://", "https://"))


def main():
    if not DB_PATH.exists():
        print(f"ERROR: Database not found: {DB_PATH}")
        return 2

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT id, input_name, canonical_name, scientific_name, wfo_url, gbif_url, wikipedia_url, image_filename "
        "FROM plants"
    )
    rows = cur.fetchall()
    conn.close()

    report = {
        "total_plants": len(rows),
        "critical": [],
        "warnings": [],
        "stats": {},
    }

    # 1) Validate critical name availability.
    missing_name_rows = []
    for row in rows:
        plant_id, input_name, canonical_name, scientific_name = row[:4]
        if not (input_name or canonical_name or scientific_name):
            missing_name_rows.append({"id": plant_id})
    if missing_name_rows:
        report["critical"].append({
            "check": "missing_name_fields",
            "count": len(missing_name_rows),
            "examples": missing_name_rows[:20],
        })

    # 2) Validate URL format (warning-only).
    bad_url_rows = []
    for row in rows:
        plant_id, _, _, _, wfo_url, gbif_url, wikipedia_url, _ = row
        for field, value in (
            ("wfo_url", wfo_url),
            ("gbif_url", gbif_url),
            ("wikipedia_url", wikipedia_url),
        ):
            if value and not _is_http_url(value):
                bad_url_rows.append({"id": plant_id, "field": field, "value": value})
    if bad_url_rows:
        report["warnings"].append({
            "check": "invalid_url_format",
            "count": len(bad_url_rows),
            "examples": bad_url_rows[:20],
        })

    # 3) Validate image references (warning-only).
    missing_image_files = []
    for row in rows:
        plant_id = row[0]
        image_filename = row[7]
        if image_filename:
            image_path = IMAGES_DIR / image_filename
            if not image_path.exists():
                missing_image_files.append({"id": plant_id, "image_filename": image_filename})
    if missing_image_files:
        report["warnings"].append({
            "check": "missing_image_files",
            "count": len(missing_image_files),
            "examples": missing_image_files[:20],
        })

    # 4) Validate final slug uniqueness with build strategy (critical).
    base_counts = defaultdict(int)
    base_values = []
    for row in rows:
        plant_id, input_name, canonical_name, scientific_name = row[:4]
        name = canonical_name or scientific_name or input_name
        base = slugify(name) or f"plant-{plant_id}"
        base_counts[base] += 1
        base_values.append((plant_id, base))

    seen = defaultdict(int)
    final_slugs = defaultdict(list)
    for plant_id, base in base_values:
        if base_counts[base] == 1:
            final = base
        else:
            seen[base] += 1
            final = f"{base}-{seen[base]}"
        final_slugs[final].append(plant_id)

    dupes = [{"slug": slug, "ids": ids} for slug, ids in final_slugs.items() if len(ids) > 1]
    if dupes:
        report["critical"].append({
            "check": "duplicate_final_slugs",
            "count": len(dupes),
            "examples": dupes[:20],
        })

    report["stats"] = {
        "plants_with_images": sum(1 for r in rows if r[7]),
        "plants_with_wikipedia": sum(1 for r in rows if r[6]),
        "critical_checks_failed": len(report["critical"]),
        "warning_checks_failed": len(report["warnings"]),
    }

    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Validation report written: {REPORT_PATH}")
    print(f"Plants: {report['total_plants']}")
    print(f"Critical failures: {len(report['critical'])}")
    print(f"Warnings: {len(report['warnings'])}")

    if report["critical"]:
        for failure in report["critical"]:
            print(f"CRITICAL {failure['check']}: {failure['count']}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

