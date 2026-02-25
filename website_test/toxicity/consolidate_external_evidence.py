import csv
from pathlib import Path

from classify_toxicity import load_plants, classify_toxicity


BASE_DIR = Path(__file__).resolve().parent.parent
TOX_DIR = BASE_DIR / "toxicity"
REVIEW_QUEUE_PATH = TOX_DIR / "review_queue_external_sources.csv"
AUTO_EVIDENCE_PATH = TOX_DIR / "external_evidence_auto.csv"

OUT_ALL = TOX_DIR / "toxicity_consensus_all.csv"
OUT_TOXIC = TOX_DIR / "toxicity_consensus_toxic.csv"
OUT_POSSIBLY = TOX_DIR / "toxicity_consensus_possibly_toxic.csv"

SEVERITY = {
    "not_toxic": 0,
    "unknown": 1,
    "possibly_toxic": 2,
    "toxic": 3,
}


def normalize_status(value):
    v = (value or "").strip().lower()
    if v in SEVERITY:
        return v
    return "unknown"


def pick_higher(a, b):
    return a if SEVERITY[a] >= SEVERITY[b] else b


def combine_statuses(statuses):
    result = "unknown"
    for s in statuses:
        result = pick_higher(result, normalize_status(s))
    return result


def read_external_rows():
    if not REVIEW_QUEUE_PATH.exists():
        return {}
    out = {}
    with open(REVIEW_QUEUE_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            plant_id = int(row["id"])
            out[plant_id] = row
    return out


def read_auto_rows():
    if not AUTO_EVIDENCE_PATH.exists():
        return {}
    grouped = {}
    with open(AUTO_EVIDENCE_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pid = int(row["id"])
            grouped.setdefault(pid, []).append(row)
    return grouped


def write_csv(path, rows):
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main():
    plants = load_plants()
    external = read_external_rows()
    auto = read_auto_rows()
    rows = []

    for plant in plants:
        pid = plant["id"]
        base = classify_toxicity(plant.get("toxicity_info"))
        ext = external.get(pid, {})

        auto_rows = auto.get(pid, [])
        auto_h = [r.get("humans_status", "") for r in auto_rows]
        auto_c = [r.get("cats_status", "") for r in auto_rows]
        auto_d = [r.get("dogs_status", "") for r in auto_rows]

        humans = combine_statuses([base["humans"], ext.get("external_humans_status", ""), *auto_h])
        cats = combine_statuses([base["cats"], ext.get("external_cats_status", ""), *auto_c])
        dogs = combine_statuses([base["dogs"], ext.get("external_dogs_status", ""), *auto_d])
        overall = combine_statuses([base["overall"], humans, cats, dogs])
        direct_signal = (
            base["overall"] in ("toxic", "possibly_toxic")
            or normalize_status(ext.get("external_humans_status", "")) in ("toxic", "possibly_toxic")
            or normalize_status(ext.get("external_cats_status", "")) in ("toxic", "possibly_toxic")
            or normalize_status(ext.get("external_dogs_status", "")) in ("toxic", "possibly_toxic")
            or any(normalize_status(v) in ("toxic", "possibly_toxic") for v in auto_h)
            or any(normalize_status(v) in ("toxic", "possibly_toxic") for v in auto_c)
            or any(normalize_status(v) in ("toxic", "possibly_toxic") for v in auto_d)
        )

        rows.append(
            {
                "id": pid,
                "canonical_name": plant.get("canonical_name") or plant.get("scientific_name") or plant.get("input_name"),
                "scientific_name": plant.get("scientific_name") or "",
                "common_name": plant.get("common_name") or "",
                "family": plant.get("family") or "",
                "base_overall_status": base["overall"],
                "external_humans_status": normalize_status(ext.get("external_humans_status", "")),
                "external_cats_status": normalize_status(ext.get("external_cats_status", "")),
                "external_dogs_status": normalize_status(ext.get("external_dogs_status", "")),
                "auto_sources_count": len(auto_rows),
                "consensus_humans_status": humans,
                "consensus_cats_status": cats,
                "consensus_dogs_status": dogs,
                "consensus_overall_status": overall,
                "direct_evidence_signal": "yes" if direct_signal else "no",
                "family_inference": "",
                "confidence": ext.get("confidence", ""),
                "source_urls_used": ext.get("source_urls_used", ""),
                "evidence_notes": ext.get("evidence_notes", ""),
            }
        )

    # Family-based inference:
    # if a family has >=2 toxic plants, plants with no direct signal in that family
    # get "family known toxic" as a conservative possibly_toxic tag.
    toxic_count_by_family = {}
    for row in rows:
        fam = (row.get("family") or "").strip()
        if not fam:
            continue
        if row["consensus_overall_status"] == "toxic":
            toxic_count_by_family[fam] = toxic_count_by_family.get(fam, 0) + 1

    for row in rows:
        fam = (row.get("family") or "").strip()
        if not fam:
            continue
        if row.get("direct_evidence_signal") != "no":
            continue
        family_toxic_count = toxic_count_by_family.get(fam, 0)
        if family_toxic_count >= 2 and row["consensus_overall_status"] in ("unknown", "not_toxic"):
            row["consensus_overall_status"] = "possibly_toxic"
            row["family_inference"] = "family known toxic"
            notes = (row.get("evidence_notes") or "").strip()
            suffix = f"family known toxic ({family_toxic_count} toxic species in family)"
            row["evidence_notes"] = f"{notes}; {suffix}".strip("; ")

    toxic = [r for r in rows if r["consensus_overall_status"] == "toxic"]
    possibly = [r for r in rows if r["consensus_overall_status"] == "possibly_toxic"]

    write_csv(OUT_ALL, rows)
    write_csv(OUT_TOXIC, toxic)
    write_csv(OUT_POSSIBLY, possibly)

    print(f"Consensus rows: {len(rows)}")
    print(f"Toxic: {len(toxic)}")
    print(f"Possibly toxic: {len(possibly)}")
    print(f"Wrote: {OUT_ALL}")


if __name__ == "__main__":
    main()
