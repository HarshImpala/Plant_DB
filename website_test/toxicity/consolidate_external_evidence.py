import csv
import re
import sqlite3
from pathlib import Path
from urllib.parse import urlparse

from classify_toxicity import load_plants, classify_toxicity


BASE_DIR = Path(__file__).resolve().parent.parent
TOX_DIR = BASE_DIR / "toxicity"
DB_PATH = BASE_DIR / "data" / "plants.db"
REVIEW_QUEUE_PATH = TOX_DIR / "review_queue_external_sources.csv"
AUTO_EVIDENCE_PATH = TOX_DIR / "external_evidence_auto.csv"
OVERRIDES_PATH = TOX_DIR / "manual_toxicity_overrides.csv"

OUT_ALL = TOX_DIR / "toxicity_consensus_all.csv"
OUT_TOXIC = TOX_DIR / "toxicity_consensus_toxic.csv"
OUT_POSSIBLY = TOX_DIR / "toxicity_consensus_possibly_toxic.csv"

SEVERITY = {
    "not_toxic": 0,
    "unknown": 1,
    "possibly_toxic": 2,
    "toxic": 3,
}

SOURCE_WEIGHT = {
    "aspca": 1.0,
    "petpoison": 0.95,
    "poison_control": 0.9,
    "fda_plantox": 0.9,
    "merck_vet": 0.85,
    "ucdavis_vetmed": 0.85,
    "nc_state_plants": 0.8,
    "rhs": 0.75,
    "manual_external": 0.8,
    "internal_note": 0.45,
}

DOMAIN_WEIGHT = {
    "aspca.org": SOURCE_WEIGHT["aspca"],
    "petpoisonhelpline.com": SOURCE_WEIGHT["petpoison"],
    "poison.org": SOURCE_WEIGHT["poison_control"],
    "accessdata.fda.gov": SOURCE_WEIGHT["fda_plantox"],
    "merckvetmanual.com": SOURCE_WEIGHT["merck_vet"],
    "vetmed.ucdavis.edu": SOURCE_WEIGHT["ucdavis_vetmed"],
    "plants.ces.ncsu.edu": SOURCE_WEIGHT["nc_state_plants"],
    "rhs.org.uk": SOURCE_WEIGHT["rhs"],
}

CONFIDENCE_MULTIPLIER = {
    "high": 1.0,
    "medium": 0.8,
    "low": 0.6,
    "low_auto": 0.45,
    "error": 0.0,
    "": 0.75,
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


def split_urls(text):
    raw = (text or "").strip()
    if not raw:
        return []
    parts = re.split(r"[|;\n,]+", raw)
    out = []
    for p in parts:
        u = p.strip()
        if u:
            out.append(u)
    return out


def domain_weight(url):
    try:
        host = (urlparse(url).netloc or "").lower()
    except ValueError:
        return SOURCE_WEIGHT["manual_external"]
    if host.startswith("www."):
        host = host[4:]
    for domain, weight in DOMAIN_WEIGHT.items():
        if host == domain or host.endswith("." + domain):
            return weight
    return SOURCE_WEIGHT["manual_external"]


def confidence_multiplier(value):
    return CONFIDENCE_MULTIPLIER.get((value or "").strip().lower(), 0.7)


def weighted_status(evidences):
    # Weighted vote among status labels; tie-break by conservative severity.
    scores = {k: 0.0 for k in SEVERITY}
    for ev in evidences:
        status = normalize_status(ev.get("status", "unknown"))
        weight = float(ev.get("weight", 0.0))
        if weight <= 0:
            continue
        scores[status] += weight

    best = "unknown"
    best_score = -1.0
    for status, score in scores.items():
        if score > best_score:
            best = status
            best_score = score
        elif score == best_score and SEVERITY[status] > SEVERITY[best]:
            best = status
    return best, best_score


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


def read_override_rows():
    if not OVERRIDES_PATH.exists():
        return {}, {}
    by_id = {}
    by_name = {}
    with open(OVERRIDES_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rid = (row.get("id") or "").strip()
            canonical = (row.get("canonical_name") or "").strip().lower()
            if rid.isdigit():
                by_id[int(rid)] = row
            if canonical:
                by_name[canonical] = row
    return by_id, by_name


def write_csv(path, rows):
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def ensure_toxicity_columns(conn):
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(plants)")
    columns = {row[1] for row in cur.fetchall()}
    required = [
        ("toxicity_status_overall", "TEXT"),
        ("toxicity_status_humans", "TEXT"),
        ("toxicity_status_cats", "TEXT"),
        ("toxicity_status_dogs", "TEXT"),
        ("toxicity_status_family_inference", "TEXT"),
        ("toxicity_status_confidence", "REAL"),
        ("toxicity_status_source", "TEXT"),
        ("toxicity_status_updated_at", "TIMESTAMP"),
    ]
    for name, ddl in required:
        if name not in columns:
            cur.execute(f"ALTER TABLE plants ADD COLUMN {name} {ddl}")
    conn.commit()


def write_consensus_to_db(rows):
    conn = sqlite3.connect(DB_PATH)
    ensure_toxicity_columns(conn)
    cur = conn.cursor()
    for row in rows:
        confidence = row.get("weighted_overall_score")
        try:
            confidence_value = float(confidence) if confidence not in (None, "") else None
        except ValueError:
            confidence_value = None
        cur.execute(
            """
            UPDATE plants
            SET
                toxicity_status_overall = ?,
                toxicity_status_humans = ?,
                toxicity_status_cats = ?,
                toxicity_status_dogs = ?,
                toxicity_status_family_inference = ?,
                toxicity_status_confidence = ?,
                toxicity_status_source = ?,
                toxicity_status_updated_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                row.get("consensus_overall_status"),
                row.get("weighted_humans_status"),
                row.get("weighted_cats_status"),
                row.get("weighted_dogs_status"),
                row.get("family_inference") or "",
                confidence_value,
                "consensus_external_weighted",
                int(row["id"]),
            ),
        )
    conn.commit()
    conn.close()


def apply_manual_override(row, override):
    if not override:
        return row, False
    changed = False

    def apply_status(field_name, override_field):
        nonlocal changed
        value = normalize_status(override.get(override_field, ""))
        if value != "unknown" or (override.get(override_field, "").strip().lower() == "unknown"):
            row[field_name] = value
            consensus_field = field_name.replace("weighted_", "consensus_")
            if consensus_field in row:
                row[consensus_field] = value
            changed = True

    apply_status("weighted_humans_status", "override_humans_status")
    apply_status("weighted_cats_status", "override_cats_status")
    apply_status("weighted_dogs_status", "override_dogs_status")
    apply_status("weighted_overall_status", "override_overall_status")

    family_inference = (override.get("override_family_inference") or "").strip()
    if family_inference:
        row["family_inference"] = family_inference
        changed = True

    confidence_value = (override.get("override_confidence") or "").strip()
    if confidence_value:
        row["weighted_overall_score"] = confidence_value
        changed = True

    notes = (override.get("override_notes") or "").strip()
    if notes:
        existing = (row.get("evidence_notes") or "").strip()
        row["evidence_notes"] = f"{existing}; override: {notes}".strip("; ")
        changed = True

    if changed:
        row["override_applied"] = "yes"
        row["toxicity_status_source"] = "manual_override"
    return row, changed


def main():
    plants = load_plants()
    external = read_external_rows()
    auto = read_auto_rows()
    overrides_by_id, overrides_by_name = read_override_rows()
    rows = []

    for plant in plants:
        pid = plant["id"]
        base = classify_toxicity(plant.get("toxicity_info"))
        ext = external.get(pid, {})
        ext_conf_mult = confidence_multiplier(ext.get("confidence", ""))

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

        evidence_urls = split_urls(ext.get("source_urls_used", ""))
        if evidence_urls:
            manual_external_weight = sum(domain_weight(u) for u in evidence_urls) / len(evidence_urls)
        else:
            manual_external_weight = SOURCE_WEIGHT["manual_external"]
        manual_external_weight *= ext_conf_mult

        ev_h = [{"status": base["humans"], "weight": SOURCE_WEIGHT["internal_note"]}]
        ev_c = [{"status": base["cats"], "weight": SOURCE_WEIGHT["internal_note"]}]
        ev_d = [{"status": base["dogs"], "weight": SOURCE_WEIGHT["internal_note"]}]

        ev_h.append({"status": ext.get("external_humans_status", ""), "weight": manual_external_weight})
        ev_c.append({"status": ext.get("external_cats_status", ""), "weight": manual_external_weight})
        ev_d.append({"status": ext.get("external_dogs_status", ""), "weight": manual_external_weight})

        for auto_row in auto_rows:
            auto_weight = SOURCE_WEIGHT.get(auto_row.get("source", ""), 0.7) * confidence_multiplier(auto_row.get("confidence", ""))
            ev_h.append({"status": auto_row.get("humans_status", ""), "weight": auto_weight})
            ev_c.append({"status": auto_row.get("cats_status", ""), "weight": auto_weight})
            ev_d.append({"status": auto_row.get("dogs_status", ""), "weight": auto_weight})

        weighted_humans, weighted_humans_score = weighted_status(ev_h)
        weighted_cats, weighted_cats_score = weighted_status(ev_c)
        weighted_dogs, weighted_dogs_score = weighted_status(ev_d)
        weighted_overall, weighted_overall_score = weighted_status([
            {"status": weighted_humans, "weight": weighted_humans_score},
            {"status": weighted_cats, "weight": weighted_cats_score},
            {"status": weighted_dogs, "weight": weighted_dogs_score},
        ])

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
                "weighted_humans_status": weighted_humans,
                "weighted_cats_status": weighted_cats,
                "weighted_dogs_status": weighted_dogs,
                "weighted_overall_status": weighted_overall,
                "weighted_overall_score": f"{weighted_overall_score:.3f}",
                "direct_evidence_signal": "yes" if direct_signal else "no",
                "family_inference": "",
                "override_applied": "no",
                "toxicity_status_source": "consensus_external_weighted",
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

    # Manual override layer (curator-reviewed)
    override_count = 0
    for row in rows:
        pid = int(row["id"])
        canonical = (row.get("canonical_name") or "").strip().lower()
        override = overrides_by_id.get(pid) or overrides_by_name.get(canonical)
        _, changed = apply_manual_override(row, override)
        if changed:
            override_count += 1

    toxic = [r for r in rows if r["consensus_overall_status"] == "toxic"]
    possibly = [r for r in rows if r["consensus_overall_status"] == "possibly_toxic"]

    write_csv(OUT_ALL, rows)
    write_csv(OUT_TOXIC, toxic)
    write_csv(OUT_POSSIBLY, possibly)
    write_consensus_to_db(rows)

    print(f"Consensus rows: {len(rows)}")
    print(f"Toxic: {len(toxic)}")
    print(f"Possibly toxic: {len(possibly)}")
    print(f"Manual overrides applied: {override_count}")
    print(f"Wrote: {OUT_ALL}")
    print(f"Updated plants toxicity_status_* fields in DB: {DB_PATH}")


if __name__ == "__main__":
    main()
