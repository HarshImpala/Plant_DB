import csv
import html
import re
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
TOX_DIR = BASE_DIR / "toxicity"
QUEUE_PATH = TOX_DIR / "review_queue_external_sources.csv"
OUT_PATH = TOX_DIR / "external_evidence_auto.csv"


SOURCE_QUERIES = [
    ("aspca", "aspca.org"),
    ("petpoison", "petpoisonhelpline.com"),
    ("ucdavis_vetmed", "vetmed.ucdavis.edu"),
    ("poison_control", "poison.org"),
    ("nc_state_plants", "plants.ces.ncsu.edu"),
    ("merck_vet", "merckvetmanual.com"),
]


def normalize(text):
    return (text or "").strip().lower()


def fetch_url(url, timeout=20):
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; PlantToxicityBot/1.0)"
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def strip_html(raw_html):
    text = raw_html.decode("utf-8", errors="ignore")
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def bing_rss_search(query, limit=3):
    url = "https://www.bing.com/search?format=rss&q=" + urllib.parse.quote_plus(query)
    raw = fetch_url(url)
    root = ET.fromstring(raw)
    out = []
    for item in root.findall("./channel/item"):
        link = item.findtext("link") or ""
        title = item.findtext("title") or ""
        desc = item.findtext("description") or ""
        out.append({"link": link, "title": title, "description": desc})
        if len(out) >= limit:
            break
    return out


def classify_species_toxicity(text, species):
    t = normalize(text)
    toxic_patterns = {
        "humans": [r"toxic to humans?", r"poisonous to humans?", r"human toxicity"],
        "cats": [r"toxic to cats?", r"cat toxicity", r"poisonous to cats?"],
        "dogs": [r"toxic to dogs?", r"dog toxicity", r"poisonous to dogs?"],
    }
    not_toxic_patterns = {
        "humans": [r"not toxic to humans?", r"non[- ]toxic to humans?"],
        "cats": [r"not toxic to cats?", r"non[- ]toxic to cats?"],
        "dogs": [r"not toxic to dogs?", r"non[- ]toxic to dogs?"],
    }

    if any(re.search(p, t) for p in not_toxic_patterns[species]):
        return "not_toxic"
    if any(re.search(p, t) for p in toxic_patterns[species]):
        return "toxic"

    if species in ("cats", "dogs") and ("pets" in t or "pet" in t):
        if "non-toxic" in t or "not toxic" in t:
            return "not_toxic"
        if "toxic" in t or "poisonous" in t:
            return "possibly_toxic"
    if species == "humans" and ("toxic" in t or "poison" in t):
        return "possibly_toxic"
    return "unknown"


def infer_statuses(page_text):
    return {
        "humans_status": classify_species_toxicity(page_text, "humans"),
        "cats_status": classify_species_toxicity(page_text, "cats"),
        "dogs_status": classify_species_toxicity(page_text, "dogs"),
    }


def choose_best_result(results, domain):
    for r in results:
        if domain in (r.get("link") or ""):
            return r
    return results[0] if results else None


def main():
    if not QUEUE_PATH.exists():
        raise SystemExit(f"Missing queue file: {QUEUE_PATH}")

    rows = []
    with open(QUEUE_PATH, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    out_rows = []
    for idx, row in enumerate(rows, start=1):
        plant_id = row["id"]
        plant_name = (row.get("scientific_name") or row.get("canonical_name") or "").strip()
        if not plant_name:
            continue

        for source_name, domain in SOURCE_QUERIES:
            query = f'site:{domain} "{plant_name}" toxicity'
            try:
                results = bing_rss_search(query, limit=3)
                best = choose_best_result(results, domain)
                if not best:
                    continue
                page_url = best["link"]
                page_raw = fetch_url(page_url, timeout=20)
                page_text = strip_html(page_raw)[:60000]
                statuses = infer_statuses(page_text)
                excerpt = page_text[:500]
                out_rows.append(
                    {
                        "id": plant_id,
                        "canonical_name": row.get("canonical_name", ""),
                        "scientific_name": row.get("scientific_name", ""),
                        "source": source_name,
                        "domain": domain,
                        "query": query,
                        "matched_url": page_url,
                        "humans_status": statuses["humans_status"],
                        "cats_status": statuses["cats_status"],
                        "dogs_status": statuses["dogs_status"],
                        "confidence": "low_auto",
                        "evidence_excerpt": excerpt,
                    }
                )
            except Exception as exc:
                out_rows.append(
                    {
                        "id": plant_id,
                        "canonical_name": row.get("canonical_name", ""),
                        "scientific_name": row.get("scientific_name", ""),
                        "source": source_name,
                        "domain": domain,
                        "query": query,
                        "matched_url": "",
                        "humans_status": "unknown",
                        "cats_status": "unknown",
                        "dogs_status": "unknown",
                        "confidence": "error",
                        "evidence_excerpt": f"fetch_error: {exc}",
                    }
                )
            time.sleep(0.25)

        if idx % 25 == 0:
            print(f"Processed {idx}/{len(rows)} plants...")

    fieldnames = [
        "id",
        "canonical_name",
        "scientific_name",
        "source",
        "domain",
        "query",
        "matched_url",
        "humans_status",
        "cats_status",
        "dogs_status",
        "confidence",
        "evidence_excerpt",
    ]
    with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"Wrote auto evidence: {OUT_PATH}")
    print(f"Rows: {len(out_rows)}")


if __name__ == "__main__":
    main()
