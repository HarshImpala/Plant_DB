import pandas as pd
import requests
import time
import json
import re
from pathlib import Path

GBIF_MATCH_URL = "https://api.gbif.org/v1/species/match"
GBIF_SPECIES_URL = "https://api.gbif.org/v1/species"

HEADERS = {
    "User-Agent": "plant-toxicity-check/1.0 (aron_serebrenik@yahoo.com)"
}

# --------- caches ----------
MATCH_CACHE_PATH = Path("../gbif_match_cache.json")
SYN_CACHE_PATH = Path("../gbif_syn_cache.json")
VERN_CACHE_PATH = Path("../gbif_vern_cache.json")
SPECIES_CACHE_PATH = Path("../gbif_species_cache.json")

# Bump this if you want to invalidate old cached vernacular data automatically
VERN_CACHE_VERSION = "v3_preferred"

# --------- progress reporting ----------
PROGRESS_EVERY_N = 10  # print every N plants

# --------- helpers ----------
def load_cache(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}

def save_cache(path: Path, cache: dict) -> None:
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", str(s)).strip()

def key_genus(name: str | None) -> str:
    if not name or pd.isna(name):
        return ""
    name = str(name).strip().lower()
    name = name.replace("×", "x")
    name = re.sub(r"[(),.;\[\]]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name.split()[0] if name else ""

def species_epithet(name: str | None) -> str:
    """
    Returns the species epithet (2nd token) if present.
    Example: 'Vachellia collinsii (Saff.) ...' -> 'collinsii'
    """
    if not name or pd.isna(name):
        return ""
    s = normalize_spaces(str(name)).replace("×", "x")
    s = re.sub(r"[(),.;\[\]]", " ", s)
    parts = re.sub(r"\s+", " ", s).strip().split()
    return parts[1].lower() if len(parts) >= 2 else ""

def to_genus_species(canonical: str | None) -> str | None:
    if not canonical or pd.isna(canonical):
        return None
    parts = str(canonical).strip().split()
    if len(parts) >= 2:
        return f"{parts[0]} {parts[1]}"
    return str(canonical).strip()

def looks_mostly_ascii(s: str, min_ratio: float = 0.90) -> bool:
    if not s:
        return False
    ascii_count = sum(1 for ch in s if ord(ch) < 128)
    return (ascii_count / max(1, len(s))) >= min_ratio

def fix_mojibake(s: str) -> str:
    """
    Best-effort repair for common UTF-8 -> Latin-1/CP1252 mojibake.
    """
    if not s:
        return s
    if any(x in s for x in ["Ã", "â", "�"]):
        for enc in ("latin1", "cp1252"):
            try:
                repaired = s.encode(enc).decode("utf-8")
                if sum(repaired.count(x) for x in ["Ã", "â", "�"]) < sum(s.count(x) for x in ["Ã", "â", "�"]):
                    return repaired
            except Exception:
                pass
    return s

def dedupe_casefold(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for s in items:
        k = s.casefold()
        if k not in seen:
            seen.add(k)
            out.append(s)
    return out

# --------- GBIF calls ----------
def gbif_match_cached(name: str, cache: dict, kingdom: str = "Plantae", sleep_s: float = 0.1) -> dict:
    key = f"{kingdom}||{name}".strip()
    if key in cache:
        return cache[key]

    params = {"name": name, "kingdom": kingdom}
    r = requests.get(GBIF_MATCH_URL, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()

    cache[key] = data
    time.sleep(sleep_s)
    return data

def gbif_species_cached(usage_key, cache: dict, sleep_s: float = 0.1) -> dict:
    """
    GET /species/{usageKey} (cached). Used to resolve synonyms to accepted names.
    """
    if usage_key is None or pd.isna(usage_key):
        return {}
    try:
        uk = int(usage_key)
    except Exception:
        return {}

    k = str(uk)
    if k in cache:
        return cache[k]

    url = f"{GBIF_SPECIES_URL}/{uk}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()

    cache[k] = data
    time.sleep(sleep_s)
    return data

def resolve_highest_accepted_usage_key(
    initial_usage_key,
    species_cache: dict,
    sleep_s: float = 0.1,
    max_hops: int = 10
):
    """
    Starting from a GBIF usageKey, climb to the highest accepted taxon.
    Returns: (accepted_usage_key, accepted_species_record, hop_count)

    We follow acceptedKey / acceptedUsageKey while the record is synonym-like.
    """
    if initial_usage_key is None or pd.isna(initial_usage_key):
        return (None, {}, 0)

    try:
        current = int(initial_usage_key)
    except Exception:
        return (None, {}, 0)

    hop = 0
    visited = set()

    SYN_LIKE = {
        "SYNONYM",
        "HOMOTYPIC_SYNONYM",
        "HETEROTYPIC_SYNONYM",
        "MISAPPLIED",
        "PROPARTE_SYNONYM",
    }

    while hop < max_hops and current not in visited:
        visited.add(current)
        rec = gbif_species_cached(current, cache=species_cache, sleep_s=sleep_s) or {}

        status = (rec.get("taxonomicStatus") or rec.get("status") or "").strip().upper()

        next_key = rec.get("acceptedKey")
        if next_key is None:
            next_key = rec.get("acceptedUsageKey")

        # Stop if not synonym-like OR no pointer to an accepted record
        if status and status not in SYN_LIKE:
            return (current, rec, hop)
        if not next_key:
            return (current, rec, hop)

        try:
            current = int(next_key)
        except Exception:
            return (current, rec, hop)

        hop += 1

    rec = gbif_species_cached(current, cache=species_cache, sleep_s=sleep_s) or {}
    return (current, rec, hop)

def gbif_synonyms_all_cached(
    usage_key,
    cache: dict,
    sleep_s: float = 0.1,
    page_limit: int = 300
) -> list[str]:
    """
    Fetch ALL pages from /species/{usageKey}/synonyms (GBIF is paginated).
    Cached as a single flattened list of names.
    """
    if usage_key is None or pd.isna(usage_key):
        return []
    try:
        uk = int(usage_key)
    except Exception:
        return []

    k = str(uk)
    if k in cache:
        return cache[k]

    url = f"{GBIF_SPECIES_URL}/{uk}/synonyms"
    offset = 0
    out: list[str] = []

    while True:
        params = {"limit": page_limit, "offset": offset}
        r = requests.get(url, params=params, headers=HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()

        results = data.get("results", []) or []
        for item in results:
            nm = item.get("scientificName") or item.get("canonicalName")
            if nm:
                out.append(str(nm))

        end_of_records = bool(data.get("endOfRecords"))
        count = data.get("count")
        offset += len(results)

        if end_of_records:
            break
        if not results:
            break
        if count is not None and offset >= int(count):
            break

        time.sleep(sleep_s)

    out = dedupe_casefold(out)
    cache[k] = out
    time.sleep(sleep_s)
    return out

# --------- vernacular names: choose GBIF "preferred" English where possible ----------
EN_LANGS = {"en", "eng", "english"}

def gbif_english_vernaculars_cached(usage_key, cache: dict, sleep_s: float = 0.1) -> list[dict]:
    """
    Returns a list of dicts:
      [{"name": "...", "lang": "...", "preferred": bool}, ...]
    Cached with versioning.
    """
    if usage_key is None or pd.isna(usage_key):
        return []
    try:
        uk = int(usage_key)
    except Exception:
        return []

    cache_key = f"{VERN_CACHE_VERSION}||{uk}"
    if cache_key in cache:
        return cache[cache_key]

    url = f"{GBIF_SPECIES_URL}/{uk}/vernacularNames"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()

    out: list[dict] = []
    for item in data.get("results", []):
        vname = item.get("vernacularName")
        if not vname:
            continue

        raw = normalize_spaces(vname)
        cleaned = normalize_spaces(fix_mojibake(raw))

        lang = (item.get("language") or item.get("languageCode") or item.get("lang") or "")
        lang = str(lang).strip().lower()

        preferred = item.get("preferred")
        if preferred is None:
            preferred = item.get("isPreferred")
        preferred = bool(preferred) if preferred is not None else False

        # Filter to English
        if lang:
            if lang not in EN_LANGS:
                continue
            if cleaned:
                out.append({"name": cleaned, "lang": lang, "preferred": preferred})
            continue

        # If language missing, keep only mostly-ascii (to avoid non-English slipping in)
        if cleaned and looks_mostly_ascii(cleaned, min_ratio=0.90):
            out.append({"name": cleaned, "lang": "", "preferred": False})

    # De-dupe (case-insensitive), keep first occurrence
    seen = set()
    deduped = []
    for d in out:
        kk = d["name"].casefold()
        if kk not in seen:
            seen.add(kk)
            deduped.append(d)

    cache[cache_key] = deduped
    time.sleep(sleep_s)
    return deduped

def pick_primary_english_name_from_vernaculars(vernaculars: list[dict]) -> str:
    """
    Prefer GBIF 'preferred' English name first.
    Fallback: shortest reasonable ASCII-ish name.
    """
    if not vernaculars:
        return ""

    preferred = [v["name"] for v in vernaculars if v.get("preferred") is True]
    if preferred:
        return sorted(preferred, key=lambda x: (len(x), x.lower()))[0]

    english_coded = [v["name"] for v in vernaculars if v.get("lang") in EN_LANGS]
    pool = english_coded if english_coded else [v["name"] for v in vernaculars]

    pool2 = [n for n in pool if len(n) >= 4] or pool
    return sorted(pool2, key=lambda x: (len(x), x.lower()))[0]

# --------- load your plants ----------
plants = pd.read_excel(
    r"C:\Users\aron_\PycharmProjects\obsidian_app\PostgreSQL_DB\new_scripts_WFO_main_source\excel_files\tropical_test\tropusi_haszon_test.xlsx"
)
plants.columns = plants.columns.str.strip()

# --------- run matching + resolve accepted + synonyms + English names ----------
match_cache   = load_cache(MATCH_CACHE_PATH)
syn_cache     = load_cache(SYN_CACHE_PATH)
vern_cache    = load_cache(VERN_CACHE_PATH)
species_cache = load_cache(SPECIES_CACHE_PATH)

gbif_rows = []
total = len(plants)

for idx, nm in enumerate(plants["Latin name"].astype(str), start=1):
    res_match = gbif_match_cached(nm, cache=match_cache, kingdom="Plantae")
    matched_usage_key = res_match.get("usageKey")

    accepted_usage_key, accepted_rec, hop_count = resolve_highest_accepted_usage_key(
        matched_usage_key, species_cache=species_cache, sleep_s=0.1, max_hops=10
    )

    # --------- progress output ----------
    if idx == 1 or idx % PROGRESS_EVERY_N == 0 or idx == total:
        pct = (idx / total) * 100
        print(f"[{idx}/{total} | {pct:6.2f}%] Processing: {nm} | accepted hops: {hop_count}")

    # Use accepted record as the main target going forward (fallbacks to match if missing)
    canonical = accepted_rec.get("canonicalName") or res_match.get("canonicalName")
    scientific_name = accepted_rec.get("scientificName") or res_match.get("scientificName")
    tax_status = (accepted_rec.get("taxonomicStatus") or accepted_rec.get("status") or "").strip()

    # For synonym subset selection
    target_genus = key_genus(canonical)
    accepted_ep = species_epithet(canonical)
    matched_ep = species_epithet(res_match.get("canonicalName") or res_match.get("scientificName"))
    ep_target = accepted_ep or matched_ep

    usage_key_for_rest = accepted_usage_key if accepted_usage_key is not None else matched_usage_key

    # --- Synonyms: fetch ALL pages from ACCEPTED key ---
    synonyms = gbif_synonyms_all_cached(usage_key_for_rest, cache=syn_cache)

    # Optional: include the matched scientific name for auditing (if input was a synonym)
    matched_scientific = res_match.get("scientificName")
    if matched_scientific:
        synonyms = dedupe_casefold([matched_scientific] + synonyms)

    # Keep ALL synonyms (no genus guard)
    all_synonyms = synonyms

    # Useful subsets:
    synonyms_same_genus = [s for s in all_synonyms if key_genus(s) == target_genus]
    synonyms_same_species = [s for s in all_synonyms if species_epithet(s) == ep_target] if ep_target else []

    # Vernaculars (English) — from ACCEPTED key
    vernaculars = gbif_english_vernaculars_cached(usage_key_for_rest, cache=vern_cache)
    primary_english = pick_primary_english_name_from_vernaculars(vernaculars)
    all_english_names = [v["name"] for v in vernaculars]

    gbif_rows.append({
        "input_name": nm,

        # original GBIF match info
        "gbif_matchType": res_match.get("matchType"),
        "gbif_confidence": res_match.get("confidence"),
        "gbif_matched_scientificName": res_match.get("scientificName"),
        "gbif_matched_canonicalName": res_match.get("canonicalName"),
        "gbif_matched_usageKey": matched_usage_key,

        # accepted-resolution info
        "gbif_accepted_usageKey": usage_key_for_rest,
        "gbif_accepted_taxonomicStatus": tax_status,
        "gbif_accepted_hops": hop_count,

        # main (accepted) name fields used downstream
        "gbif_canonicalName": canonical,
        "gbif_genus_species": to_genus_species(canonical),
        "gbif_scientificName": scientific_name,

        # synonyms (all + subsets)
        "gbif_synonyms": " | ".join(all_synonyms),
        "gbif_synonym_count": len(all_synonyms),

        "gbif_synonyms_same_species": " | ".join(synonyms_same_species),
        "gbif_synonyms_same_species_count": len(synonyms_same_species),

        "gbif_synonyms_same_genus": " | ".join(synonyms_same_genus),
        "gbif_synonyms_same_genus_count": len(synonyms_same_genus),

        # english/common names
        "gbif_english_name": primary_english,
        "gbif_english_names": " | ".join(all_english_names),
        "gbif_english_name_count": len(all_english_names),
    })

# Save caches
save_cache(MATCH_CACHE_PATH, match_cache)
save_cache(SYN_CACHE_PATH, syn_cache)
save_cache(VERN_CACHE_PATH, vern_cache)
save_cache(SPECIES_CACHE_PATH, species_cache)

gbif_df = pd.DataFrame(gbif_rows)

# Excel-friendly outputs
gbif_df.to_csv("plants_gbif_matched.csv", index=False, encoding="utf-8-sig")
gbif_df.to_excel("plants_gbif_matched.xlsx", index=False)

print(gbif_df.head())
print("Wrote plants_gbif_matched.csv (utf-8-sig) and plants_gbif_matched.xlsx")
print(f"Done. Processed {total} plants.")
