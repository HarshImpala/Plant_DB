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

# Bump this if you want to invalidate old cached vernacular data automatically
VERN_CACHE_VERSION = "v3_preferred"

# --------- helpers ----------
def load_cache(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}

def save_cache(path: Path, cache: dict) -> None:
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

def key_genus(name: str | None) -> str:
    if not name or pd.isna(name):
        return ""
    name = str(name).strip().lower()
    name = name.replace("×", "x")
    name = re.sub(r"[(),.;\[\]]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name.split()[0] if name else ""

def to_genus_species(canonical: str | None) -> str | None:
    if not canonical or pd.isna(canonical):
        return None
    parts = str(canonical).strip().split()
    if len(parts) >= 2:
        return f"{parts[0]} {parts[1]}"
    return str(canonical).strip()

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", str(s)).strip()

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

def gbif_synonyms_cached(usage_key, cache: dict, sleep_s: float = 0.1) -> list[str]:
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
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()

    syns = []
    for item in data.get("results", []):
        nm = item.get("scientificName") or item.get("canonicalName")
        if nm:
            syns.append(str(nm))

    cache[k] = syns
    time.sleep(sleep_s)
    return syns

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

        # GBIF vernacular model has "preferred" (Boolean). :contentReference[oaicite:1]{index=1}
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
            # language unknown, mark as not preferred
            out.append({"name": cleaned, "lang": "", "preferred": False})

    # De-dupe (case-insensitive), keep first occurrence
    seen = set()
    deduped = []
    for d in out:
        k = d["name"].casefold()
        if k not in seen:
            seen.add(k)
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
        # if multiple, prefer the shortest preferred (often the main label)
        return sorted(preferred, key=lambda x: (len(x), x.lower()))[0]

    # fallback: prefer entries with known English language code
    english_coded = [v["name"] for v in vernaculars if v.get("lang") in EN_LANGS]
    pool = english_coded if english_coded else [v["name"] for v in vernaculars]

    # avoid super-short oddities by requiring at least 4 chars if possible
    pool2 = [n for n in pool if len(n) >= 4] or pool
    return sorted(pool2, key=lambda x: (len(x), x.lower()))[0]

# --------- load your plants ----------
plants = pd.read_excel(
    r"C:\Users\aron_\PycharmProjects\obsidian_app\PostgreSQL_DB\excel_files\tropical_test\tropusi_haszon_test.xlsx"
)
plants.columns = plants.columns.str.strip()

# --------- run matching + synonyms + English names ----------
match_cache = load_cache(MATCH_CACHE_PATH)
syn_cache = load_cache(SYN_CACHE_PATH)
vern_cache = load_cache(VERN_CACHE_PATH)

gbif_rows = []

for nm in plants["Latin name"].astype(str):
    res = gbif_match_cached(nm, cache=match_cache, kingdom="Plantae")

    canonical = res.get("canonicalName")
    target_genus = key_genus(canonical)
    usage_key = res.get("usageKey")

    # Synonyms (genus-guarded)
    synonyms = gbif_synonyms_cached(usage_key, cache=syn_cache)
    filtered_synonyms = [s for s in synonyms if key_genus(s) == target_genus]

    # Vernaculars (English) with preferred flag
    vernaculars = gbif_english_vernaculars_cached(usage_key, cache=vern_cache)
    primary_english = pick_primary_english_name_from_vernaculars(vernaculars)
    all_english_names = [v["name"] for v in vernaculars]

    gbif_rows.append({
        "input_name": nm,
        "gbif_matchType": res.get("matchType"),
        "gbif_confidence": res.get("confidence"),
        "gbif_canonicalName": canonical,
        "gbif_genus_species": to_genus_species(canonical),
        "gbif_scientificName": res.get("scientificName"),
        "gbif_usageKey": usage_key,

        "gbif_synonyms": " | ".join(filtered_synonyms),
        "gbif_synonym_count": len(filtered_synonyms),

        # english/common names
        "gbif_english_name": primary_english,
        "gbif_english_names": " | ".join(all_english_names),
        "gbif_english_name_count": len(all_english_names),
    })

save_cache(MATCH_CACHE_PATH, match_cache)
save_cache(SYN_CACHE_PATH, syn_cache)
save_cache(VERN_CACHE_PATH, vern_cache)

gbif_df = pd.DataFrame(gbif_rows)

# Excel-friendly outputs
gbif_df.to_csv("plants_gbif_matched.csv", index=False, encoding="utf-8-sig")
gbif_df.to_excel("plants_gbif_matched.xlsx", index=False)

print(gbif_df.head())
print("Wrote plants_gbif_matched.csv (utf-8-sig) and plants_gbif_matched.xlsx")
