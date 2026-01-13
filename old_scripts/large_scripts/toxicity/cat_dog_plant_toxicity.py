import pandas as pd
import re
import requests
import json
import time
from pathlib import Path
from rapidfuzz import process, fuzz

# =========================================================
# Paths (yours)
# =========================================================
ASPCA_PATH = r"/PostgreSQL_DB/excel_files/DogsCatsHorses_aspca_toxic_plant_list.csv"
GBIF_MATCHED_PATH = r"/PostgreSQL_DB/large_scripts/plants_gbif_matched.csv"
OUT_XLSX = "toxicity_results_pets_gbif.xlsx"

# =========================================================
# ASPCA columns (yours)
# =========================================================
NAME_COL = "Name"
ASPCA_SCI_COL = "Scientific_Name"
DOG_COL = "Toxicity_Dog"
CAT_COL = "Toxicity_Cat"

# =========================================================
# GBIF synonyms (cached)
# =========================================================
GBIF_SPECIES_URL = "https://api.gbif.org/v1/species"
HEADERS = {"User-Agent": "plant-toxicity-check/1.0 (aron_serebrenik@yahoo.com)"}
SYN_CACHE_PATH = Path("../gbif_syn_cache.json")

def load_syn_cache() -> dict:
    if SYN_CACHE_PATH.exists():
        return json.loads(SYN_CACHE_PATH.read_text(encoding="utf-8"))
    return {}

def save_syn_cache(cache: dict) -> None:
    SYN_CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

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

# =========================================================
# Normalization helpers
# =========================================================
RANK_MARKERS = {"sp", "spp", "ssp", "subsp", "var", "forma", "f", "cv", "cultivar"}

def strip_cultivar(s: str) -> str:
    if s is None or pd.isna(s):
        return ""
    s = str(s)
    s = re.sub(r"['\"][^'\"]+['\"]", " ", s)  # remove cultivar quotes
    return s

def clean_text(s: str) -> str:
    if s is None or pd.isna(s):
        return ""
    s = strip_cultivar(s).strip().lower()
    s = s.replace("×", "x")
    s = re.sub(r"[(),.;\[\]]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def clean_tokens(s: str) -> list[str]:
    s = clean_text(s)
    toks = s.split()
    toks = [t for t in toks if t not in RANK_MARKERS and t != "x"]
    return toks

def key_species(s: str) -> str:
    toks = clean_tokens(s)
    return f"{toks[0]} {toks[1]}" if len(toks) >= 2 else (toks[0] if toks else "")

def key_genus(s: str) -> str:
    toks = clean_tokens(s)
    return toks[0] if toks else ""

def key_full(s: str, max_tokens: int = 5) -> str:
    toks = clean_tokens(s)
    return " ".join(toks[:max_tokens]) if toks else ""

# =========================================================
# Load ASPCA dataset + precompute keys
# =========================================================
aspca = pd.read_csv(ASPCA_PATH)

aspca[NAME_COL] = aspca[NAME_COL].astype(str)
aspca[ASPCA_SCI_COL] = aspca[ASPCA_SCI_COL].astype(str)

aspca["aspca_species_key"] = aspca[ASPCA_SCI_COL].map(key_species)
aspca["aspca_genus_key"] = aspca[ASPCA_SCI_COL].map(key_genus)
aspca["aspca_full_key"] = aspca[ASPCA_SCI_COL].map(key_full)
aspca["name_norm"] = aspca[NAME_COL].map(clean_text)

aspca_by_species = aspca.set_index("aspca_species_key", drop=False)
aspca_species_list = aspca["aspca_species_key"].dropna().unique().tolist()

# =========================================================
# ASPCA lookup (tiered + fuzzy + genus-restricted)
# =========================================================
def _first_row(x):
    if isinstance(x, pd.DataFrame):
        return x.iloc[0]
    return x

def aspca_lookup(query: str):
    q_species = key_species(query)
    q_genus = key_genus(query)
    q_full = key_full(query)

    # 1) exact species
    if q_species and q_species in aspca_by_species.index:
        return _first_row(aspca_by_species.loc[q_species]), 100, "species_exact"

    # 2) fuzzy species (global)
    if q_species:
        m = process.extractOne(q_species, aspca_species_list, scorer=fuzz.WRatio)
        if m:
            match_key, score, _ = m
            if score >= 90:
                return _first_row(aspca_by_species.loc[match_key]), int(score), "species_fuzzy"

    # 3) genus-restricted fuzzy representative
    if q_genus:
        sub = aspca[aspca["aspca_genus_key"] == q_genus].copy()
        if len(sub):
            qn = clean_text(query)

            sub["score_sci"] = sub["aspca_full_key"].apply(lambda x: fuzz.WRatio(q_full, x) if q_full else 0)
            sub["score_name"] = sub["name_norm"].apply(lambda x: fuzz.WRatio(qn, x))
            sub["score"] = sub[["score_sci", "score_name"]].max(axis=1)

            best = sub.sort_values("score", ascending=False).iloc[0]
            best_score = int(best["score"])

            if best_score >= 75:
                return best, best_score, "genus_fuzzy"

    return None, 0, "no_match"

# =========================================================
# Genus-level "known toxic group" inference layer (AUTO)
# =========================================================
def parse_bool_toxic(x) -> bool | None:
    """
    Convert ASPCA toxicity cell to True/False/None.
    Handles typical dataset values like 'Toxic', 'Non-Toxic', 'Yes', 'No', etc.
    """
    if x is None or pd.isna(x):
        return None
    s = str(x).strip().lower()
    if not s:
        return None
    if s in {"toxic", "yes", "true", "1"}:
        return True
    if s in {"non-toxic", "nontoxic", "no", "false", "0"}:
        return False
    # sometimes ASPCA datasets have e.g. 'Mildly Toxic', 'Toxic to Cats', etc.
    if "toxic" in s and "non" not in s:
        return True
    return None

# Build genus statistics from ASPCA itself:
# If a genus has a strong majority of toxic entries, infer "likely toxic" for unmatched within that genus.
aspca["dog_tox_bool"] = aspca[DOG_COL].map(parse_bool_toxic)
aspca["cat_tox_bool"] = aspca[CAT_COL].map(parse_bool_toxic)

genus_stats = (
    aspca.groupby("aspca_genus_key", dropna=True)
        .agg(
            n=("aspca_genus_key", "size"),
            dog_toxic_rate=("dog_tox_bool", lambda s: float(pd.Series(s).dropna().mean()) if pd.Series(s).dropna().size else float("nan")),
            cat_toxic_rate=("cat_tox_bool", lambda s: float(pd.Series(s).dropna().mean()) if pd.Series(s).dropna().size else float("nan")),
            dog_known=("dog_tox_bool", lambda s: int(pd.Series(s).dropna().size)),
            cat_known=("cat_tox_bool", lambda s: int(pd.Series(s).dropna().size)),
        )
        .reset_index()
)

# thresholds you can tune (conservative defaults)
MIN_KNOWN_PER_GENUS = 3        # require at least 3 labeled entries
TOXIC_RATE_THRESHOLD = 0.80    # >=80% toxic => infer likely toxic

# quick lookup dict
genus_stats_by_key = genus_stats.set_index("aspca_genus_key").to_dict(orient="index")

def infer_from_genus(target_genus: str):
    """
    Return inferred toxicity for cats/dogs based on ASPCA genus statistics.
    Returns (dog_infer, cat_infer, inference_strength_dict) where dog_infer/cat_infer are:
      True / False / "unknown"
    """
    if not target_genus or target_genus not in genus_stats_by_key:
        return "unknown", "unknown", {}

    st = genus_stats_by_key[target_genus]
    dog_known = st.get("dog_known", 0)
    cat_known = st.get("cat_known", 0)
    dog_rate = st.get("dog_toxic_rate")
    cat_rate = st.get("cat_toxic_rate")

    dog_infer = "unknown"
    cat_infer = "unknown"

    if isinstance(dog_rate, float) and dog_known >= MIN_KNOWN_PER_GENUS:
        if dog_rate >= TOXIC_RATE_THRESHOLD:
            dog_infer = True
        elif dog_rate <= (1 - TOXIC_RATE_THRESHOLD):
            dog_infer = False

    if isinstance(cat_rate, float) and cat_known >= MIN_KNOWN_PER_GENUS:
        if cat_rate >= TOXIC_RATE_THRESHOLD:
            cat_infer = True
        elif cat_rate <= (1 - TOXIC_RATE_THRESHOLD):
            cat_infer = False

    return dog_infer, cat_infer, st

# =========================================================
# Load GBIF matched plants
# =========================================================
gbif_df = pd.read_csv(GBIF_MATCHED_PATH)
HAS_SYNONYM_COL = "gbif_synonyms" in gbif_df.columns

syn_cache = load_syn_cache()

def parse_synonyms_cell(cell) -> list[str]:
    if cell is None or pd.isna(cell):
        return []
    s = str(cell).strip()
    if not s:
        return []
    return [p.strip() for p in s.split("|") if p.strip()]

# =========================================================
# Match loop: ASPCA + genus-guardrails + inference fallback
# =========================================================
out_rows = []

for _, r in gbif_df.iterrows():
    input_name = r.get("input_name")
    canon = r.get("gbif_canonicalName")
    gs = r.get("gbif_genus_species")
    usage_key = r.get("gbif_usageKey")
    gbif_conf = r.get("gbif_confidence")

    target_name = canon if isinstance(canon, str) and canon.strip() else (gs if isinstance(gs, str) and gs.strip() else input_name)
    target_genus = key_genus(target_name)

    candidates = []
    for x in [canon, gs, input_name]:
        if isinstance(x, str) and x.strip():
            candidates.append(x.strip())

    syns = []
    if HAS_SYNONYM_COL:
        syns = parse_synonyms_cell(r.get("gbif_synonyms"))
    if not syns:
        syns = gbif_synonyms_cached(usage_key, syn_cache)

    # keep synonyms in same genus (prevents Terminalia -> Juglans mistakes)
    syns = [s for s in syns if key_genus(s) == target_genus] if target_genus else syns
    candidates.extend(syns)

    # de-duplicate (preserve order)
    seen = set()
    candidates = [c for c in candidates if not (c in seen or seen.add(c))]

    hit = None
    best_score = 0
    best_method = "no_match"
    best_query = None

    for cand in candidates:
        h, score, method = aspca_lookup(cand)
        if h is None:
            continue

        # accept immediately if exact species match
        if method == "species_exact" and score == 100:
            hit, best_score, best_method, best_query = h, score, method, cand
            break

        # genus guardrails for non-exact matches
        aspca_genus = key_genus(h.get(ASPCA_SCI_COL, ""))
        cand_genus = key_genus(cand)

        if target_genus:
            if cand_genus and cand_genus != target_genus:
                continue
            if aspca_genus and aspca_genus != target_genus:
                continue

        if score > best_score:
            hit, best_score, best_method, best_query = h, score, method, cand

    # If no ASPCA hit, infer from genus stats (clearly labeled)
    inferred = False
    inferred_dog = "unknown"
    inferred_cat = "unknown"
    infer_info = {}

    if hit is None:
        inferred_dog, inferred_cat, infer_info = infer_from_genus(target_genus)
        inferred = (inferred_dog != "unknown") or (inferred_cat != "unknown")

    out_rows.append({
        "input_latin_name": input_name,
        "query_used": best_query,
        "target_genus": target_genus,

        # direct ASPCA match info
        "aspca_match_method": best_method,
        "aspca_match_score": best_score,
        "aspca_name": hit.get(NAME_COL) if hit is not None else None,
        "aspca_scientific_name": hit.get(ASPCA_SCI_COL) if hit is not None else None,
        "toxic_cats": str(hit.get(CAT_COL)) if hit is not None else "unknown",
        "toxic_dogs": str(hit.get(DOG_COL)) if hit is not None else "unknown",
        "source_pets": "ASPCA" if hit is not None else None,

        # inference layer (clearly labeled)
        "inferred_from_genus": inferred,
        "inferred_toxic_cats": inferred_cat,
        "inferred_toxic_dogs": inferred_dog,
        "inference_source": "ASPCA_genus_stats" if inferred else None,
        "inference_genus_n": infer_info.get("n") if infer_info else None,
        "inference_cat_known": infer_info.get("cat_known") if infer_info else None,
        "inference_cat_toxic_rate": infer_info.get("cat_toxic_rate") if infer_info else None,
        "inference_dog_known": infer_info.get("dog_known") if infer_info else None,
        "inference_dog_toxic_rate": infer_info.get("dog_toxic_rate") if infer_info else None,

        # review logic: inference ALWAYS needs review; non-exact matches too
        "needs_manual_review": (
            inferred or
            (hit is None) or
            (best_method != "species_exact") or
            (pd.notna(gbif_conf) and gbif_conf < 90)
        ),
    })

results = pd.DataFrame(out_rows)
results.to_excel(OUT_XLSX, index=False)

save_syn_cache(syn_cache)

print("Direct ASPCA matched:", results["source_pets"].notna().sum(), "of", len(results))
print("Genus-inferred:", results["inferred_from_genus"].fillna(False).sum(), "of", len(results))
print(f"Wrote {OUT_XLSX}")
