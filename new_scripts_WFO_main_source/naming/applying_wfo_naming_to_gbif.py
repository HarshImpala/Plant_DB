import json
import re
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ============================================================
# INPUT / OUTPUT
# ============================================================
INFILE_XLSX = "plants_gbif_matched.xlsx"   # or plants_gbif_matched_plus_wfo.xlsx if you prefer
OUT_XLSX = "plants_gbif_matched_plus_wfo.xlsx"
OUT_CSV = "plants_gbif_matched_plus_wfo.csv"

# ============================================================
# WFO endpoints
# ============================================================
WFO_MATCH_REST_URL = "https://list.worldfloraonline.org/matching_rest.php"
WFO_BROWSER_URL = "https://list.worldfloraonline.org/browser.php"

HEADERS = {
    "User-Agent": "plant-catalogue/1.0 (aron_serebrenik@yahoo.com)",
    "Accept": "application/json",
}

# ============================================================
# caches
# ============================================================
WFO_MATCH_CACHE_PATH = Path("wfo_match_cache.json")
WFO_DETAILS_CACHE_PATH = Path("wfo_details_cache.json")

SLEEP_S = 0.15
PROGRESS_EVERY_N = 10


# ============================================================
# helpers
# ============================================================
def load_cache(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}

def save_cache(path: Path, cache: dict) -> None:
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", str(s)).strip()

def cf(s: str) -> str:
    return normalize_spaces(s).casefold()

def dedupe_casefold(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for s in items:
        s2 = normalize_spaces(s)
        if not s2:
            continue
        k = s2.casefold()
        if k not in seen:
            seen.add(k)
            out.append(s2)
    return out

def strip_authors_to_canonical(name: str) -> str:
    """
    'Acalypha hispida Burm.f.' -> 'Acalypha hispida'
    """
    s = normalize_spaces(name).replace("×", "x")
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"[(),;]", " ", s)
    s = normalize_spaces(s)
    parts = s.split()
    if len(parts) < 2:
        return s
    genus, species = parts[0], parts[1]
    if len(parts) >= 4 and parts[2].lower() in {"subsp.", "subsp", "var.", "var", "f.", "f"}:
        return normalize_spaces(f"{genus} {species} {parts[2]} {parts[3]}")
    return normalize_spaces(f"{genus} {species}")

def author_spacing_variants(name: str) -> list[str]:
    """
    Helps WFO with Burm.f. vs Burm. f.
    """
    s = normalize_spaces(name)
    variants = [s]
    variants.append(re.sub(r"([A-Za-z])\.(?=[a-z])", r"\1. ", s))  # Burm.f. -> Burm. f.
    variants.append(re.sub(r"\.\s*$", "", s))                      # trim trailing dot
    variants.append(s.replace(".", ""))                            # remove dots
    variants.append(re.sub(r"[;,]", " ", s))                       # remove punctuation
    return dedupe_casefold(variants)

def generate_wfo_query_candidates(gbif_scientific: str, gbif_canonical: str | None = None) -> list[str]:
    candidates: list[str] = []
    if gbif_canonical and str(gbif_canonical).strip():
        candidates.append(str(gbif_canonical).strip())

    if gbif_scientific and gbif_scientific.strip():
        candidates.append(strip_authors_to_canonical(gbif_scientific))
        candidates.extend(author_spacing_variants(gbif_scientific))

    return dedupe_casefold(candidates)

def pick_gbif_scientific_as_main(row: pd.Series) -> tuple[str, str]:
    sci = ""
    can = ""
    for col in ("gbif_scientificName", "gbif_matched_scientificName", "input_name"):
        v = row.get(col, "")
        if pd.notna(v) and str(v).strip():
            sci = str(v).strip()
            break
    for col in ("gbif_canonicalName", "gbif_matched_canonicalName"):
        v = row.get(col, "")
        if pd.notna(v) and str(v).strip():
            can = str(v).strip()
            break
    return sci, can


# ============================================================
# STRICT botanical-name validator (prevents bibliography grabs)
# ============================================================
def is_plausible_scientific_name(s: str) -> bool:
    """
    Strict-ish filter:
      - reject anything with digits (kills 1781-1838 etc.)
      - must start with Genus (Capitalized) + species (all lowercase)
      - allow optional rank words + infraspecific epithet
      - rest may contain authorship, parentheses etc.
    """
    s = normalize_spaces(s)
    if not s:
        return False
    if any(ch.isdigit() for ch in s):
        return False

    # Tokenize a simplified view (drop some punctuation that confuses token checks)
    t = re.sub(r"[,\[\];]", " ", s)
    t = normalize_spaces(t)

    parts = t.split()
    if len(parts) < 2:
        return False

    genus = parts[0]
    species = parts[1]

    # Genus: Capitalized, letters/hyphen only
    if not re.fullmatch(r"[A-Z][a-z-]+", genus):
        return False

    # Species epithet: lowercase letters/hyphen/×/x only
    # (this blocks "von", but note "von" is also lowercase; the real blocker is genus)
    # The real fix is the genus+species pattern plus no digits + section-bounding below.
    if not re.fullmatch(r"[a-z×x-]+", species):
        return False

    # Avoid ultra-short weirdness
    if len(species) < 2:
        return False

    return True


# ============================================================
# WFO REST match (cached)
# ============================================================
def wfo_match_rest_cached(
    name: str,
    cache: dict,
    fuzzy_names: int = 0,
    fuzzy_authors: int = 0,
    check_homonyms: bool = True,
    check_rank: bool = True,
    accept_single_candidate: bool = True,
    sleep_s: float = SLEEP_S,
) -> dict:
    key = (
        f"rest||{name}||fn={fuzzy_names}||fa={fuzzy_authors}"
        f"||h={check_homonyms}||r={check_rank}||a={accept_single_candidate}"
    )
    if key in cache:
        return cache[key]

    params = {
        "input_string": name,
        "fuzzy_names": fuzzy_names if fuzzy_names else None,
        "fuzzy_authors": fuzzy_authors if fuzzy_authors else None,
        "check_homonyms": "true" if check_homonyms else "false",
        "check_rank": "true" if check_rank else "false",
        "accept_single_candidate": "true" if accept_single_candidate else "false",
    }
    params = {k: v for k, v in params.items() if v is not None}

    r = requests.get(WFO_MATCH_REST_URL, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()

    cache[key] = data
    time.sleep(sleep_s)
    return data

def _extract_name_fields(obj: dict) -> tuple[str, str]:
    wfo_id = str(obj.get("wfo_id") or obj.get("wfoId") or "").strip()
    full_plain = normalize_spaces(obj.get("full_name_plain") or obj.get("fullNameStringPlain") or "")
    return wfo_id, full_plain

def rest_match_extract(rest: dict) -> tuple[str, str, list, bool]:
    mobj = rest.get("match") or {}
    wfo_id, full_plain = _extract_name_fields(mobj)
    narrative = rest.get("narrative") or []
    ok_match = bool(wfo_id) or bool(full_plain)
    return wfo_id, full_plain, narrative, ok_match

def pick_best_candidate_from_rest(rest: dict, desired_fullname: str) -> tuple[str, str]:
    candidates = rest.get("candidates") or []
    if not isinstance(candidates, list) or not candidates:
        return "", ""

    desired_full = cf(desired_fullname)
    desired_can = cf(strip_authors_to_canonical(desired_fullname))

    for c in candidates:
        if not isinstance(c, dict):
            continue
        cid, cname = _extract_name_fields(c)
        if cname and cf(cname) == desired_full:
            return cid, cname

    for c in candidates:
        if not isinstance(c, dict):
            continue
        cid, cname = _extract_name_fields(c)
        if cname and cf(strip_authors_to_canonical(cname)) == desired_can:
            return cid, cname

    for c in candidates:
        if not isinstance(c, dict):
            continue
        cid, cname = _extract_name_fields(c)
        if cid or cname:
            return cid, cname

    return "", ""


# ============================================================
# WFO browser.php HTML (STRICT section-bounded synonym extraction)
# ============================================================
def wfo_browser_html_cached(wfo_id: str, cache: dict, sleep_s: float = SLEEP_S) -> str:
    if not wfo_id:
        return ""
    k = f"browser||{wfo_id}"
    if k in cache:
        return cache[k]

    r = requests.get(
        WFO_BROWSER_URL,
        params={"id": wfo_id},
        headers={"User-Agent": HEADERS["User-Agent"]},
        timeout=30,
    )
    r.raise_for_status()
    html = r.text
    cache[k] = html
    time.sleep(sleep_s)
    return html

def _header_tag_name(tag) -> str:
    return tag.name.lower() if getattr(tag, "name", None) else ""

def _is_header(tag) -> bool:
    return _header_tag_name(tag) in {"h1", "h2", "h3", "h4", "h5", "h6"}

def _header_text(tag) -> str:
    return normalize_spaces(tag.get_text(" ", strip=True)) if tag else ""

def _find_first_header(soup: BeautifulSoup, regex: re.Pattern):
    for h in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
        if regex.search(_header_text(h)):
            return h
    return None

def _collect_until_next_header(start_header, stop_header_regex: re.Pattern) -> list:
    """
    Collect sibling/next elements after start_header until:
      - next header OR
      - header matches stop_header_regex (References/Bibliography/etc.)
    Returns a list of tags within the section.
    """
    out = []
    cur = start_header
    steps = 0
    while cur is not None and steps < 300:
        cur = cur.find_next()
        if cur is None:
            break

        if _is_header(cur):
            ht = _header_text(cur)
            if stop_header_regex.search(ht):
                break
            # stop at the next header regardless
            break

        out.append(cur)
        steps += 1
    return out

def parse_browser_for_accepted_and_synonyms(html: str) -> tuple[str, list[str]]:
    if not html:
        return "", []

    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text("\n", strip=True)

    # accepted name (text-based)
    accepted = ""
    m = re.search(r"Accepted name is:\s*(.+)", page_text)
    if m:
        accepted = normalize_spaces(m.group(1))

    # --- synonyms section bounded extraction ---
    synonyms: list[str] = []

    syn_header = _find_first_header(soup, re.compile(r"\bSynonym(s)?\b", re.IGNORECASE))

    # Stop at headers that begin non-synonym sections
    stop_hdr = re.compile(r"\b(Bibliograph|Reference|Citation|Literature|Sources?)\b", re.IGNORECASE)

    if syn_header:
        section_tags = _collect_until_next_header(syn_header, stop_header_regex=stop_hdr)

        # Prefer anchor texts that link to browser.php?id=...
        for tag in section_tags:
            # If this tag itself is a header, we'd stop earlier. So here: collect names.
            for a in tag.find_all("a", href=True):
                href = a.get("href", "")
                if "browser.php" not in href:
                    continue
                txt = normalize_spaces(a.get_text(" ", strip=True))
                if is_plausible_scientific_name(txt):
                    synonyms.append(txt)

            # Also allow table/list items inside the synonym section
            for li in tag.find_all("li"):
                txt = normalize_spaces(li.get_text(" ", strip=True))
                if is_plausible_scientific_name(txt):
                    synonyms.append(txt)

            for tr in tag.find_all("tr"):
                txt = normalize_spaces(tr.get_text(" ", strip=True))
                if is_plausible_scientific_name(txt):
                    synonyms.append(txt)

    # De-dupe
    synonyms = dedupe_casefold(synonyms)

    # If somehow accepted name appears in synonym list, remove it
    if accepted:
        a_cf = cf(accepted)
        synonyms = [s for s in synonyms if cf(s) != a_cf]

    return accepted, synonyms


# ============================================================
# Robust REST matching strategy (kept)
# ============================================================
def wfo_rest_match_with_variants(candidates: list[str], match_cache: dict) -> tuple[str, str, str, dict]:
    attempts = []

    fuzzy_sets = [
        {"fuzzy_names": 0, "fuzzy_authors": 0},
        {"fuzzy_names": 1, "fuzzy_authors": 0},
        {"fuzzy_names": 1, "fuzzy_authors": 1},
        {"fuzzy_names": 2, "fuzzy_authors": 1},
    ]
    homonym_sets = [True, False]
    accept_sets = [True, False]

    for cand in candidates:
        for h in homonym_sets:
            for fs in fuzzy_sets:
                for a in accept_sets:
                    rest = wfo_match_rest_cached(
                        cand,
                        cache=match_cache,
                        fuzzy_names=fs["fuzzy_names"],
                        fuzzy_authors=fs["fuzzy_authors"],
                        check_homonyms=h,
                        check_rank=True,
                        accept_single_candidate=a,
                    )
                    wfo_id, full_plain, narrative, ok = rest_match_extract(rest)

                    chosen_from_candidates = False
                    if not ok and a is False:
                        cid, cname = pick_best_candidate_from_rest(rest, desired_fullname=cand)
                        if cid or cname:
                            wfo_id, full_plain = cid, cname
                            ok = True
                            chosen_from_candidates = True

                    attempts.append({
                        "query": cand,
                        "check_homonyms": h,
                        "accept_single_candidate": a,
                        "fuzzy_names": fs["fuzzy_names"],
                        "fuzzy_authors": fs["fuzzy_authors"],
                        "ok": ok,
                        "chosen_from_candidates": chosen_from_candidates,
                        "wfo_id": wfo_id,
                        "full_name_plain": full_plain,
                        "candidate_count": len(rest.get("candidates") or []) if isinstance(rest.get("candidates"), list) else None,
                    })

                    if ok:
                        dbg = {
                            "mode": "rest+variants",
                            "chosen_query": cand,
                            "chosen_params": {
                                "check_homonyms": h,
                                "accept_single_candidate": a,
                                "fuzzy_names": fs["fuzzy_names"],
                                "fuzzy_authors": fs["fuzzy_authors"],
                                "chosen_from_candidates": chosen_from_candidates,
                            },
                            "wfo_id": wfo_id,
                            "full_name_plain": full_plain,
                            "attempts_count": len(attempts),
                        }
                        return wfo_id, full_plain, cand, dbg

    dbg = {"mode": "rest+variants", "note": "no match", "attempts_count": len(attempts), "attempts_tail": attempts[-10:]}
    return "", "", "", dbg


# ============================================================
# Main
# ============================================================
def main():
    if INFILE_XLSX.lower().endswith(".csv"):
        df = pd.read_csv(INFILE_XLSX, encoding="utf-8-sig")
    else:
        df = pd.read_excel(INFILE_XLSX)

    df.columns = df.columns.str.strip()

    match_cache = load_cache(WFO_MATCH_CACHE_PATH)
    details_cache = load_cache(WFO_DETAILS_CACHE_PATH)

    n = len(df)
    t0 = time.time()

    # columns (create if missing)
    out_cols = [
        "wfo_input_used",
        "wfo_query_used",
        "wfo_match_id",
        "wfo_match_full_name",
        "wfo_accepted_name",
        "wfo_synonyms",
        "wfo_synonym_count",
        "wfo_debug",
    ]
    for c in out_cols:
        if c not in df.columns:
            df[c] = ""

    for idx, (i, row) in enumerate(df.iterrows(), start=1):
        gbif_sci, gbif_can = pick_gbif_scientific_as_main(row)
        df.at[i, "wfo_input_used"] = gbif_sci

        if idx == 1 or idx % PROGRESS_EVERY_N == 0 or idx == n:
            pct = (idx / n) * 100 if n else 100.0
            elapsed = time.time() - t0
            rate = idx / elapsed if elapsed > 0 else 0.0
            eta_s = (n - idx) / rate if rate > 0 else 0.0
            print(f"[{idx}/{n} | {pct:5.1f}%] rate={rate:0.2f}/s eta={eta_s/60:0.1f}m name='{gbif_sci}'")

        if not gbif_sci:
            df.at[i, "wfo_debug"] = json.dumps({"mode": "skip_empty"}, ensure_ascii=False)
            continue

        candidates = generate_wfo_query_candidates(gbif_sci, gbif_canonical=gbif_can)
        wfo_id, full_plain, query_used, dbg = wfo_rest_match_with_variants(candidates, match_cache=match_cache)

        df.at[i, "wfo_query_used"] = query_used
        df.at[i, "wfo_match_id"] = wfo_id
        df.at[i, "wfo_match_full_name"] = full_plain

        accepted = ""
        synonyms = []
        if wfo_id:
            html = wfo_browser_html_cached(wfo_id, cache=details_cache)
            accepted, synonyms = parse_browser_for_accepted_and_synonyms(html)

        df.at[i, "wfo_accepted_name"] = accepted or full_plain
        df.at[i, "wfo_synonyms"] = " | ".join(synonyms)
        df.at[i, "wfo_synonym_count"] = len(synonyms)

        dbg2 = dict(dbg)
        dbg2["parse"] = {
            "accepted_found": bool(accepted),
            "synonym_count": len(synonyms),
        }
        df.at[i, "wfo_debug"] = json.dumps(dbg2, ensure_ascii=False)

    save_cache(WFO_MATCH_CACHE_PATH, match_cache)
    save_cache(WFO_DETAILS_CACHE_PATH, details_cache)

    df.to_excel(OUT_XLSX, index=False)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    print(f"Wrote: {OUT_XLSX}")
    print(f"Wrote: {OUT_CSV}")
    print(f"Caches: {WFO_MATCH_CACHE_PATH} , {WFO_DETAILS_CACHE_PATH}")

if __name__ == "__main__":
    main()
