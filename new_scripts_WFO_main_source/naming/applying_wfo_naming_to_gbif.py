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
INFILE_XLSX = "plants_gbif_matched.xlsx"   # or "plants_gbif_matched.csv"
OUT_XLSX = "plants_gbif_matched_plus_wfo.xlsx"
OUT_CSV = "plants_gbif_matched_plus_wfo.csv"

# ============================================================
# WFO endpoints
# ============================================================
WFO_MATCH_REST_URL = "https://list.worldfloraonline.org/matching_rest.php"
WFO_GQL_URL = "https://list.worldfloraonline.org/gql.php"
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

# throttle
SLEEP_S = 0.15

# progress
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

def dedupe_casefold(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for s in items:
        k = s.casefold()
        if k not in seen:
            seen.add(k)
            out.append(s)
    return out

def looks_like_botanical_name(s: str) -> bool:
    s = normalize_spaces(s)
    if len(s) < 4 or len(s) > 220:
        return False
    return bool(re.match(r"^[A-Z][a-z-]+(\s+[a-z×-]+){1,3}.*", s))

def strip_authors_to_canonical(name: str) -> str:
    """
    Best-effort canonicalization to just 'Genus species' (and optionally infraspecific epithet).
    Works for strings like 'Acalypha hispida Burm.f.' -> 'Acalypha hispida'
    """
    s = normalize_spaces(name).replace("×", "x")
    # remove bracketed author parts like "(Saff.)"
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"[(),;]", " ", s)
    s = normalize_spaces(s)

    parts = s.split()
    if len(parts) < 2:
        return s

    # keep Genus + species
    genus = parts[0]
    species = parts[1]

    # optionally keep common infraspecific ranks if present (subsp., var., f.)
    if len(parts) >= 4 and parts[2].lower() in {"subsp.", "subsp", "var.", "var", "f.", "f"}:
        return normalize_spaces(f"{genus} {species} {parts[2]} {parts[3]}")

    return normalize_spaces(f"{genus} {species}")

def author_punctuation_variants(name: str) -> list[str]:
    """
    Generate small variants for author abbreviations:
    - remove trailing dot(s)
    - collapse multiple dots
    - remove punctuation entirely from author part
    """
    s = normalize_spaces(name)
    vars_ = [s]

    # remove trailing dot
    vars_.append(re.sub(r"\.\s*$", "", s))

    # collapse multiple dots
    vars_.append(re.sub(r"\.{2,}", ".", s))

    # remove all dots
    vars_.append(s.replace(".", ""))

    # remove commas/semicolons
    vars_.append(re.sub(r"[;,]", " ", s))

    return dedupe_casefold([normalize_spaces(v) for v in vars_ if v and v.strip()])

def generate_wfo_query_candidates(gbif_scientific: str, gbif_canonical: str | None = None) -> list[str]:
    """
    Try WFO with a priority list of query variants.
    This fixes cases like 'Acalypha hispida Burm.f.' by falling back to canonical.
    """
    candidates: list[str] = []

    if gbif_scientific and gbif_scientific.strip():
        candidates.extend(author_punctuation_variants(gbif_scientific))

    # canonical provided by GBIF (often without authors)
    if gbif_canonical and str(gbif_canonical).strip():
        candidates.append(str(gbif_canonical).strip())

    # canonical derived from scientific
    if gbif_scientific and gbif_scientific.strip():
        candidates.append(strip_authors_to_canonical(gbif_scientific))

    # de-dupe and remove empties
    candidates = dedupe_casefold([normalize_spaces(c) for c in candidates if c and str(c).strip()])

    # ensure the fully stripped canonical is near the front (WFO often matches this best)
    canon = strip_authors_to_canonical(gbif_scientific) if gbif_scientific else ""
    if canon and canon in candidates:
        candidates.remove(canon)
        candidates.insert(0, canon)

    # ensure gbif_canonical near front
    if gbif_canonical:
        gc = str(gbif_canonical).strip()
        if gc and gc in candidates:
            candidates.remove(gc)
            candidates.insert(0, gc)

    return candidates

def pick_gbif_scientific_as_main(row: pd.Series) -> tuple[str, str]:
    """
    Per request: use gbif_scientificName as main lookup source.
    Returns (gbif_scientificName, gbif_canonicalName) with fallbacks.
    """
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
    key = f"rest||{name}||fn={fuzzy_names}||fa={fuzzy_authors}||h={check_homonyms}||r={check_rank}||a={accept_single_candidate}"
    if key in cache:
        return cache[key]

    params = {
        "input_string": name,
        "fuzzy_names": fuzzy_names if fuzzy_names else None,
        "fuzzy_authors": fuzzy_authors if fuzzy_authors else None,
        "check_homonyms": "true" if check_homonyms else None,
        "check_rank": "true" if check_rank else None,
        "accept_single_candidate": "true" if accept_single_candidate else None,
    }
    params = {k: v for k, v in params.items() if v is not None}

    r = requests.get(WFO_MATCH_REST_URL, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()

    cache[key] = data
    time.sleep(sleep_s)
    return data

def rest_match_extract(rest: dict) -> tuple[str, str, list[str], bool]:
    """
    Returns (wfo_id, full_name_plain, narrative, ok_match)
    """
    mobj = rest.get("match") or {}
    wfo_id = str(mobj.get("wfo_id") or mobj.get("wfoId") or "").strip()
    full_plain = normalize_spaces(mobj.get("full_name_plain") or mobj.get("fullNameStringPlain") or "")
    narrative = rest.get("narrative") or []
    ok_match = bool(wfo_id) or bool(full_plain)
    return wfo_id, full_plain, narrative, ok_match

# ============================================================
# WFO: GraphQL (optional; best effort)
# ============================================================
def wfo_gql(query: str, variables: dict | None = None, timeout: int = 45) -> dict:
    payload = {"query": query, "variables": variables or {}}
    r = requests.post(WFO_GQL_URL, json=payload, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.json()

def wfo_try_gql_accepted_and_synonyms(input_name: str) -> tuple[str, str, list[str], dict]:
    query = """
    query($s: String!) {
      taxonNameMatch(inputString: $s, acceptSingleCandidate: true) {
        match {
          wfoId
          fullNameStringPlain
          currentNameUsage {
            role
            taxonConcept {
              wfoId
              acceptedName { wfoId fullNameStringPlain }
              synonyms { wfoId fullNameStringPlain }
            }
          }
        }
        narrative
      }
    }
    """
    try:
        resp = wfo_gql(query, {"s": input_name})
        if "errors" in resp and resp["errors"]:
            return "", "", [], {"mode": "gql", "errors": resp["errors"]}

        match = resp.get("data", {}).get("taxonNameMatch", {}).get("match")
        if not match:
            return "", "", [], {"mode": "gql", "note": "no match"}

        matched_name = normalize_spaces(match.get("fullNameStringPlain") or "")
        accepted_node = (
            match.get("currentNameUsage", {})
                .get("taxonConcept", {})
                .get("acceptedName", {})
        )
        accepted_name = normalize_spaces(accepted_node.get("fullNameStringPlain") or "")

        syn_nodes = (
            match.get("currentNameUsage", {})
                .get("taxonConcept", {})
                .get("synonyms", []) or []
        )
        synonyms = [normalize_spaces(x.get("fullNameStringPlain") or "") for x in syn_nodes if x.get("fullNameStringPlain")]
        synonyms = dedupe_casefold([s for s in synonyms if s])

        if not accepted_name and matched_name:
            accepted_name = matched_name

        dbg = {
            "mode": "gql",
            "ok": True,
            "wfo_id": match.get("wfoId"),
            "narrative": resp.get("data", {}).get("taxonNameMatch", {}).get("narrative"),
        }
        return matched_name, accepted_name, synonyms, dbg
    except Exception as e:
        return "", "", [], {"mode": "gql", "exception": repr(e)}

# ============================================================
# WFO: Browser HTML (fallback)
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

def _find_section_container(soup: BeautifulSoup, header_regex: re.Pattern):
    candidates = soup.find_all(string=lambda t: t and header_regex.search(str(t)))
    for c in candidates:
        tag = c.parent
        if not tag:
            continue
        cur = tag
        for _ in range(25):
            cur = cur.find_next()
            if cur is None:
                break
            if cur.name in ("table", "ul", "ol"):
                return cur
            if cur.name == "div" and (cur.find("li") or cur.find("tr")):
                return cur
    return None

def parse_browser_for_accepted_and_synonyms(html: str) -> tuple[str, list[str]]:
    if not html:
        return "", []

    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text("\n", strip=True)

    accepted = ""
    m = re.search(r"Accepted name is:\s*(.+)", page_text)
    if m:
        accepted = normalize_spaces(m.group(1))

    synonyms: list[str] = []
    sec = _find_section_container(soup, re.compile(r"\bSynonym(s)?\b", re.IGNORECASE))
    if sec:
        # tables
        tables = [sec] if sec.name == "table" else sec.find_all("table")
        for tbl in tables:
            for tr in tbl.find_all("tr"):
                txt = normalize_spaces(tr.get_text(" ", strip=True))
                if looks_like_botanical_name(txt):
                    synonyms.append(txt)
        # lists
        lists = []
        if sec.name in ("ul", "ol"):
            lists.append(sec)
        lists.extend(sec.find_all(["ul", "ol"]))
        for ul in lists:
            for li in ul.find_all("li"):
                txt = normalize_spaces(li.get_text(" ", strip=True))
                if looks_like_botanical_name(txt):
                    synonyms.append(txt)
        # div
        if sec.name == "div":
            for li in sec.find_all("li"):
                txt = normalize_spaces(li.get_text(" ", strip=True))
                if looks_like_botanical_name(txt):
                    synonyms.append(txt)

    synonyms = dedupe_casefold(synonyms)
    return accepted, synonyms

# ============================================================
# NEW: multi-try WFO matching strategy
# ============================================================
def wfo_rest_match_with_variants(
    candidates: list[str],
    match_cache: dict,
) -> tuple[str, str, list[str], dict]:
    """
    Try REST matching with multiple input variants, escalating to fuzzy matching.
    Returns: (wfo_id, full_plain, narrative, debug)
    """
    attempts = []
    # progressively more permissive parameter sets
    param_sets = [
        {"fuzzy_names": 0, "fuzzy_authors": 0},
        {"fuzzy_names": 1, "fuzzy_authors": 0},
        {"fuzzy_names": 1, "fuzzy_authors": 1},
        {"fuzzy_names": 2, "fuzzy_authors": 1},
    ]

    for cand in candidates:
        for ps in param_sets:
            rest = wfo_match_rest_cached(
                cand,
                cache=match_cache,
                fuzzy_names=ps["fuzzy_names"],
                fuzzy_authors=ps["fuzzy_authors"],
                check_homonyms=True,
                check_rank=True,
                accept_single_candidate=True,
            )
            wfo_id, full_plain, narrative, ok = rest_match_extract(rest)
            attempts.append({
                "query": cand,
                "fuzzy_names": ps["fuzzy_names"],
                "fuzzy_authors": ps["fuzzy_authors"],
                "wfo_id": wfo_id,
                "full_name_plain": full_plain,
                "error": rest.get("error", None),
                "count_candidates": len((rest.get("candidates") or [])) if isinstance(rest, dict) else None,
            })
            if ok:
                dbg = {
                    "mode": "rest+variants",
                    "chosen_query": cand,
                    "chosen_params": ps,
                    "wfo_id": wfo_id,
                    "full_name_plain": full_plain,
                    "rest_narrative": narrative,
                    "attempts": attempts,
                }
                return wfo_id, full_plain, narrative, dbg

    dbg = {"mode": "rest+variants", "note": "no match", "attempts": attempts}
    return "", "", [], dbg

# ============================================================
# Main
# ============================================================
def main():
    # Load input
    if INFILE_XLSX.lower().endswith(".csv"):
        df = pd.read_csv(INFILE_XLSX, encoding="utf-8-sig")
    else:
        df = pd.read_excel(INFILE_XLSX)

    match_cache = load_cache(WFO_MATCH_CACHE_PATH)
    details_cache = load_cache(WFO_DETAILS_CACHE_PATH)

    n = len(df)
    t0 = time.time()

    new_cols = [
        "wfo_input_used",          # the primary GBIF scientific name used
        "wfo_query_used",          # which variant actually matched (if any)
        "wfo_match_wfo_id",
        "wfo_match_full_name",
        "wfo_accepted_name",
        "wfo_synonyms",
        "wfo_synonym_count",
        "wfo_method",
        "wfo_debug",
    ]
    for c in new_cols:
        if c not in df.columns:
            df[c] = ""

    for idx, (i, row) in enumerate(df.iterrows(), start=1):
        gbif_sci, gbif_can = pick_gbif_scientific_as_main(row)
        df.at[i, "wfo_input_used"] = gbif_sci

        # progress
        if idx == 1 or idx % PROGRESS_EVERY_N == 0 or idx == n:
            pct = (idx / n) * 100 if n else 100.0
            elapsed = time.time() - t0
            rate = idx / elapsed if elapsed > 0 else 0.0
            eta_s = (n - idx) / rate if rate > 0 else 0.0
            print(f"[{idx}/{n} | {pct:5.1f}%] rate={rate:0.2f}/s eta={eta_s/60:0.1f}m name='{gbif_sci}'")

        if not gbif_sci:
            df.at[i, "wfo_method"] = "skip_empty"
            df.at[i, "wfo_debug"] = json.dumps({"mode": "skip_empty"}, ensure_ascii=False)
            continue

        # Generate query candidates (THIS is what fixes Acalypha hispida Burm.f.)
        candidates = generate_wfo_query_candidates(gbif_sci, gbif_canonical=gbif_can)

        # 1) Try GraphQL on the best canonical-ish candidates first (optional, best-effort)
        #    If it fails, we still have robust REST+variants.
        gql_used = False
        matched_name = accepted_name = ""
        synonyms = []
        dbg_final = {}

        for cand in candidates[:2]:  # only top 2 to avoid extra load
            m, a, syns, dbg = wfo_try_gql_accepted_and_synonyms(cand)
            if m or a or syns:
                matched_name, accepted_name, synonyms = m, a, syns
                dbg_final = dbg | {"tried_query": cand, "candidates": candidates[:6]}
                df.at[i, "wfo_query_used"] = cand
                df.at[i, "wfo_method"] = "gql"
                gql_used = True
                break

        if gql_used:
            df.at[i, "wfo_match_full_name"] = matched_name
            df.at[i, "wfo_accepted_name"] = accepted_name
            df.at[i, "wfo_synonyms"] = " | ".join(synonyms)
            df.at[i, "wfo_synonym_count"] = len(synonyms)
            df.at[i, "wfo_debug"] = json.dumps(dbg_final, ensure_ascii=False)

            # still fetch/store WFO id via REST on the same query used
            rest = wfo_match_rest_cached(df.at[i, "wfo_query_used"], cache=match_cache)
            wfo_id, full_plain, narrative, ok = rest_match_extract(rest)
            if ok:
                df.at[i, "wfo_match_wfo_id"] = wfo_id
                if not df.at[i, "wfo_match_full_name"]:
                    df.at[i, "wfo_match_full_name"] = full_plain
            continue

        # 2) REST matching with multiple variants + escalating fuzzy parameters
        wfo_id, full_plain, narrative, dbg = wfo_rest_match_with_variants(candidates, match_cache=match_cache)

        df.at[i, "wfo_match_wfo_id"] = wfo_id
        df.at[i, "wfo_match_full_name"] = full_plain
        df.at[i, "wfo_method"] = "rest+variants"
        df.at[i, "wfo_debug"] = json.dumps(dbg, ensure_ascii=False)

        # which query succeeded?
        df.at[i, "wfo_query_used"] = dbg.get("chosen_query", "")

        # 3) If we have an ID, parse accepted name + synonyms from browser.php
        accepted2 = ""
        synonyms2: list[str] = []
        if wfo_id:
            html = wfo_browser_html_cached(wfo_id, cache=details_cache)
            accepted2, synonyms2 = parse_browser_for_accepted_and_synonyms(html)

        df.at[i, "wfo_accepted_name"] = accepted2 or full_plain
        df.at[i, "wfo_synonyms"] = " | ".join(synonyms2)
        df.at[i, "wfo_synonym_count"] = len(synonyms2)

    save_cache(WFO_MATCH_CACHE_PATH, match_cache)
    save_cache(WFO_DETAILS_CACHE_PATH, details_cache)

    df.to_excel(OUT_XLSX, index=False)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    print(f"Wrote: {OUT_XLSX}")
    print(f"Wrote: {OUT_CSV}")
    print(f"Caches: {WFO_MATCH_CACHE_PATH} , {WFO_DETAILS_CACHE_PATH}")

if __name__ == "__main__":
    main()
