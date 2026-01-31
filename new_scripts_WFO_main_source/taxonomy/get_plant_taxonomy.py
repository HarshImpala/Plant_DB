"""
wfo_family_genus_enrichment.py
-----------------------------
Reads an input XLSX, uses plant names in column "wfo_accepted_name",
looks up WFO family + genus via World Flora Online,
and writes an output XLSX.

Auto-throttle:
- Starts with a small delay between requests
- If WFO responds with 429/5xx or times out, it increases delay (and adds backoff)
- If things are healthy for a while, it slowly decreases delay again

Edit INPUT_XLSX and OUTPUT_XLSX at the bottom.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import pandas as pd
import requests


MATCHING_REST_URL = "https://list.worldfloraonline.org/matching_rest.php"
SW_DATA_URL = "https://list.worldfloraonline.org/sw_data.php"


# -----------------------------
# Utility helpers
# -----------------------------
def safe_filename(s: str, max_len: int = 120) -> str:
    s = s.strip().replace(os.sep, "_")
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
    if len(s) <= max_len:
        return s
    h = hashlib.sha1(s.encode("utf-8")).hexdigest()[:10]
    return s[: max_len - 11] + "_" + h


def read_json(path: str) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def write_json(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)


# -----------------------------
# Progress bar
# -----------------------------
def print_progress(i: int, total: int, label: str, delay_s: float, bar_width: int = 30) -> None:
    pct = i / total if total else 1.0
    filled = int(bar_width * pct)
    bar = "█" * filled + "░" * (bar_width - filled)
    percent_str = f"{pct * 100:6.2f}%"
    msg = f"[{bar}] {percent_str} | {i}/{total} | delay={delay_s:.2f}s | {label}"
    sys.stdout.write("\r" + msg[:160])
    sys.stdout.flush()
    if i == total:
        print()  # newline at end


# -----------------------------
# Auto-throttle
# -----------------------------
@dataclass
class AutoThrottle:
    """
    Adaptive delay controller:
    - Increase delay on errors/overload signals
    - Decrease delay slowly on sustained success
    """
    delay: float = 0.05          # initial delay between requests
    min_delay: float = 0.00
    max_delay: float = 3.00
    up_mult: float = 1.6         # multiply delay on trouble
    down_mult: float = 0.92      # multiply delay down after success window
    success_window: int = 18     # after this many successful requests, reduce delay a bit
    jitter: float = 0.15         # random jitter proportion (+/-)

    _success_streak: int = 0

    def sleep(self) -> None:
        if self.delay <= 0:
            return
        # jitter helps avoid synchronized hammering
        j = self.delay * self.jitter
        time.sleep(max(0.0, self.delay + random.uniform(-j, j)))

    def on_success(self) -> None:
        self._success_streak += 1
        if self._success_streak >= self.success_window:
            self.delay = max(self.min_delay, self.delay * self.down_mult)
            self._success_streak = 0

    def on_throttle(self) -> None:
        # WFO told us (explicitly or implicitly) to slow down
        self.delay = min(self.max_delay, max(0.05, self.delay) * self.up_mult)
        self._success_streak = 0

    def on_error(self) -> None:
        # general errors: also slow down, but same policy is fine
        self.delay = min(self.max_delay, max(0.05, self.delay) * self.up_mult)
        self._success_streak = 0


# -----------------------------
# WFO parsing helpers
# -----------------------------
def first_literal(obj: dict, key: str) -> Optional[str]:
    v = obj.get(key)
    if isinstance(v, list) and v:
        return v[0].get("value")
    return None


def first_uri(obj: dict, key: str) -> Optional[str]:
    v = obj.get(key)
    if isinstance(v, list) and v:
        return v[0].get("value")
    return None


def rank_from_uri(uri: Optional[str]) -> Optional[str]:
    if not uri:
        return None
    return uri.rstrip("/").split("/")[-1].lower()


def concept_id_from_uri(uri: str) -> str:
    return uri.rstrip("/").split("/")[-1]


# -----------------------------
# HTTP with throttle + retries
# -----------------------------
def request_json(
    session: requests.Session,
    throttle: AutoThrottle,
    url: str,
    params: dict,
    retries: int = 6,
    timeout: int = 40,
) -> dict:
    """
    - Sleeps according to throttle before each attempt
    - On 429/5xx/timeouts, escalates throttle and retries with backoff
    - On success, nudges throttle downward slowly
    """
    last_err = None

    for attempt in range(retries):
        throttle.sleep()
        try:
            r = session.get(url, params=params, timeout=timeout)

            # Explicit overload signals -> throttle hard
            if r.status_code == 429:
                throttle.on_throttle()
                raise RuntimeError("HTTP 429 (rate limited)")

            if r.status_code in (500, 502, 503, 504):
                throttle.on_throttle()
                raise RuntimeError(f"HTTP {r.status_code} (server busy)")

            r.raise_for_status()

            data = r.json()
            throttle.on_success()
            return data

        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            throttle.on_error()
        except Exception as e:
            last_err = e
            # if it's not a throttle-aware status error, still slow down a bit
            throttle.on_error()

        # exponential backoff between retries (in addition to throttle delay)
        backoff = (2 ** attempt) * 0.4 + random.uniform(0, 0.25)
        time.sleep(backoff)

    raise RuntimeError(f"Request failed after retries: {last_err}")


# -----------------------------
# Core WFO logic
# -----------------------------
def match_name_to_wfo_id(
    session: requests.Session,
    throttle: AutoThrottle,
    plant_name: str,
    cache_dir: str,
) -> Optional[str]:

    key = safe_filename(f"match_{plant_name}")
    cache_path = os.path.join(cache_dir, "match", f"{key}.json")

    data = read_json(cache_path)
    if data is None:
        data = request_json(
            session=session,
            throttle=throttle,
            url=MATCHING_REST_URL,
            params={"input_string": plant_name},
            retries=6,
            timeout=40,
        )
        write_json(cache_path, data)

    if isinstance(data.get("match"), dict):
        return data["match"].get("wfo_id")

    candidates = data.get("candidates") or []
    if candidates:
        return candidates[0].get("wfo_id")

    return None


def fetch_sw_graph(
    session: requests.Session,
    throttle: AutoThrottle,
    wfo_id: str,
    cache_dir: str,
) -> dict:

    key = safe_filename(f"sw_{wfo_id}")
    cache_path = os.path.join(cache_dir, "sw", f"{key}.json")

    graph = read_json(cache_path)
    if graph is None:
        graph = request_json(
            session=session,
            throttle=throttle,
            url=SW_DATA_URL,
            params={"format": "json", "wfo": wfo_id},
            retries=6,
            timeout=45,
        )
        write_json(cache_path, graph)

    return graph


def find_family_genus(
    session: requests.Session,
    throttle: AutoThrottle,
    wfo_name_id: str,
    cache_dir: str,
) -> Tuple[Optional[str], Optional[str]]:

    graph = fetch_sw_graph(session, throttle, wfo_name_id, cache_dir)

    name_uri = f"https://list.worldfloraonline.org/{wfo_name_id}"
    name_obj = graph.get(name_uri)
    if not isinstance(name_obj, dict):
        return None, None

    concept_uri = first_uri(
        name_obj,
        "https://list.worldfloraonline.org/terms/currentPreferredUsage",
    )

    family = None
    genus = None

    hops = 0
    while concept_uri and hops < 40:
        hops += 1
        concept_id = concept_id_from_uri(concept_uri)
        c_graph = fetch_sw_graph(session, throttle, concept_id, cache_dir)

        concept_obj = c_graph.get(concept_uri)
        if not isinstance(concept_obj, dict):
            break

        nm_uri = first_uri(
            concept_obj,
            "https://list.worldfloraonline.org/terms/hasName",
        )
        if nm_uri and nm_uri in c_graph:
            name_node = c_graph[nm_uri]
            rank = rank_from_uri(
                first_uri(name_node, "https://list.worldfloraonline.org/terms/rank")
            )
            full_name = first_literal(
                name_node, "https://list.worldfloraonline.org/terms/fullName"
            )

            if rank == "genus" and genus is None:
                genus = full_name
            elif rank == "family" and family is None:
                family = full_name

        concept_uri = first_uri(
            concept_obj,
            "http://purl.org/dc/terms/isPartOf",
        )

        if family and genus:
            break

    return family, genus


# -----------------------------
# Main enrichment routine
# -----------------------------
def main(input_xlsx: str, output_xlsx: str) -> None:
    CACHE_DIR = ".wfo_cache"
    NAME_COL = "wfo_accepted_name"

    df = pd.read_excel(input_xlsx)

    if NAME_COL not in df.columns:
        raise ValueError(f"Missing column '{NAME_COL}'")

    df["wfo_family"] = pd.NA
    df["wfo_genus"] = pd.NA

    session = requests.Session()
    session.headers["User-Agent"] = "wfo-family-genus-script (auto-throttle)"

    throttle = AutoThrottle(
        delay=0.05,
        min_delay=0.00,
        max_delay=3.00,
        up_mult=1.6,
        down_mult=0.92,
        success_window=18,
        jitter=0.15,
    )

    unique_names = df[NAME_COL].dropna().astype(str).unique().tolist()
    total = len(unique_names)

    print(f"Processing {total} unique plant names\n")

    cache: Dict[str, Tuple[Optional[str], Optional[str]]] = {}

    for i, plant in enumerate(unique_names, 1):
        print_progress(i, total, plant, delay_s=throttle.delay)

        try:
            wfo_id = match_name_to_wfo_id(session, throttle, plant, CACHE_DIR)
            if not wfo_id:
                cache[plant] = (None, None)
                continue

            family, genus = find_family_genus(session, throttle, wfo_id, CACHE_DIR)
            cache[plant] = (family, genus)

        except Exception as e:
            # Print on new line so we don't destroy the progress bar line
            print(f"\nERROR for '{plant}': {e}")
            cache[plant] = (None, None)

    df["wfo_family"] = df[NAME_COL].map(lambda x: cache.get(str(x), (None, None))[0])
    df["wfo_genus"] = df[NAME_COL].map(lambda x: cache.get(str(x), (None, None))[1])

    os.makedirs(os.path.dirname(os.path.abspath(output_xlsx)), exist_ok=True)
    df.to_excel(output_xlsx, index=False)

    print(f"\nSaved output → {output_xlsx}")


# -----------------------------
# EDIT ONLY THIS SECTION
# -----------------------------
if __name__ == "__main__":

    INPUT_XLSX = r"C:\Users\aron_\PycharmProjects\obsidian_app\PostgreSQL_DB\new_scripts_WFO_main_source\naming\plants_gbif_matched_plus_wfo_syn_diff.xlsx"
    OUTPUT_XLSX = r"C:\Users\aron_\PycharmProjects\obsidian_app\PostgreSQL_DB\new_scripts_WFO_main_source\taxonomy\plants_gbif_matched_plus_wfo_syn_diff_and_taxonomy.xlsx"

    main(INPUT_XLSX, OUTPUT_XLSX)
