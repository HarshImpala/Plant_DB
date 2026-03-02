"""Lightweight translation helpers for native locations."""
from __future__ import annotations

import json
import os
import urllib.request
from pathlib import Path
from typing import Dict, Tuple


BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
TRANSLATION_CACHE_PATH = DATA_DIR / "translation_cache_hu.json"
TRANSLATION_OVERRIDES_PATH = DATA_DIR / "translation_overrides_hu.json"


def _load_json(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f) or {}
    except (OSError, json.JSONDecodeError):
        return {}


def _save_json(path: Path, payload: Dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _should_skip_translation(token: str) -> bool:
    text = token.strip()
    return len(text) == 2 and text.isalpha() and text.upper() == text


def _libretranslate(text: str) -> str | None:
    api_url = os.environ.get("LIBRETRANSLATE_URL")
    if not api_url:
        return None
    api_key = os.environ.get("LIBRETRANSLATE_API_KEY")
    payload = {
        "q": text,
        "source": "en",
        "target": "hu",
        "format": "text",
    }
    if api_key:
        payload["api_key"] = api_key
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(api_url.rstrip("/") + "/translate", data=data, headers={
        "Content-Type": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8")
            parsed = json.loads(body)
            return parsed.get("translatedText")
    except Exception:
        return None


def translate_token(token: str) -> Tuple[str, bool]:
    """Translate a single token, returning (translated, was_translated)."""
    text = (token or "").strip()
    if not text:
        return "", False
    if _should_skip_translation(text):
        return text, False

    overrides = _load_json(TRANSLATION_OVERRIDES_PATH)
    if text in overrides and overrides[text]:
        return overrides[text], True

    cache = _load_json(TRANSLATION_CACHE_PATH)
    if text in cache and cache[text]:
        return cache[text], True

    translated = _libretranslate(text)
    if translated and translated.strip():
        cache[text] = translated.strip()
        _save_json(TRANSLATION_CACHE_PATH, cache)
        return translated.strip(), True

    return text, False


def translate_pipe_separated(value: str | None) -> Tuple[str | None, bool]:
    """Translate pipe-separated lists; returns (translated_text, any_translated)."""
    if not value:
        return value, False
    tokens = [t.strip() for t in str(value).split("|") if t.strip()]
    translated_tokens = []
    translated_any = False
    for token in tokens:
        translated, did_translate = translate_token(token)
        translated_tokens.append(translated)
        if did_translate and translated.lower() != token.lower():
            translated_any = True
    return " | ".join(translated_tokens), translated_any
