"""
Post-build smoke tests for generated static site output.

Checks:
- Internal links resolve to existing files.
- Internal image/script/style asset paths resolve.
- JSON-LD blocks are valid JSON.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, asdict
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse, unquote


BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / "output"
REPORT_PATH = BASE_DIR / "data" / "smoke_test_report.json"


INTERNAL_ASSET_ATTRS = {
    "a": "href",
    "img": "src",
    "script": "src",
    "link": "href",
}


def _is_external_url(value: str) -> bool:
    value = value.strip().lower()
    return (
        value.startswith("http://")
        or value.startswith("https://")
        or value.startswith("mailto:")
        or value.startswith("tel:")
        or value.startswith("javascript:")
        or value.startswith("data:")
        or value.startswith("//")
    )


def _normalize_ref(value: str) -> str:
    parsed = urlparse(value)
    if parsed.fragment:
        value = value.split("#", 1)[0]
    if "?" in value:
        value = value.split("?", 1)[0]
    return unquote(value.strip())


def _looks_dynamic_ref(value: str) -> bool:
    value = value.strip()
    if not value:
        return False
    if re.search(r"\s", value):
        return True
    dynamic_markers = ("{{", "}}", "${", "<%", "%>", '"+', '"+', "'+", "+'", " + ")
    return any(marker in value for marker in dynamic_markers)


def _resolve_target(html_file: Path, ref: str) -> Path:
    normalized = _normalize_ref(ref)
    if not normalized:
        return html_file
    if normalized.startswith("/"):
        return OUTPUT_DIR / normalized.lstrip("/")
    return (html_file.parent / normalized).resolve()


@dataclass
class Finding:
    kind: str
    file: str
    detail: str


class PageParser(HTMLParser):
    def __init__(self, html_file: Path):
        super().__init__(convert_charrefs=True)
        self.html_file = html_file
        self.refs: list[tuple[str, str]] = []
        self.jsonld_blocks: list[str] = []
        self._capture_jsonld = False
        self._jsonld_buffer: list[str] = []

    def handle_starttag(self, tag, attrs):
        attr_map = dict(attrs)
        key = INTERNAL_ASSET_ATTRS.get(tag)
        if key and attr_map.get(key):
            self.refs.append((tag, attr_map[key]))
        if tag == "script" and (attr_map.get("type") or "").strip().lower() == "application/ld+json":
            self._capture_jsonld = True
            self._jsonld_buffer = []

    def handle_data(self, data):
        if self._capture_jsonld:
            self._jsonld_buffer.append(data)

    def handle_endtag(self, tag):
        if tag == "script" and self._capture_jsonld:
            self._capture_jsonld = False
            self.jsonld_blocks.append("".join(self._jsonld_buffer).strip())
            self._jsonld_buffer = []


def check_file_links(html_file: Path) -> list[Finding]:
    findings: list[Finding] = []
    parser = PageParser(html_file)
    parser.feed(html_file.read_text(encoding="utf-8"))

    for tag, ref in parser.refs:
        if not ref.strip() or _is_external_url(ref) or _looks_dynamic_ref(ref):
            continue
        target = _resolve_target(html_file, ref)
        if not target.exists():
            findings.append(
                Finding(
                    kind="broken_reference",
                    file=str(html_file.relative_to(BASE_DIR)),
                    detail=f"{tag} -> {ref}",
                )
            )

    for idx, block in enumerate(parser.jsonld_blocks, start=1):
        if not block:
            findings.append(
                Finding(
                    kind="empty_jsonld",
                    file=str(html_file.relative_to(BASE_DIR)),
                    detail=f"script #{idx} was empty",
                )
            )
            continue
        try:
            json.loads(block)
        except json.JSONDecodeError as exc:
            findings.append(
                Finding(
                    kind="invalid_jsonld",
                    file=str(html_file.relative_to(BASE_DIR)),
                    detail=f"script #{idx}: {exc.msg} at pos {exc.pos}",
                )
            )
    return findings


def run() -> int:
    if not OUTPUT_DIR.exists():
        print(f"ERROR: output directory not found: {OUTPUT_DIR}")
        return 2

    html_files = sorted(OUTPUT_DIR.rglob("*.html"))
    findings: list[Finding] = []
    for html_file in html_files:
        findings.extend(check_file_links(html_file))

    summary = {
        "html_files_checked": len(html_files),
        "finding_count": len(findings),
        "findings": [asdict(finding) for finding in findings],
    }
    REPORT_PATH.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"Smoke test checked {len(html_files)} HTML files")
    print(f"Findings: {len(findings)}")
    if findings:
        for finding in findings[:20]:
            print(f"- {finding.kind}: {finding.file} ({finding.detail})")
        if len(findings) > 20:
            print(f"... and {len(findings) - 20} more")
        print(f"Report written to: {REPORT_PATH}")
        return 1

    print("Smoke test passed with zero findings")
    print(f"Report written to: {REPORT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(run())
