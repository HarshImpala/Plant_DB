"""
Plant field override editor (GUI).

Purpose:
- Override any plant-page-related field directly in data/plants.db.
- Edit core plant columns plus relationship fields (synonyms/common names).
- Preview plant page in browser with current changes.

Usage:
  python tools/plant_override_editor.py
  python tools/plant_override_editor.py --rebuild
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import subprocess
import sys
import webbrowser
from pathlib import Path

try:
    import tkinter as tk
    from tkinter import messagebox
    from tkinter.scrolledtext import ScrolledText
    from tkinter import ttk
except Exception:
    tk = None
    messagebox = None
    ScrolledText = None
    ttk = None


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "plants.db"
BUILD_SCRIPT = BASE_DIR / "generator" / "build_site.py"
SEARCH_DATA_PATH = BASE_DIR / "output" / "static" / "data" / "search-data.json"
OUTPUT_PLANT_DIR = BASE_DIR / "output" / "plant"

RELATION_KEYS = {"synonyms", "common_names_en", "common_names_hu"}
READ_ONLY_KEYS = {"id", "created_at"}


def normalize_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_pipe_or_list(value: object) -> list[str]:
    if isinstance(value, list):
        raw = value
    else:
        text = normalize_text(value)
        if not text:
            return []
        raw = re.split(r"[|,\n]+", text)
    out = []
    seen = set()
    for item in raw:
        entry = normalize_text(item)
        if not entry:
            continue
        key = entry.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(entry)
    return out


def location_key_from_name(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (name or "").strip().lower()).strip("-")
    if not slug:
        slug = "unknown"
    return f"loc-{slug}"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_location_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS garden_locations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location_key TEXT UNIQUE NOT NULL,
            display_name TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS plant_garden_locations (
            plant_id INTEGER PRIMARY KEY,
            location_id INTEGER NOT NULL,
            FOREIGN KEY (plant_id) REFERENCES plants(id),
            FOREIGN KEY (location_id) REFERENCES garden_locations(id)
        )
        """
    )
    conn.commit()


def list_plants(conn: sqlite3.Connection) -> list[dict]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, input_name, canonical_name, scientific_name, common_name
        FROM plants
        ORDER BY canonical_name, scientific_name, input_name
        """
    )
    items = []
    for row in cur.fetchall():
        d = dict(row)
        label = d["canonical_name"] or d["scientific_name"] or d["input_name"] or f"plant-{d['id']}"
        common = d.get("common_name") or ""
        if common:
            label = f"{label} ({common})"
        d["label"] = label
        items.append(d)
    return items


def get_plant_columns(conn: sqlite3.Connection) -> list[str]:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(plants)")
    return [row[1] for row in cur.fetchall()]


def load_plant_payload(conn: sqlite3.Connection, plant_id: int) -> dict:
    cur = conn.cursor()
    cur.execute("SELECT * FROM plants WHERE id = ?", (plant_id,))
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Plant id {plant_id} not found")
    payload = dict(row)

    cur.execute("SELECT synonym_name FROM plant_synonyms WHERE plant_id = ? ORDER BY synonym_name", (plant_id,))
    payload["synonyms"] = [r[0] for r in cur.fetchall()]

    cur.execute(
        "SELECT common_name FROM plant_common_names WHERE plant_id = ? AND language = 'en' ORDER BY common_name",
        (plant_id,),
    )
    payload["common_names_en"] = [r[0] for r in cur.fetchall()]

    cur.execute(
        "SELECT common_name FROM plant_common_names WHERE plant_id = ? AND language = 'hu' ORDER BY common_name",
        (plant_id,),
    )
    payload["common_names_hu"] = [r[0] for r in cur.fetchall()]
    return payload


def update_garden_location_mapping(conn: sqlite3.Connection, plant_id: int, display_name: str | None) -> None:
    ensure_location_tables(conn)
    cur = conn.cursor()
    if not display_name:
        cur.execute("DELETE FROM plant_garden_locations WHERE plant_id = ?", (plant_id,))
        conn.commit()
        return

    location_key = location_key_from_name(display_name)
    cur.execute(
        """
        INSERT INTO garden_locations (location_key, display_name)
        VALUES (?, ?)
        ON CONFLICT(display_name) DO UPDATE SET location_key = excluded.location_key
        """,
        (location_key, display_name),
    )
    cur.execute("SELECT id FROM garden_locations WHERE display_name = ?", (display_name,))
    loc_id = cur.fetchone()[0]
    cur.execute(
        """
        INSERT INTO plant_garden_locations (plant_id, location_id)
        VALUES (?, ?)
        ON CONFLICT(plant_id) DO UPDATE SET location_id = excluded.location_id
        """,
        (plant_id, loc_id),
    )
    conn.commit()


def save_payload(conn: sqlite3.Connection, payload: dict) -> None:
    if "id" not in payload:
        raise ValueError("Payload must include 'id'")
    plant_id = int(payload["id"])
    columns = set(get_plant_columns(conn))
    cur = conn.cursor()

    updates = {}
    for key, value in payload.items():
        if key in RELATION_KEYS or key in READ_ONLY_KEYS:
            continue
        if key not in columns:
            continue
        updates[key] = None if value == "" else value

    if updates:
        set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
        values = list(updates.values()) + [plant_id]
        cur.execute(
            f"UPDATE plants SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            values,
        )

    synonyms = parse_pipe_or_list(payload.get("synonyms"))
    cur.execute("DELETE FROM plant_synonyms WHERE plant_id = ?", (plant_id,))
    for syn in synonyms:
        cur.execute(
            "INSERT OR IGNORE INTO plant_synonyms (plant_id, synonym_name, source) VALUES (?, ?, 'manual')",
            (plant_id, syn),
        )

    names_en = parse_pipe_or_list(payload.get("common_names_en"))
    names_hu = parse_pipe_or_list(payload.get("common_names_hu"))
    cur.execute("DELETE FROM plant_common_names WHERE plant_id = ? AND language IN ('en', 'hu')", (plant_id,))
    for name in names_en:
        cur.execute(
            "INSERT OR IGNORE INTO plant_common_names (plant_id, common_name, language) VALUES (?, ?, 'en')",
            (plant_id, name),
        )
    for name in names_hu:
        cur.execute(
            "INSERT OR IGNORE INTO plant_common_names (plant_id, common_name, language) VALUES (?, ?, 'hu')",
            (plant_id, name),
        )

    conn.commit()
    if "garden_location" in updates:
        update_garden_location_mapping(conn, plant_id, normalize_text(updates.get("garden_location")))


def run_build() -> None:
    subprocess.run([sys.executable, str(BUILD_SCRIPT)], cwd=str(BASE_DIR), check=True)


def load_slug_map() -> dict[int, str]:
    if not SEARCH_DATA_PATH.exists():
        return {}
    try:
        items = json.loads(SEARCH_DATA_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    mapping = {}
    for item in items:
        pid = item.get("id")
        slug = item.get("slug")
        if isinstance(pid, int) and isinstance(slug, str):
            mapping[pid] = slug
    return mapping


def open_plant_page(plant_id: int) -> Path:
    slug_map = load_slug_map()
    slug = slug_map.get(plant_id)
    if not slug:
        raise FileNotFoundError(
            "Plant slug not found in output search-data. Run a build first to generate preview assets."
        )
    path = OUTPUT_PLANT_DIR / f"{slug}.html"
    if not path.exists():
        raise FileNotFoundError(f"Built plant page not found: {path}")
    webbrowser.open(path.as_uri())
    return path


class OverrideEditorApp:
    def __init__(self, root: tk.Tk, rebuild_after_save: bool = False):
        self.root = root
        self.rebuild_after_save = rebuild_after_save
        self.conn = get_conn()
        self.plants = list_plants(self.conn)
        self.filtered = list(self.plants)
        self.current_plant_id: int | None = None
        self.is_dirty = False

        self._configure_style()
        root.title("Plant Override Editor")
        root.geometry("1280x800")

        wrapper = ttk.Frame(root, padding=8)
        wrapper.pack(fill=tk.BOTH, expand=True)

        title = ttk.Label(wrapper, text="Plant Override Editor", style="Title.TLabel")
        title.pack(anchor="w", pady=(0, 6))

        container = ttk.Panedwindow(wrapper, orient=tk.HORIZONTAL)
        container.pack(fill=tk.BOTH, expand=True)

        left = ttk.Frame(container, padding=6)
        right = ttk.Frame(container, padding=6)
        container.add(left, weight=1)
        container.add(right, weight=3)

        ttk.Label(left, text="Search Plants").pack(anchor="w")
        self.search_var = tk.StringVar()
        self.search_var.trace_add("write", lambda *_: self.refresh_list())
        search_entry = ttk.Entry(left, textvariable=self.search_var)
        search_entry.pack(fill=tk.X, pady=(4, 8))

        self.listbox = tk.Listbox(left, activestyle="none")
        self.listbox.pack(fill=tk.BOTH, expand=True)
        self.listbox.bind("<<ListboxSelect>>", self.on_select_plant)
        self.refresh_list()

        toolbar = ttk.Frame(right)
        toolbar.pack(fill=tk.X, pady=(0, 8))
        ttk.Button(toolbar, text="Reload", command=self.reload_current).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(toolbar, text="Save", command=self.save_current).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(toolbar, text="Save + Rebuild", command=lambda: self.save_current(run_rebuild=True)).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(toolbar, text="Preview Saved Page", command=self.preview_saved_page).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(toolbar, text="Preview Current Changes", command=self.preview_current_changes).pack(side=tk.LEFT)

        hint = ttk.Label(
            right,
            text="Edit JSON payload. Save writes DB overrides. Preview Current Changes runs Save + Build + Open page.",
            style="Hint.TLabel",
        )
        hint.pack(fill=tk.X, pady=(0, 8))

        self.editor = ScrolledText(right, wrap=tk.NONE, font=("Consolas", 10))
        self.editor.pack(fill=tk.BOTH, expand=True)
        self.editor.bind("<<Modified>>", self.on_editor_modified)

        self.status = ttk.Label(right, text="Ready.", style="Status.TLabel")
        self.status.pack(fill=tk.X, pady=(8, 0))

        root.bind("<Control-s>", lambda _e: self.save_current())
        root.bind("<Control-Return>", lambda _e: self.preview_current_changes())
        root.bind("<Control-r>", lambda _e: self.reload_current())

    def _configure_style(self) -> None:
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure("Title.TLabel", font=("Segoe UI", 14, "bold"))
        style.configure("Hint.TLabel", foreground="#4e5d6c")
        style.configure("Status.TLabel", foreground="#2d7a4f")

    def set_status(self, text: str, is_error: bool = False) -> None:
        self.status.configure(text=text)
        self.status.configure(foreground="#a13232" if is_error else "#2d7a4f")
        self.root.update_idletasks()

    def refresh_list(self) -> None:
        q = (self.search_var.get() or "").strip().lower()
        self.filtered = [p for p in self.plants if q in p["label"].lower()]
        self.listbox.delete(0, tk.END)
        for item in self.filtered:
            self.listbox.insert(tk.END, item["label"])

    def _ensure_not_dirty(self) -> bool:
        if not self.is_dirty:
            return True
        proceed = messagebox.askyesno("Unsaved changes", "Discard unsaved changes and continue?")
        return bool(proceed)

    def on_select_plant(self, _event=None) -> None:
        if not self._ensure_not_dirty():
            return
        selection = self.listbox.curselection()
        if not selection:
            return
        idx = selection[0]
        plant = self.filtered[idx]
        self.current_plant_id = int(plant["id"])
        payload = load_plant_payload(self.conn, self.current_plant_id)
        self.editor.delete("1.0", tk.END)
        self.editor.insert(tk.END, json.dumps(payload, ensure_ascii=False, indent=2))
        self.editor.edit_modified(False)
        self.is_dirty = False
        self.set_status(f"Loaded plant id={self.current_plant_id}")

    def on_editor_modified(self, _event=None) -> None:
        if self.editor.edit_modified():
            self.is_dirty = True
            self.editor.edit_modified(False)

    def reload_current(self) -> None:
        if self.current_plant_id is None:
            return
        payload = load_plant_payload(self.conn, self.current_plant_id)
        self.editor.delete("1.0", tk.END)
        self.editor.insert(tk.END, json.dumps(payload, ensure_ascii=False, indent=2))
        self.editor.edit_modified(False)
        self.is_dirty = False
        self.set_status(f"Reloaded plant id={self.current_plant_id}")

    def _parse_editor_payload(self) -> dict:
        if self.current_plant_id is None:
            raise ValueError("No plant selected.")
        raw = self.editor.get("1.0", tk.END).strip()
        payload = json.loads(raw)
        if int(payload.get("id", -1)) != self.current_plant_id:
            raise ValueError("Payload id does not match selected plant.")
        return payload

    def save_current(self, run_rebuild: bool = False) -> None:
        try:
            payload = self._parse_editor_payload()
            save_payload(self.conn, payload)
            self.is_dirty = False
            self.set_status(f"Saved overrides for id={self.current_plant_id}")
            if run_rebuild or self.rebuild_after_save:
                self.set_status("Rebuilding site...")
                run_build()
                self.set_status("Build complete.")
        except Exception as exc:
            messagebox.showerror("Save error", str(exc))
            self.set_status(f"Save failed: {exc}", is_error=True)

    def preview_saved_page(self) -> None:
        try:
            if self.current_plant_id is None:
                raise ValueError("Select a plant first.")
            path = open_plant_page(self.current_plant_id)
            self.set_status(f"Opened preview: {path.name}")
        except Exception as exc:
            messagebox.showerror("Preview error", str(exc))
            self.set_status(f"Preview failed: {exc}", is_error=True)

    def preview_current_changes(self) -> None:
        try:
            payload = self._parse_editor_payload()
            save_payload(self.conn, payload)
            self.is_dirty = False
            self.set_status("Saved. Rebuilding for preview...")
            run_build()
            path = open_plant_page(self.current_plant_id)
            self.set_status(f"Preview opened: {path.name}")
        except Exception as exc:
            messagebox.showerror("Preview error", str(exc))
            self.set_status(f"Preview failed: {exc}", is_error=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Plant override editor")
    parser.add_argument("--rebuild", action="store_true", help="Auto-rebuild site after each save in GUI mode.")
    args = parser.parse_args()

    if tk is None:
        print("Tkinter is not available in this Python environment.")
        return 1
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        return 1

    root = tk.Tk()
    app = OverrideEditorApp(root, rebuild_after_save=args.rebuild)
    root.protocol("WM_DELETE_WINDOW", root.destroy)
    root.mainloop()
    try:
        app.conn.close()
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
