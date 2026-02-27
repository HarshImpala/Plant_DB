# Plant Override Editor

Tool:
- `tools/plant_override_editor.py`

Purpose:
- Override any plant-page field directly in `data/plants.db`.
- Edit relationship fields (`synonyms`, `common_names_en`, `common_names_hu`) in the same JSON payload.

## Run

From `PostgreSQL_DB/website_test`:

```powershell
python tools/plant_override_editor.py
```

Auto rebuild after each save:

```powershell
python tools/plant_override_editor.py --rebuild
```

## Workflow

1. Search/select a plant in the left panel.
2. Edit the JSON payload in the right editor.
3. Click:
- `Save Override` to write to DB.
- `Save + Rebuild` to write and rebuild site.
- `Preview Saved Page` to open the currently built page in browser.
- `Preview Current Changes` to save + rebuild + open the page in browser.

## Notes

- `id` is required and must match the selected plant.
- `created_at` is treated as read-only.
- Empty string values become `NULL`.
- `garden_location` update also syncs normalized location mapping tables used by map pages.
- Relationship lists accept JSON arrays or text separated by `|`, `,`, or newline.

## Shortcuts

- `Ctrl+S`: Save
- `Ctrl+Enter`: Preview current changes (save + rebuild + open)
- `Ctrl+R`: Reload selected plant from DB
