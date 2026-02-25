# Plant XLSX Importer (Desktop/CLI)

This tool lets you import plant-page data from a single `.xlsx` file, then rebuild the site.

Script path:
- `tools/plant_xlsx_importer_app.py`

## What It Supports

The template includes every plant-page field currently used by the site:

- `input_name` (required, unique key)
- `scientific_name`
- `canonical_name`
- `common_name`
- `common_name_hungarian`
- `family`
- `genus`
- `wfo_id`
- `wfo_url`
- `gbif_usage_key`
- `gbif_url`
- `wikipedia_url_english`
- `wikipedia_url_hungarian`
- `description_english`
- `description_hungarian`
- `description_hungarian_is_translated` (`1`/`0`)
- `native_countries`
- `native_regions`
- `native_confidence`
- `toxicity_info`
- `garden_location`
- `image_filename`
- `image_source`
- `curator_comments`
- `synonyms` (pipe-separated)
- `common_names_en` (pipe-separated)
- `common_names_hu` (pipe-separated)
- `collection_slug` (optional, updates `data/collections.json`)

## Use It

From `PostgreSQL_DB/website_test`:

1. Generate template
```powershell
python tools/plant_xlsx_importer_app.py --template data/plant_import_template.xlsx
```

2. Fill the template and import + rebuild
```powershell
python tools/plant_xlsx_importer_app.py --import-file data/your_file.xlsx
```

3. GUI mode
```powershell
python tools/plant_xlsx_importer_app.py
```

## Build a Windows EXE

1. Install PyInstaller (once):
```powershell
pip install pyinstaller
```

2. Build:
```powershell
pyinstaller --noconfirm --onefile --windowed --name PlantXlsxImporter tools/plant_xlsx_importer_app.py
```

3. EXE output:
- `dist/PlantXlsxImporter.exe`

Run that exe, choose an `.xlsx`, and it will import data and rebuild the site.

