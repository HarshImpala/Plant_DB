"""
Enrich curator_data.csv with ASPCA toxicity information.

Matches plants by genus+species against the ASPCA toxic plant list and
updates the toxicity_info column in curator_data.csv. Safe to re-run —
only overwrites toxicity_info for matched plants; leaves manual entries
for unmatched plants untouched.
"""

import csv
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
CURATOR_CSV = BASE_DIR / "data" / "curator_data.csv"
ASPCA_CSV = (
    Path(__file__).parent.parent.parent
    / "new_scripts_WFO_main_source"
    / "excel_files"
    / "DogsCatsHorses_aspca_toxic_plant_list.csv"
)


def genus_species(name):
    """Return lowercase 'genus species' (first two words) from a plant name."""
    words = name.strip().lower().split()
    # Skip hybrid marker 'x' or '×' as first word
    if len(words) >= 3 and words[0] in ('x', '×'):
        return ' '.join(words[1:3])
    return ' '.join(words[:2]) if len(words) >= 2 else name.strip().lower()


def build_toxicity_string(dog, cat, horse):
    """Build a human-readable toxicity summary from ASPCA values."""
    groups = {'toxic': [], 'non-toxic': [], 'unknown': []}
    for animal, value in [('dogs', dog), ('cats', cat), ('horses', horse)]:
        groups.get(value.strip().lower(), groups['unknown']).append(animal)

    parts = []
    if groups['toxic']:
        parts.append('Toxic to ' + ', '.join(groups['toxic']))
    if groups['non-toxic']:
        parts.append('non-toxic to ' + ', '.join(groups['non-toxic']))
    if groups['unknown']:
        parts.append('unknown for ' + ', '.join(groups['unknown']))

    return '. '.join(parts).capitalize() + '. (Source: ASPCA)'


def load_aspca(path):
    """Load ASPCA CSV into two lookup dicts:
    - species_data: exact 'genus species' key -> toxicity tuple
    - genus_data:   genus key -> toxicity tuple, only for 'genus spp.'/'genus sp.'
                    or genus-only entries (covers whole genus per ASPCA)
    """
    species_data = {}
    genus_data = {}
    with open(path, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            key = row['Scientific_Name'].strip().lower()
            tox = (row['Toxicity_Dog'], row['Toxicity_Cat'], row['Toxicity_Horse'])
            words = key.split()
            if len(words) == 1:
                # genus-only entry (e.g. "spathiphyllum") — covers whole genus
                genus_data.setdefault(words[0], tox)
            elif len(words) >= 2 and words[1] in ('spp.', 'sp.', 'spp', 'sp'):
                # "genus spp." entry — explicitly covers all species
                genus_data.setdefault(words[0], tox)
            else:
                species_data.setdefault(key, tox)
    return species_data, genus_data


def main():
    species_data, genus_data = load_aspca(ASPCA_CSV)
    print(f"Loaded {len(species_data)} species-level and {len(genus_data)} genus-level ASPCA entries")

    with open(CURATOR_CSV, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
        fieldnames = list(rows[0].keys()) if rows else []

    exact, genus_match, skipped = 0, 0, 0
    for row in rows:
        key = genus_species(row['input_name'])
        genus = key.split()[0] if key.split() else key

        if key in species_data:
            dog, cat, horse = species_data[key]
            row['toxicity_info'] = build_toxicity_string(dog, cat, horse)
            exact += 1
        elif genus in genus_data:
            dog, cat, horse = genus_data[genus]
            row['toxicity_info'] = build_toxicity_string(dog, cat, horse) \
                .replace('(Source: ASPCA)', '(Source: ASPCA, genus-level)')
            genus_match += 1
        else:
            skipped += 1

    with open(CURATOR_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Exact species match: {exact}")
    print(f"Genus-level match:   {genus_match}")
    print(f"No match:            {skipped} (toxicity_info left as-is)")


if __name__ == '__main__':
    main()
