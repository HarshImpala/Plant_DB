import csv
import re
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / 'data' / 'plants.db'
OUT_DIR = BASE_DIR / 'toxicity'


def normalize(text):
    return (text or '').strip().lower()


def classify_toxicity(text):
    t = normalize(text)
    if not t:
        return {
            'overall': 'unknown',
            'humans': 'unknown',
            'cats': 'unknown',
            'dogs': 'unknown',
            'reason': 'no toxicity_info',
        }

    toxic_terms = [
        'toxic', 'poison', 'poisonous', 'fatal', 'harmful', 'dangerous',
        'irritant', 'causes vomiting', 'dermatitis', 'ingestion risk',
    ]
    possible_terms = [
        'may be toxic', 'possibly toxic', 'suspected', 'unclear', 'unknown toxicity',
        'can cause mild', 'mild irritation', 'avoid ingestion', 'use caution',
    ]
    non_toxic_terms = [
        'non-toxic', 'nontoxic', 'not toxic', 'safe for pets', 'edible',
    ]

    def subject_status(subject_patterns):
        subject_hit = any(re.search(p, t) for p in subject_patterns)
        if not subject_hit:
            return 'unknown'
        if any(term in t for term in non_toxic_terms):
            return 'not_toxic'
        if any(term in t for term in toxic_terms):
            return 'toxic'
        if any(term in t for term in possible_terms):
            return 'possibly_toxic'
        return 'possibly_toxic'

    humans = subject_status([r'\bhuman', r'\bpeople', r'\badult', r'\bchild'])
    cats = subject_status([r'\bcat', r'\bfeline'])
    dogs = subject_status([r'\bdog', r'\bcanine'])

    if any(v == 'toxic' for v in (humans, cats, dogs)):
        overall = 'toxic'
    elif any(v == 'possibly_toxic' for v in (humans, cats, dogs)):
        overall = 'possibly_toxic'
    elif any(term in t for term in toxic_terms):
        overall = 'toxic'
    elif any(term in t for term in possible_terms):
        overall = 'possibly_toxic'
    elif any(term in t for term in non_toxic_terms):
        overall = 'not_toxic'
    else:
        overall = 'possibly_toxic'

    return {
        'overall': overall,
        'humans': humans,
        'cats': cats,
        'dogs': dogs,
        'reason': 'keyword classification from toxicity_info',
    }


def load_plants():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute('''
        SELECT id, input_name, canonical_name, scientific_name, common_name, family, toxicity_info
        FROM plants
        ORDER BY canonical_name, scientific_name
    ''')
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def write_csv(path, rows):
    if not rows:
        return
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    plants = load_plants()

    classified = []
    for p in plants:
        c = classify_toxicity(p.get('toxicity_info'))
        classified.append({
            'id': p['id'],
            'canonical_name': p.get('canonical_name') or p.get('scientific_name') or p.get('input_name'),
            'scientific_name': p.get('scientific_name') or '',
            'common_name': p.get('common_name') or '',
            'overall_status': c['overall'],
            'humans_status': c['humans'],
            'cats_status': c['cats'],
            'dogs_status': c['dogs'],
            'toxicity_info': p.get('toxicity_info') or '',
            'classification_reason': c['reason'],
        })

    toxic = [r for r in classified if r['overall_status'] == 'toxic']
    possibly = [r for r in classified if r['overall_status'] == 'possibly_toxic']
    unknown = [r for r in classified if r['overall_status'] == 'unknown']

    write_csv(OUT_DIR / 'toxicity_all_classified.csv', classified)
    write_csv(OUT_DIR / 'toxicity_toxic.csv', toxic)
    write_csv(OUT_DIR / 'toxicity_possibly_toxic.csv', possibly)
    write_csv(OUT_DIR / 'toxicity_unknown.csv', unknown)

    print(f'Total plants: {len(classified)}')
    print(f'Toxic: {len(toxic)}')
    print(f'Possibly toxic: {len(possibly)}')
    print(f'Unknown: {len(unknown)}')
    print(f'Output dir: {OUT_DIR}')


if __name__ == '__main__':
    main()
