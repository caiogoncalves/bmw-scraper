"""
BMW Car Finder Dashboard Server
--------------------------------
Flask server that reads bmw_data_scored.csv and serves
a rich, filterable dashboard at http://HOST:8081

Usage: /opt/bmw_env/bin/python3 /opt/dashboard_server.py
"""

import os
import math
import pandas as pd
from flask import Flask, jsonify, request, send_from_directory

app = Flask(__name__, static_folder='static', template_folder='templates')

CSV_PATH    = os.getenv("BMW_OUTPUT_CSV", "/opt/bmw_data_scored.csv")
STATIC_DIR  = os.path.dirname(os.path.abspath(__file__))

# ── Load & clean data once at startup ────────────────────────────────────────
print(f"[BMW] Loading {CSV_PATH}…")
df_raw = pd.read_csv(CSV_PATH, low_memory=False)

# Keep useful columns for the UI
KEEP = [
    'vehicleId', 'name', 'model', 'version', 'price', 'year',
    'mileage', 'fuel_type', 'transmission', 'hp', 'dealer_name',
    'color', 'url',
    'price_numeric', 'mileage_numeric', 'built_year',
    'feature_count', 'has_trekhaak', 'is_zwart', 'final_score',
]
df = df_raw[[c for c in KEEP if c in df_raw.columns]].copy()
df['final_score']   = df['final_score'].round(2)
df['built_year']    = df['built_year'].fillna(0).astype(int)
df['has_trekhaak']  = df['has_trekhaak'].fillna(0).astype(int)
df['is_zwart']      = df['is_zwart'].fillna(0).astype(int)
df['feature_count'] = df['feature_count'].fillna(0).astype(int)
df['price_numeric'] = df['price_numeric'].fillna(0)
df['mileage_numeric'] = df['mileage_numeric'].fillna(0)

print(f"[BMW] Loaded {len(df)} cars. Ready!")

# ── API ───────────────────────────────────────────────────────────────────────

@app.route('/api/stats')
def stats():
    return jsonify({
        'total':        int(len(df)),
        'avg_price':    int(df['price_numeric'].replace(0, pd.NA).dropna().mean()),
        'avg_mileage':  int(df['mileage_numeric'].replace(0, pd.NA).dropna().mean()),
        'avg_score':    round(float(df['final_score'].mean()), 1),
        'with_trekhaak': int(df['has_trekhaak'].sum()),
        'zwart':        int(df['is_zwart'].sum()),
        'min_year':     int(df['built_year'].replace(0, pd.NA).dropna().min()),
        'max_year':     int(df['built_year'].replace(0, pd.NA).dropna().max()),
        'min_price':    int(df['price_numeric'].replace(0, pd.NA).dropna().min()),
        'max_price':    int(df['price_numeric'].replace(0, pd.NA).dropna().max()),
    })


@app.route('/api/models')
def models():
    names = sorted(df['name'].dropna().unique().tolist())
    return jsonify(names)


@app.route('/api/cars')
def cars():
    q = request.args

    mask = pd.Series(True, index=df.index)

    # Search
    search = q.get('search', '').strip()
    if search:
        mask &= df['name'].astype(str).str.contains(search, case=False, na=False)

    # Year range
    min_year = q.get('min_year', type=int)
    max_year = q.get('max_year', type=int)
    if min_year:
        mask &= df['built_year'] >= min_year
    if max_year:
        mask &= df['built_year'] <= max_year

    # Price range
    min_price = q.get('min_price', type=float)
    max_price = q.get('max_price', type=float)
    if min_price:
        mask &= df['price_numeric'] >= min_price
    if max_price:
        mask &= df['price_numeric'] <= max_price

    # Mileage max
    max_mileage = q.get('max_mileage', type=float)
    if max_mileage:
        mask &= df['mileage_numeric'] <= max_mileage

    # Trekhaak
    if q.get('trekhaak') == '1':
        mask &= df['has_trekhaak'] == 1

    # Zwart
    if q.get('zwart') == '1':
        mask &= df['is_zwart'] == 1

    # Fuel
    fuel = q.get('fuel', '').strip()
    if fuel:
        mask &= df['fuel_type'].astype(str).str.contains(fuel, case=False, na=False)

    filtered = df[mask].copy()

    # Sort
    sort_by  = q.get('sort', 'final_score')
    sort_dir = q.get('dir', 'desc') == 'asc'
    if sort_by in filtered.columns:
        filtered = filtered.sort_values(sort_by, ascending=sort_dir)

    # Pagination
    page     = max(1, q.get('page', 1, type=int))
    per_page = min(100, q.get('per_page', 20, type=int))
    total    = len(filtered)
    start    = (page - 1) * per_page
    end      = start + per_page

    records = filtered.iloc[start:end].fillna('').to_dict(orient='records')

    return jsonify({
        'total':    total,
        'page':     page,
        'pages':    math.ceil(total / per_page),
        'per_page': per_page,
        'cars':     records,
    })


@app.route('/api/chart/price_distribution')
def chart_price():
    bins   = [0, 20000, 30000, 40000, 50000, 60000, 75000, 100000, 999999]
    labels = ['<20k', '20-30k', '30-40k', '40-50k', '50-60k', '60-75k', '75-100k', '>100k']
    counts = pd.cut(df['price_numeric'].replace(0, pd.NA).dropna(), bins=bins, labels=labels).value_counts().reindex(labels, fill_value=0)
    return jsonify({'labels': labels, 'data': counts.tolist()})


@app.route('/api/chart/score_vs_price')
def chart_scatter():
    sample = df[df['price_numeric'] > 0].nlargest(200, 'final_score')[
        ['name', 'price_numeric', 'final_score', 'has_trekhaak', 'is_zwart']
    ]
    return jsonify(sample.fillna('').to_dict(orient='records'))


@app.route('/api/chart/fuel_breakdown')
def chart_fuel():
    counts = df['fuel_type'].fillna('Desconhecido').value_counts().head(8)
    return jsonify({'labels': counts.index.tolist(), 'data': counts.tolist()})


# ── Static Files ─────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return send_from_directory(STATIC_DIR, 'dashboard.html')


@app.route('/<path:filename>')
def static_files(filename):
    return send_from_directory(STATIC_DIR, filename)


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8081, debug=False)
