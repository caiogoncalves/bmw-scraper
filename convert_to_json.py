"""
convert_to_json.py
------------------
Converte bmw_data_scored.csv em data/bmw_data.json
compacto para o dashboard Vercel (client-side).

Rode no LXC depois de cada scraping:
  /opt/bmw_env/bin/python3 /opt/convert_to_json.py
"""
import pandas as pd
import json
import os

CSV_PATH    = os.getenv("BMW_OUTPUT_CSV", "/opt/bmw_data_scored.csv")
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(SCRIPT_DIR, "data", "bmw_data.json")

KEEP_COLS = [
    "vehicleId", "name", "version", "built_year",
    "price", "price_numeric",
    "mileage", "mileage_numeric",
    "fuel_type", "color",
    "has_trekhaak", "is_zwart",
    "feature_count", "final_score",
    "url",
]

print(f"[BMW] Reading {CSV_PATH}…")
df = pd.read_csv(CSV_PATH, low_memory=False)
df = df[[c for c in KEEP_COLS if c in df.columns]].copy()

# Clean up types
df["final_score"]    = df["final_score"].round(2)
df["built_year"]     = df["built_year"].fillna(0).astype(int)
df["has_trekhaak"]   = df["has_trekhaak"].fillna(0).astype(int)
df["is_zwart"]       = df["is_zwart"].fillna(0).astype(int)
df["feature_count"]  = df["feature_count"].fillna(0).astype(int)
df["price_numeric"]  = df["price_numeric"].fillna(0).round(0).astype(int)
df["mileage_numeric"]= df["mileage_numeric"].fillna(0).round(0).astype(int)

# Sort best first
df = df.sort_values("final_score", ascending=False)

os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
records = df.fillna("").to_dict(orient="records")

with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(records, f, separators=(",", ":"), ensure_ascii=False)

print(f"[BMW] Saved {len(records)} cars → {OUTPUT_JSON} ({os.path.getsize(OUTPUT_JSON)//1024}KB)")
