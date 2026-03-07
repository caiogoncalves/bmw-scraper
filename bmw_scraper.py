"""
BMW Occasions Scraper & Scorer
-------------------------------
Apenas raspa dados do site BMW NL e calcula scores.
A recomendação via IA é feita separadamente pelo n8n.

Output: bmw_data_scored.csv (no mesmo diretório do script)
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
import random
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Caminho de output — pode ser alterado via variável de ambiente
OUTPUT_CSV = os.getenv("BMW_OUTPUT_CSV", "/opt/bmw_data_scored.csv")


# ── Helper ───────────────────────────────────────────────────────────────────

def _normalize_series(s: pd.Series) -> pd.Series:
    """Min-max normalize; returns 0.5 if constant."""
    lo, hi = s.min(), s.max()
    return 0.5 if hi == lo else (s - lo) / (hi - lo)


# ── Scraper ──────────────────────────────────────────────────────────────────

class BMWScraper:
    BASE_URL        = "https://occasions.bmw.nl/bmw/zoeken"
    BASE_DETAIL_URL = "https://occasions.bmw.nl/bmw/zoeken/resultaten/details/id/"

    DETAIL_SECTIONS = [
        'Uitvoeringen en Pakketten', 'Interieur', 'Entertainment en communicatie',
        'Exterieur', 'Klimaatbeheersing', 'Elektrische voorzieningen',
        'Aandrijving en onderstel', 'Veiligheid',
    ]

    def __init__(self, test_mode: bool = False):
        self.test_mode = test_mode
        self.session   = self._build_session()

    @staticmethod
    def _build_session() -> requests.Session:
        s = requests.Session()
        s.headers.update({
            "User-Agent"      : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept"          : "application/json, text/javascript, */*; q=0.01",
            "Referer"         : "https://occasions.bmw.nl/bmw/zoeken",
            "X-Requested-With": "XMLHttpRequest",
            "Connection"      : "keep-alive",
        })
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=20))
        return s

    @staticmethod
    def _delay(min_s: float = 1.0, max_s: float = 3.0) -> None:
        time.sleep(random.uniform(min_s, max_s))

    def _normalize_vehicle(self, v: dict) -> dict | None:
        if not isinstance(v, dict):
            return None

        raw_model   = v.get("model")
        raw_engine  = v.get("engine")
        raw_chassis = v.get("chassis")
        vid         = v.get("vehicleId")

        if raw_model and raw_model != "N/A":
            final_model   = raw_model
            final_version = raw_engine or "N/A"
        elif raw_engine:
            final_model   = raw_engine
            final_version = raw_chassis if raw_chassis and raw_chassis != "N/A" else raw_engine
        else:
            final_model   = "N/A"
            final_version = "N/A"

        return {
            "vehicleId"   : vid,
            "name"        : v.get("name", "N/A"),
            "model"       : final_model,
            "version"     : final_version,
            "price"       : v.get("price", "N/A"),
            "year"        : v.get("builtDate") or v.get("datePartOne", "N/A"),
            "mileage"     : v.get("mileage", "N/A"),
            "fuel_type"   : v.get("fuel", "N/A"),
            "transmission": v.get("transmission", "N/A"),
            "hp"          : v.get("powerHp", "N/A"),
            "dealer_name" : v.get("dealerName", "N/A"),
            "color"       : v.get("color", "N/A"),
            "url"         : f"{self.BASE_DETAIL_URL}{vid}" if vid else "N/A",
        }

    def scrape_main_listings(self) -> pd.DataFrame:
        print("\n[STATUS] Scraping listing pages…")
        all_cars = []
        seen_ids = set()
        page     = 1

        base_payload = {
            "action"  : "getFiltersVehicles",
            "mode"    : "default",
            "formData": [],
            "sort"    : "builtDate-desc",
        }

        while True:
            print(f"  -> Page {page}…", end="\r")
            payload = {**base_payload, "page": str(page)}

            try:
                resp = self.session.post(self.BASE_URL, json=payload, timeout=15)
                resp.raise_for_status()
                vehicles = resp.json().get('vehicles', {}).get('vehicles', [])
            except Exception as e:
                logger.error(f"Error on page {page}: {e}")
                break

            if not vehicles:
                break

            new_ids = set()
            for v in vehicles:
                car = self._normalize_vehicle(v)
                if car and car['vehicleId'] not in seen_ids:
                    all_cars.append(car)
                    seen_ids.add(car['vehicleId'])
                    new_ids.add(car['vehicleId'])

            if self.test_mode or not new_ids or len(vehicles) < 18:
                break

            page += 1
            self._delay(0.5, 1.5)

        print(f"\n[STATUS] Found {len(all_cars)} cars.")
        return pd.DataFrame(all_cars)

    def _scrape_single_detail(self, url: str, vehicle_id) -> dict | None:
        if url == "N/A":
            return None

        self._delay(1.0, 2.5)

        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'lxml')
        except Exception as e:
            return {"vehicleId": vehicle_id, "error": str(e)}

        details = {"vehicleId": vehicle_id}
        for section in soup.find_all('div', class_='details detailsFullWidth detailsSpecial'):
            title_div = section.find('div', class_='title')
            items_div = section.find('div', class_='items')
            if not (title_div and items_div):
                continue
            ul = items_div.find('ul')
            if ul:
                key          = title_div.get_text(strip=True)
                details[key] = ", ".join(li.get_text(strip=True) for li in ul.find_all('li'))
        return details

    def scrape_details_concurrently(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.test_mode:
            df = df.head(5)
            print("  -> Test Mode: 5 cars only.")

        total = len(df)
        print(f"\n[STATUS] Scraping detail pages for {total} cars…")

        results   = []
        completed = 0

        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {
                pool.submit(self._scrape_single_detail, row.url, row.vehicleId): row.vehicleId
                for row in df.itertuples(index=False)
            }
            for future in as_completed(futures):
                data = future.result()
                if data:
                    results.append(data)
                completed += 1
                if completed % 10 == 0:
                    print(f"  -> {completed}/{total} done…", end="\r")

        print(f"\n[STATUS] Detail scraping finished.")
        return pd.DataFrame(results)


# ── Scoring ───────────────────────────────────────────────────────────────────

FEATURE_COLS = [
    'Uitvoeringen en Pakketten', 'Interieur', 'Entertainment en communicatie',
    'Exterieur', 'Klimaatbeheersing', 'Elektrische voorzieningen',
    'Aandrijving en onderstel', 'Veiligheid',
]

W_PRICE, W_MILEAGE, W_YEAR, W_FEATURES = 0.4, 0.3, 0.1, 0.2
BONUS_TREKHAAK, BONUS_ZWART            = 0.5, 0.2


def calculate_car_scores(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[STATUS] Calculating scores…")
    df = df.copy()

    def _strip_currency(col: pd.Series, remove: str) -> pd.Series:
        return pd.to_numeric(
            col.astype(str).str.replace(remove, '', regex=False)
                           .str.replace('.', '', regex=False)
                           .str.replace(',', '.', regex=False)
                           .str.strip(),
            errors='coerce'
        )

    df['price_numeric']   = _strip_currency(df['price'],   '€')
    df['mileage_numeric'] = _strip_currency(df['mileage'], 'km')
    df['built_year']      = pd.to_numeric(df['year'], errors='coerce')

    df['price_numeric']   = df['price_numeric'].fillna(df['price_numeric'].max())
    df['mileage_numeric'] = df['mileage_numeric'].fillna(df['mileage_numeric'].max())
    df['built_year']      = df['built_year'].fillna(df['built_year'].median())

    df['score_price']   = 1 - _normalize_series(df['price_numeric'])
    df['score_mileage'] = 1 - _normalize_series(df['mileage_numeric'])
    df['score_year']    = _normalize_series(df['built_year'])

    present_cols = [c for c in FEATURE_COLS if c in df.columns]
    if present_cols:
        df['feature_count'] = sum(
            df[c].fillna('').astype(str).str.count(',') + df[c].notna().astype(int)
            for c in present_cols
        )
    else:
        df['feature_count'] = 0

    df['score_features'] = _normalize_series(df['feature_count'])

    ext = df['Exterieur'].fillna('') if 'Exterieur' in df.columns else pd.Series('', index=df.index)
    df['has_trekhaak'] = ext.str.contains('Trekhaak', na=False).astype(int)
    df['is_zwart']     = df['color'].astype(str).str.lower().str.contains('zwart', na=False).astype(int)

    df['base_score'] = (
        df['score_price']    * W_PRICE   +
        df['score_mileage']  * W_MILEAGE +
        df['score_year']     * W_YEAR    +
        df['score_features'] * W_FEATURES
    )
    raw               = df['base_score'] + df['has_trekhaak'] * BONUS_TREKHAAK + df['is_zwart'] * BONUS_ZWART
    df['final_score'] = raw / raw.max() * 100

    print("[STATUS] Scoring complete.")
    return df.sort_values('final_score', ascending=False)


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper  = BMWScraper(test_mode=False)
    df_main  = scraper.scrape_main_listings()

    if df_main.empty:
        print("[STATUS] No data found. Exiting.")
        exit(1)

    df_details = scraper.scrape_details_concurrently(df_main)

    print("\n[STATUS] Merging data…")
    final_df  = pd.merge(df_main, df_details, on="vehicleId", how="left")
    scored_df = calculate_car_scores(final_df)

    scored_df.to_csv(OUTPUT_CSV, index=False)
    print(f"[STATUS] Saved '{OUTPUT_CSV}'. Total: {len(scored_df)} cars.")
    print("[DONE]")
