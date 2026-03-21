"""
Route Optimizer — Streamlit App
Combines routing_osv_ver5 (distance fetching) + redistribution_dist_ver6 (optimization)
"""

import math
import io
import os
import time
import random
import sqlite3
import threading
import tempfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import streamlit as st
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ═══════════════════════════════════════════════════════════════════
#  PAGE CONFIG
# ═══════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Route Optimizer",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ═══════════════════════════════════════════════════════════════════
#  CUSTOM CSS
# ═══════════════════════════════════════════════════════════════════
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Nunito:wght@400;600;700;800&display=swap');

    .block-container { max-width: 1200px; }

    .main-title {
        font-family: 'Nunito', sans-serif;
        font-weight: 800;
        font-size: 2.2rem;
        background: linear-gradient(135deg, #1a73e8 0%, #00bcd4 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.2rem;
    }
    .sub-title {
        font-family: 'Nunito', sans-serif;
        color: #666;
        font-size: 0.95rem;
        margin-bottom: 1.5rem;
    }

    .stat-card {
        background: linear-gradient(135deg, #f8f9ff 0%, #e8f4fd 100%);
        border: 1px solid #d0e4f5;
        border-radius: 12px;
        padding: 1rem 1.2rem;
        text-align: center;
    }
    .stat-card h3 {
        font-family: 'JetBrains Mono', monospace;
        font-size: 1.5rem;
        color: #1a73e8;
        margin: 0;
    }
    .stat-card p {
        font-family: 'Nunito', sans-serif;
        color: #555;
        font-size: 0.8rem;
        margin: 0.2rem 0 0 0;
    }

    .status-ok {
        color: #2e7d32;
        background: #e8f5e9;
        padding: 2px 10px;
        border-radius: 6px;
        font-weight: 600;
    }
    .status-fail {
        color: #c62828;
        background: #ffebee;
        padding: 2px 10px;
        border-radius: 6px;
        font-weight: 600;
    }

    div[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0d1b2a 0%, #1b2838 100%);
    }
    div[data-testid="stSidebar"] * {
        color: #e0e0e0 !important;
    }
    div[data-testid="stSidebar"] .stSlider > div > div > div {
        color: #4fc3f7 !important;
    }

    .log-box {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.75rem;
        background: #0d1117;
        color: #58a6ff;
        padding: 1rem;
        border-radius: 10px;
        max-height: 400px;
        overflow-y: auto;
        line-height: 1.6;
    }
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════
#  DISTANCE CACHE (same logic as routing_osv_ver5)
# ═══════════════════════════════════════════════════════════════════
class DistanceCache:
    def __init__(self, db_path: str = None):
        self._mem: dict[tuple, float] = {}
        self._write_lock = threading.Lock()
        self.db_path = db_path
        if db_path and Path(db_path).exists():
            self._load_from_db(db_path)

    def _load_from_db(self, db_path):
        try:
            conn = sqlite3.connect(db_path)
            rows = conn.execute("SELECT lat1, lon1, lat2, lon2, dist_km FROM distances").fetchall()
            conn.close()
            for r in rows:
                self._mem[(r[0], r[1], r[2], r[3])] = r[4]
        except Exception:
            pass

    def init_db(self, db_path):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
        conn.execute("""CREATE TABLE IF NOT EXISTS distances (
            lat1 REAL, lon1 REAL, lat2 REAL, lon2 REAL, dist_km REAL,
            PRIMARY KEY (lat1, lon1, lat2, lon2))""")
        conn.commit()
        conn.close()

    @staticmethod
    def _key(lat1, lon1, lat2, lon2):
        return (round(float(lat1), 6), round(float(lon1), 6),
                round(float(lat2), 6), round(float(lon2), 6))

    def get(self, lat1, lon1, lat2, lon2):
        return self._mem.get(self._key(lat1, lon1, lat2, lon2))

    def put(self, lat1, lon1, lat2, lon2, dist_km):
        self._mem[self._key(lat1, lon1, lat2, lon2)] = dist_km

    def put_batch(self, items):
        with self._write_lock:
            if self.db_path:
                try:
                    conn = sqlite3.connect(self.db_path)
                    conn.execute("PRAGMA journal_mode=WAL")
                    conn.executemany("INSERT OR IGNORE INTO distances VALUES (?,?,?,?,?)", items)
                    conn.commit()
                    conn.close()
                except Exception:
                    pass
            for lat1, lon1, lat2, lon2, d in items:
                self._mem[self._key(lat1, lon1, lat2, lon2)] = d

    def __len__(self):
        return len(self._mem)


# ═══════════════════════════════════════════════════════════════════
#  DISTANCE FUNCTIONS
# ═══════════════════════════════════════════════════════════════════
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1, lon1, lat2, lon2 = map(math.radians, map(float, [lat1, lon1, lat2, lon2]))
    a = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return round(R * 2 * math.asin(math.sqrt(a)), 2)


def osrm_table(coords, osrm_url, retries=3):
    coords_str = ";".join(f"{lon},{lat}" for lon, lat in coords)
    url = f"{osrm_url}/table/v1/driving/{coords_str}"
    params = {"annotations": "distance"}
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=60)
            if r.status_code == 200:
                data = r.json()
                if data.get("code") == "Ok":
                    return [[round(cell / 1000, 2) if cell is not None else None
                             for cell in row] for row in data["distances"]]
            elif r.status_code == 429:
                time.sleep(2 ** attempt)
        except Exception:
            time.sleep(1)
    return None


def get_distance(cache, lat1, lon1, lat2, lon2):
    cached = cache.get(lat1, lon1, lat2, lon2)
    if cached is not None:
        return cached
    fb = round(haversine(lat1, lon1, lat2, lon2) * 1.3, 2)
    cache.put(lat1, lon1, lat2, lon2, fb)
    return fb


# ═══════════════════════════════════════════════════════════════════
#  PREFETCH DISTANCES (from routing_osv_ver5)
# ═══════════════════════════════════════════════════════════════════
def prefetch_distances(cache, df_points, df_dates, osrm_url, batch_size, api_delay,
                       max_workers, log_callback=None):
    """Build distance cache for all needed point pairs using OSRM Table API."""

    def log(msg):
        if log_callback:
            log_callback(msg)

    # Build needed pairs from points + dates
    needed_pairs = set()

    for track in df_points['track'].unique():
        track_pts = df_points[df_points['track'] == track]
        if track_pts.empty:
            continue

        zero_lat = track_pts.iloc[0]['zero_Latitude']
        zero_lon = track_pts.iloc[0]['zero_Longitude']
        if pd.isna(zero_lat) or pd.isna(zero_lon):
            continue

        base = (round(float(zero_lat), 6), round(float(zero_lon), 6))

        # All unique point coords for this track
        point_coords = []
        for _, row in track_pts.iterrows():
            if pd.isna(row['Latitude']) or pd.isna(row['Longitude']):
                continue
            point_coords.append((round(float(row['Latitude']), 6),
                                 round(float(row['Longitude']), 6)))

        # Need base↔point and point↔point pairs
        all_coords = [base] + list(set(point_coords))
        for i, a in enumerate(all_coords):
            for j, b in enumerate(all_coords):
                if i != j:
                    needed_pairs.add((a[0], a[1], b[0], b[1]))

    # Filter: only missing from cache
    missing = [p for p in needed_pairs if cache.get(*p) is None]

    log(f"Пар потрібно: {len(needed_pairs)} | В кеші: {len(needed_pairs) - len(missing)} | Нових: {len(missing)}")

    if not missing:
        log("✅ Всі пари в кеші — API-запити не потрібні!")
        return

    # Collect unique coords from missing pairs
    new_coords = set()
    for lat1, lon1, lat2, lon2 in missing:
        new_coords.add((lat1, lon1))
        new_coords.add((lat2, lon2))

    new_coords_list = list(new_coords)
    log(f"Унікальних координат для запиту: {len(new_coords_list)}")

    # Build batches
    batches = []
    for i in range(0, len(new_coords_list), batch_size):
        batch = new_coords_list[i:i + batch_size]
        if len(batch) >= 2:
            batches.append(batch)

    if not batches and new_coords_list:
        batches.append(new_coords_list)

    log(f"Table API батчів: {len(batches)}")

    def process_batch(batch_coords):
        osrm_coords = [(lon, lat) for lat, lon in batch_coords]
        matrix = osrm_table(osrm_coords, osrm_url)
        items = []
        if matrix is not None:
            for i, (lat1, lon1) in enumerate(batch_coords):
                for j, (lat2, lon2) in enumerate(batch_coords):
                    if i == j:
                        continue
                    dist = matrix[i][j]
                    if dist is not None:
                        items.append((round(lat1, 6), round(lon1, 6),
                                      round(lat2, 6), round(lon2, 6), dist))
        else:
            for i, (lat1, lon1) in enumerate(batch_coords):
                for j, (lat2, lon2) in enumerate(batch_coords):
                    if i == j:
                        continue
                    fb = round(haversine(lat1, lon1, lat2, lon2) * 1.3, 2)
                    items.append((round(lat1, 6), round(lon1, 6),
                                  round(lat2, 6), round(lon2, 6), fb))
        if items:
            cache.put_batch(items)
        time.sleep(api_delay)
        return len(items)

    with ThreadPoolExecutor(max_workers=min(max_workers, 4)) as ex:
        futs = {ex.submit(process_batch, b): idx for idx, b in enumerate(batches)}
        done = 0
        for f in as_completed(futs):
            done += 1
            try:
                f.result()
                if done % 3 == 0 or done == len(batches):
                    log(f"  Table API: {done}/{len(batches)} (кеш: {len(cache)})")
            except Exception as e:
                log(f"  ⚠ Помилка: {e}")

    # Fallback for still-missing pairs via route API
    still_missing = [p for p in missing if cache.get(*p) is None]
    if still_missing:
        log(f"Дозавантаження {len(still_missing)} пар (route API)...")
        session = requests.Session()

        def fetch_one(key):
            lat1, lon1, lat2, lon2 = key
            for attempt in range(3):
                try:
                    r = session.get(
                        f"{osrm_url}/route/v1/driving/{lon1},{lat1};{lon2},{lat2}",
                        params={"overview": "false"}, timeout=30)
                    if r.status_code == 200:
                        data = r.json()
                        if data.get("code") == "Ok" and data.get("routes"):
                            d = round(data["routes"][0]["distance"] / 1000, 2)
                            cache.put_batch([(lat1, lon1, lat2, lon2, d)])
                            return
                    elif r.status_code == 429:
                        time.sleep(2 ** attempt)
                except Exception:
                    time.sleep(0.5)
            fb = round(haversine(lat1, lon1, lat2, lon2) * 1.3, 2)
            cache.put_batch([(lat1, lon1, lat2, lon2, fb)])
            time.sleep(api_delay)

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = [ex.submit(fetch_one, k) for k in still_missing]
            for i, f in enumerate(as_completed(futs), 1):
                f.result()
                if i % 20 == 0 or i == len(still_missing):
                    log(f"  route fallback: {i}/{len(still_missing)}")

    log(f"✅ Prefetch завершено. Кеш: {len(cache)} пар.")


# ═══════════════════════════════════════════════════════════════════
#  OPTIMIZER (from redistribution_dist_ver6)
# ═══════════════════════════════════════════════════════════════════
def nn_order(base, stop_names, coords, cache):
    rem = list(stop_names)
    ordered = []
    cur = base
    while rem:
        nxt = min(rem, key=lambda s: get_distance(cache, cur[0], cur[1],
                                                    coords[s][0], coords[s][1]))
        ordered.append(nxt)
        cur = coords[nxt]
        rem.remove(nxt)
    return ordered


def calc_km(base, stop_names, coords, cache):
    if not stop_names:
        return 0.0
    ordered = nn_order(base, stop_names, coords, cache)
    pts = [base] + [coords[s] for s in ordered] + [base]
    return round(sum(get_distance(cache, pts[i][0], pts[i][1], pts[i + 1][0], pts[i + 1][1])
                     for i in range(len(pts) - 1)), 2)


def build_library(base, coords, cache, max_stops, n_samples, seed):
    all_stops = list(coords.keys())
    n = len(all_stops)
    rng = random.Random(seed)
    library = {frozenset(): 0.0}

    for stop in all_stops:
        fs = frozenset([stop])
        library[fs] = calc_km(base, [stop], coords, cache)

    actual_max = min(max_stops, n)
    generated = 0
    attempts = 0
    while generated < n_samples and attempts < n_samples * 5:
        attempts += 1
        size = rng.randint(1, actual_max)
        stops = rng.sample(all_stops, size)
        fs = frozenset(stops)
        if fs not in library:
            library[fs] = calc_km(base, stops, coords, cache)
            generated += 1

    return library


def scored_fill(w_days, target_km, library, visited_global, params, seed=None):
    tolerance = params['tolerance']
    max_rest_ratio = params['max_rest_ratio']
    coverage_bonus = params['coverage_bonus']
    repeat_penalty = params['repeat_penalty']
    max_iter = params['max_iter']

    lo, hi = target_km * (1 - tolerance), target_km * (1 + tolerance)
    n_days = len(w_days)
    rng = random.Random(seed)

    lib_sorted = sorted(library.items(), key=lambda x: x[1])
    kms = [v for _, v in lib_sorted]
    fsets = [k for k, _ in lib_sorted]
    n_lib = len(lib_sorted)
    max_rest = int(n_days * max_rest_ratio)

    def try_fill(rng_local, visited_g):
        use_count = {}
        rest_count = 0
        chosen = []
        total = 0.0
        local_visited = set(visited_g)

        for i in range(n_days):
            remaining = n_days - i
            ideal = (target_km - total) / max(remaining, 1)

            window = []
            for j in range(n_lib):
                if kms[j] == 0.0:
                    window.append(j)
                elif ideal * 0.2 <= kms[j] <= ideal * 3.0:
                    window.append(j)
            if len(window) <= 1:
                window = list(range(n_lib))

            weights = []
            for j in window:
                km = kms[j]
                fs = fsets[j]
                km_score = 1.0 / (1.0 + abs(km - ideal) / max(abs(ideal), 0.01))
                has_new = bool(fs) and bool(fs - local_visited)
                cov_bonus = coverage_bonus if has_new else 1.0
                rep = use_count.get(j, 0)
                rep_pen = repeat_penalty ** rep
                rest_pen = 1.0
                if km == 0.0:
                    if rest_count >= max_rest:
                        rest_pen = 0.001
                    elif rest_count >= max_rest * 0.7:
                        rest_pen = 0.1
                weights.append(km_score * cov_bonus * rep_pen * rest_pen)

            tw = sum(weights)
            if tw <= 0:
                tw = 1.0
                weights = [1.0 / len(window)] * len(window)
            r = rng_local.random() * tw
            cum = 0.0
            sel = window[0]
            for j, w in zip(window, weights):
                cum += w
                if r <= cum:
                    sel = j
                    break

            chosen.append(sel)
            total += kms[sel]
            use_count[sel] = use_count.get(sel, 0) + 1
            if kms[sel] == 0.0:
                rest_count += 1
            local_visited |= fsets[sel]

        return chosen, total

    def fine_tune(chosen, total):
        for _ in range(max_iter):
            if lo <= total <= hi:
                break
            mid = (lo + hi) / 2
            if total > hi:
                pos = max(range(n_days), key=lambda i: kms[chosen[i]])
                cur = kms[chosen[pos]]
                if cur == 0.0:
                    break
                need = cur - (total - mid) / max(n_days * 0.1, 1)
                if need <= 0:
                    rest_now = sum(1 for c in chosen if kms[c] == 0.0)
                    if rest_now < max_rest:
                        empty_j = next((j for j in range(n_lib) if kms[j] == 0.0), None)
                        if empty_j is not None:
                            total -= cur
                            chosen[pos] = empty_j
                            continue
                    better = [j for j in range(n_lib) if 0 < kms[j] < cur]
                    if not better:
                        break
                    new_j = min(better, key=lambda j: abs(kms[j] - max(need, 0.01)))
                    total += kms[new_j] - cur
                    chosen[pos] = new_j
                else:
                    better = [j for j in range(n_lib) if 0 < kms[j] < cur]
                    if not better:
                        break
                    new_j = min(better, key=lambda j: abs(kms[j] - need))
                    total += kms[new_j] - cur
                    chosen[pos] = new_j
            else:
                pos = min(range(n_days), key=lambda i: kms[chosen[i]])
                cur = kms[chosen[pos]]
                need = cur + (mid - total) / max(n_days * 0.1, 1)
                better = [j for j in range(n_lib) if kms[j] > cur]
                if not better:
                    break
                new_j = min(better, key=lambda j: abs(kms[j] - need))
                total += kms[new_j] - cur
                chosen[pos] = new_j
        return chosen, total

    best_chosen = None
    best_var = -1
    best_total = None
    for _ in range(40):
        tr = random.Random(rng.randint(0, 10 ** 9))
        ch, tot = try_fill(tr, visited_global)
        ch, tot = fine_tune(ch[:], tot)
        var = len(set(ch))
        if lo <= tot <= hi:
            if var > best_var:
                best_var = var
                best_chosen = ch[:]
                best_total = tot
        elif best_chosen is None:
            best_var = var
            best_chosen = ch[:]
            best_total = tot

    day_plan = {}
    new_visited = set(visited_global)
    for day, idx in zip(w_days, best_chosen):
        day_plan[day] = (fsets[idx], kms[idx])
        new_visited |= fsets[idx]

    return day_plan, new_visited


def build_rows(track, date, stop_names, base, coords, cache):
    COL_TRACK = "Трек"
    COL_DATE = "Дата"
    COL_POINT = "Точка"
    COL_KM = "Пробег_км"
    COL_LAT = "Latitude"
    COL_LON = "Longitude"

    if not stop_names:
        return [{COL_TRACK: track, COL_DATE: date.strftime("%d.%m.%Y"),
                 COL_POINT: "— без виїзду —", COL_KM: 0.0,
                 COL_LAT: base[0], COL_LON: base[1]}]

    ordered = nn_order(base, list(stop_names), coords, cache)
    pts = [base] + [coords[s] for s in ordered] + [base]
    names = ["СТАРТ (база)"] + ordered + ["ФІНIШ (база)"]
    rows = []
    for i, (name, (lat, lon)) in enumerate(zip(names, pts)):
        km = 0.0 if i == 0 else round(
            get_distance(cache, pts[i - 1][0], pts[i - 1][1], lat, lon), 2)
        if km == 0.0 and name != "СТАРТ (база)":
            continue
        rows.append({COL_TRACK: track, COL_DATE: date.strftime("%d.%m.%Y"),
                     COL_POINT: name, COL_KM: km,
                     COL_LAT: lat, COL_LON: lon})
    return rows


def extract_pool(df_points_track):
    base = None
    coords = {}
    for _, row in df_points_track.iterrows():
        lat, lon = row['Latitude'], row['Longitude']
        if pd.isna(lat) or pd.isna(lon):
            continue
        name = str(row['FullName']).strip()
        coords[name] = (float(lat), float(lon))

    zero_lat = df_points_track.iloc[0].get('zero_Latitude')
    zero_lon = df_points_track.iloc[0].get('zero_Longitude')
    if not pd.isna(zero_lat) and not pd.isna(zero_lon):
        base = (float(zero_lat), float(zero_lon))
    elif coords:
        lats = [v[0] for v in coords.values()]
        lons = [v[1] for v in coords.values()]
        base = (sum(lats) / len(lats), sum(lons) / len(lons))

    return coords, base


def parse_period(period):
    parts = str(period).strip().replace("/", "-").replace(".", "-").split("-")
    parts = [p.strip() for p in parts if p.strip()]
    if len(parts) != 2:
        raise ValueError(f"Невідомий формат: {period}")
    return (int(parts[0]), int(parts[1])) if len(parts[0]) == 4 else (int(parts[1]), int(parts[0]))


def working_days(year, month):
    return list(pd.bdate_range(
        start=f"{year}-{month:02d}-01",
        end=pd.Timestamp(year, month, 1) + pd.offsets.MonthEnd(0),
        freq="B"))


def write_xlsx_to_bytes(all_rows, summary_rows, tolerance):
    wb = Workbook()
    ws1 = wb.active
    ws1.title = "Маршрути"
    hf = Font(name="Arial", bold=True, color="FFFFFF", size=9)
    hfl = PatternFill("solid", start_color="2F5496")
    thin = Side(style="thin", color="BBBBBB")
    brd = Border(left=thin, right=thin, top=thin, bottom=thin)
    sf = PatternFill("solid", start_color="E2EFDA")
    ef = PatternFill("solid", start_color="FCE4D6")
    rf = PatternFill("solid", start_color="D9E2F3")

    headers = ["Трек", "Дата", "Точка", "Пробег_км", "Latitude", "Longitude"]
    for c, h in enumerate(headers, 1):
        cell = ws1.cell(1, c, h)
        cell.font = hf
        cell.fill = hfl
        cell.alignment = Alignment(horizontal="center")
    for r, row in enumerate(all_rows, 2):
        for c, key in enumerate(headers, 1):
            cell = ws1.cell(r, c, row[key])
            cell.font = Font(name="Arial", size=9)
            cell.border = brd
            cell.alignment = Alignment(horizontal="left" if c < 4 else "center")
            if row["Точка"] == "СТАРТ (база)":
                cell.fill = sf
            elif row["Точка"] == "ФІНIШ (база)":
                cell.fill = ef
            elif row["Точка"] == "— без виїзду —":
                cell.fill = rf
    for i, w in enumerate([38, 12, 65, 11, 13, 13], 1):
        ws1.column_dimensions[get_column_letter(i)].width = w

    ws2 = wb.create_sheet("Зведення")
    sh = ["Трек", "Період", "План, км", "Факт, км", "Відхилення, %", "Статус"]
    for c, h in enumerate(sh, 1):
        cell = ws2.cell(1, c, h)
        cell.font = hf
        cell.fill = hfl
        cell.alignment = Alignment(horizontal="center")
    ok_f = PatternFill("solid", start_color="C6EFCE")
    er_f = PatternFill("solid", start_color="FFC7CE")
    ok_n = Font(name="Arial", size=9, color="006100")
    er_n = Font(name="Arial", size=9, color="9C0006")
    for r, s in enumerate(summary_rows, 2):
        ok = abs(s["dev_pct"]) <= tolerance * 100
        vals = [s["track"], s["period"], s["plan"], round(s["fact"], 1),
                f'{s["dev_pct"]:+.1f}%', "✓ В межах" if ok else "✗ Поза межами"]
        for c, v in enumerate(vals, 1):
            cell = ws2.cell(r, c, v)
            cell.border = brd
            cell.alignment = Alignment(horizontal="center")
            cell.font = (ok_n if ok else er_n) if c >= 5 else Font(name="Arial", size=9)
            if c >= 5:
                cell.fill = ok_f if ok else er_f
    for i, w in enumerate([38, 14, 12, 12, 18, 20], 1):
        ws2.column_dimensions[get_column_letter(i)].width = w

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


# ═══════════════════════════════════════════════════════════════════
#  MAIN OPTIMIZATION PIPELINE
# ═══════════════════════════════════════════════════════════════════
def run_optimization(df_points, df_dates, df_plan, params, log_callback=None):
    def log(msg):
        if log_callback:
            log_callback(msg)

    # Initialize cache
    cache = DistanceCache()
    tmp_db = os.path.join(tempfile.gettempdir(), "route_opt_cache.db")
    cache.init_db(tmp_db)
    log(f"Кеш: {tmp_db}")

    # Step 1: Prefetch distances
    log("\n═══ КРОК 1: Завантаження відстаней (OSRM) ═══")
    prefetch_distances(
        cache, df_points, df_dates,
        osrm_url=params['osrm_url'],
        batch_size=params['batch_size'],
        api_delay=params['api_delay'],
        max_workers=params['max_workers'],
        log_callback=log
    )

    # Step 2: Optimize routes
    log("\n═══ КРОК 2: Оптимізація маршрутів ═══")

    # Filter active plans
    df_plan_active = df_plan[df_plan['План, км'] > 0].drop_duplicates(
        subset=['Трек', 'Період']).copy()
    tracks = df_plan_active['Трек'].unique()
    log(f"Активних пар Трек+Період: {len(df_plan_active)}")

    all_rows_out = []
    summary_rows = []

    for track in tracks:
        log(f"\n{'═' * 50}\nТРЕК: {track}")

        track_pts = df_points[df_points['track'] == track].copy()
        if track_pts.empty:
            log("  ⚠ Немає точок — пропускаємо.")
            continue

        coords, base = extract_pool(track_pts)
        if not coords or base is None:
            log("  ⚠ Немає координат — пропускаємо.")
            continue

        log(f"  Пул точок: {len(coords)}")
        log(f"  База: {base[0]:.4f}, {base[1]:.4f}")

        # Get periods for this track
        track_plans = df_plan_active[df_plan_active['Трек'] == track]
        months_info = []
        for _, row in track_plans.iterrows():
            period = str(row['Період']).strip()
            target = float(str(row['План, км']).replace(",", "."))
            try:
                year, month = parse_period(period)
            except ValueError as e:
                log(f"  ⚠ {e}")
                continue

            # Get dates for this track and this month
            track_dates = df_dates[df_dates['track'] == track].copy()
            track_dates['DateActivity'] = pd.to_datetime(track_dates['DateActivity'])
            track_dates['Date'] = track_dates['DateActivity'].dt.date

            month_dates = track_dates[
                (track_dates['DateActivity'].dt.year == year) &
                (track_dates['DateActivity'].dt.month == month)
            ]['Date'].unique()

            if len(month_dates) > 0:
                w_days = sorted([pd.Timestamp(d) for d in month_dates])
            else:
                w_days = working_days(year, month)

            months_info.append((year, month, target, w_days, period))

        if not months_info:
            continue

        months_info.sort(key=lambda x: (x[0], x[1]))
        log(f"  Місяців: {len(months_info)}, днів: {sum(len(m[3]) for m in months_info)}")

        library = build_library(base, coords, cache,
                                params['max_stops'], params['n_samples'], params['seed'])
        log(f"  Бібліотека: {len(library)} маршрутів")

        visited_global = set()

        for year, month, target, w_days, period in months_info:
            log(f"\n  ── Період: {period} | План: {target} км | Днів: {len(w_days)}")

            day_plan, visited_global = scored_fill(
                w_days, target, library, visited_global, params
            )

            fact_km = sum(calc_km(base, list(fs), coords, cache)
                          for fs, _ in day_plan.values())
            dev_pct = (fact_km - target) / target * 100
            status = "✓" if abs(dev_pct) <= params['tolerance'] * 100 else "✗"
            log(f"  Факт: {fact_km:.1f} км | Відхилення: {dev_pct:+.1f}% {status}")

            covered_now = set()
            for fs, _ in day_plan.values():
                covered_now |= fs
            log(f"  Унікальних точок: {len(covered_now)}/{len(coords)}")

            rest_days = sum(1 for _, (fs, _) in day_plan.items() if not fs)
            log(f"  Днів з виїздом: {len(day_plan) - rest_days}, без виїзду: {rest_days}")

            for date in sorted(day_plan.keys()):
                fs, _ = day_plan[date]
                all_rows_out.extend(
                    build_rows(track, pd.Timestamp(date), fs, base, coords, cache))

            summary_rows.append({
                "track": track, "period": period,
                "plan": target, "fact": fact_km, "dev_pct": dev_pct
            })

        total_covered = len(visited_global)
        total_pts = len(coords)
        log(f"\n  Покрито за всі місяці: {total_covered}/{total_pts}")

    if not all_rows_out:
        log("\n⚠ Немає даних для запису!")
        return None, None, summary_rows

    # Build output
    xlsx_bytes = write_xlsx_to_bytes(all_rows_out, summary_rows, params['tolerance'])

    ok_cnt = sum(1 for s in summary_rows if abs(s["dev_pct"]) <= params['tolerance'] * 100)
    log(f"\n{'═' * 50}")
    log(f"РЕЗУЛЬТАТ: {len(summary_rows)} періодів | ✓ {ok_cnt} | ✗ {len(summary_rows) - ok_cnt}")

    return xlsx_bytes, all_rows_out, summary_rows


# ═══════════════════════════════════════════════════════════════════
#  STREAMLIT UI
# ═══════════════════════════════════════════════════════════════════

def render_header():
    st.markdown('<div class="main-title">🚗 Route Optimizer v6</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-title">Оптимізація маршрутів за планом пробігу • OSRM + Monte Carlo</div>',
                unsafe_allow_html=True)


def render_sidebar():
    with st.sidebar:
        st.markdown("### ⚙️ Параметри")
        st.markdown("---")

        st.markdown("**📊 Оптимізація**")
        tolerance = st.slider("TOLERANCE — допуск від плану",
                              0.01, 0.30, 0.10, 0.01,
                              help="±10% = діапазон допустимого кілометражу")
        max_stops = st.slider("MAX_STOPS — макс. точок/день",
                              1, 15, 5,
                              help="Обмежує кількість точок в одному денному маршруті")
        n_samples = st.slider("N_SAMPLES — розмір бібліотеки",
                              500, 15000, 3000, 500,
                              help="Кількість випадкових маршрутів Monte Carlo")
        seed = st.number_input("SEED — зерно випадковості",
                               value=42, min_value=0, step=1,
                               help="Змініть для іншого варіанту плану")
        max_iter = st.slider("MAX_ITER — ітерації fine-tune",
                             1000, 20000, 8000, 1000,
                             help="Запас ітерацій для коригування км")

        st.markdown("---")
        st.markdown("**🎯 Балансування**")
        coverage_bonus = st.slider("COVERAGE_BONUS — бонус за нові точки",
                                   1.0, 15.0, 3.0, 0.5,
                                   help="1.0=вимк., 3.0=помірний, 10+=агресивний")
        repeat_penalty = st.slider("REPEAT_PENALTY — штраф за повтори",
                                   0.0, 1.0, 0.3, 0.05,
                                   help="0.1=жорсткий, 0.3=помірний, 1.0=вимк.")
        max_rest_ratio = st.slider("MAX_REST_RATIO — ліміт днів без виїзду",
                                   0.0, 0.9, 0.6, 0.05,
                                   help="0.0=завжди їздити, 0.6=до 60% без виїзду")

        st.markdown("---")
        st.markdown("**🌐 OSRM API**")
        osrm_url = st.text_input("OSRM URL",
                                  value="http://router.project-osrm.org",
                                  help="URL OSRM-сервера")
        batch_size = st.slider("Batch size (Table API)",
                               10, 100, 80,
                               help="Макс. координат на один Table-запит")
        api_delay = st.slider("API delay (сек)",
                              0.05, 1.0, 0.15, 0.05,
                              help="Пауза між запитами")
        max_workers = st.slider("Потоки",
                                1, 16, 8,
                                help="Кількість паралельних потоків")

    return {
        'tolerance': tolerance,
        'max_stops': max_stops,
        'n_samples': n_samples,
        'seed': seed,
        'max_iter': max_iter,
        'coverage_bonus': coverage_bonus,
        'repeat_penalty': repeat_penalty,
        'max_rest_ratio': max_rest_ratio,
        'osrm_url': osrm_url.rstrip('/'),
        'batch_size': batch_size,
        'api_delay': api_delay,
        'max_workers': max_workers,
    }


def render_map_editor(df_points):
    """Render map for editing missing coordinates."""
    import folium
    from streamlit_folium import st_folium

    st.markdown("#### 🗺️ Координати точок")

    # Check for missing coords
    missing_point = df_points[df_points['Latitude'].isna() | df_points['Longitude'].isna()]
    missing_base = df_points[df_points['zero_Latitude'].isna() | df_points['zero_Longitude'].isna()]

    has_missing = len(missing_point) > 0 or len(missing_base) > 0

    if has_missing:
        st.warning(f"⚠️ Відсутні координати: {len(missing_point)} точок, {len(missing_base)} баз")

    # Determine center
    valid_pts = df_points.dropna(subset=['Latitude', 'Longitude'])
    if len(valid_pts) > 0:
        center_lat = valid_pts['Latitude'].mean()
        center_lon = valid_pts['Longitude'].mean()
    else:
        center_lat, center_lon = 49.0, 32.0  # Ukraine center

    m = folium.Map(location=[center_lat, center_lon], zoom_start=7,
                   tiles="CartoDB positron")

    # Add existing points
    for track in df_points['track'].unique():
        track_pts = df_points[df_points['track'] == track]
        color = "blue" if track == df_points['track'].unique()[0] else "red"

        # Base marker
        zero_lat = track_pts.iloc[0].get('zero_Latitude')
        zero_lon = track_pts.iloc[0].get('zero_Longitude')
        if not pd.isna(zero_lat) and not pd.isna(zero_lon):
            folium.Marker(
                [zero_lat, zero_lon],
                popup=f"🏠 База: {track[:25]}",
                icon=folium.Icon(color="green", icon="home", prefix="fa"),
            ).add_to(m)

        for _, row in track_pts.iterrows():
            if pd.isna(row['Latitude']) or pd.isna(row['Longitude']):
                continue
            folium.CircleMarker(
                [row['Latitude'], row['Longitude']],
                radius=5, color=color, fill=True, fill_opacity=0.7,
                popup=f"{row['FullName'][:40]}",
            ).add_to(m)

    map_data = st_folium(m, width=None, height=420, returned_objects=["last_clicked"])

    # Handle clicks for missing coordinates
    if has_missing and map_data and map_data.get("last_clicked"):
        clicked = map_data["last_clicked"]
        st.info(f"📍 Клік: {clicked['lat']:.6f}, {clicked['lng']:.6f}")

        if len(missing_point) > 0:
            st.markdown("**Призначити координати для точки:**")
            options = missing_point['FullName'].tolist()
            selected = st.selectbox("Оберіть точку", options, key="assign_point")
            if st.button("✅ Призначити координати точки", key="btn_point"):
                idx = df_points[df_points['FullName'] == selected].index
                df_points.loc[idx, 'Latitude'] = clicked['lat']
                df_points.loc[idx, 'Longitude'] = clicked['lng']
                st.success(f"Координати призначено для: {selected[:40]}")
                st.rerun()

        if len(missing_base) > 0:
            st.markdown("**Призначити координати для бази:**")
            base_tracks = missing_base['track'].unique().tolist()
            selected_track = st.selectbox("Оберіть трек", base_tracks, key="assign_base")
            if st.button("✅ Призначити координати бази", key="btn_base"):
                idx = df_points[df_points['track'] == selected_track].index
                df_points.loc[idx, 'zero_Latitude'] = clicked['lat']
                df_points.loc[idx, 'zero_Longitude'] = clicked['lng']
                st.success(f"Базу призначено для: {selected_track[:30]}")
                st.rerun()

    return df_points


def main():
    render_header()
    params = render_sidebar()

    # File upload
    uploaded = st.file_uploader(
        "📂 Завантажте вхідний файл (.xlsx)",
        type=["xlsx"],
        help="Файл з листами: sh1 (точки), sh2 (дати), sh3 (план пробігу)"
    )

    if uploaded is None:
        st.info("Завантажте файл для початку роботи. Очікуваний формат:\n"
                "- **sh1**: точки (track, FullName, Latitude, Longitude, zero_Latitude, zero_Longitude)\n"
                "- **sh2**: дати виїздів (DateActivity, track)\n"
                "- **sh3**: план пробігу (Трек, Період, План км)")
        return

    # Read data
    try:
        xls = pd.ExcelFile(uploaded)
        if 'sh1' not in xls.sheet_names or 'sh2' not in xls.sheet_names or 'sh3' not in xls.sheet_names:
            st.error("Файл має містити листи: sh1, sh2, sh3")
            return

        df_points = pd.read_excel(xls, sheet_name="sh1")
        df_dates = pd.read_excel(xls, sheet_name="sh2")
        df_plan = pd.read_excel(xls, sheet_name="sh3")
    except Exception as e:
        st.error(f"Помилка читання файлу: {e}")
        return

    # Store in session state for editing
    if 'df_points' not in st.session_state or st.session_state.get('_file_id') != uploaded.name:
        st.session_state.df_points = df_points.copy()
        st.session_state._file_id = uploaded.name

    df_points = st.session_state.df_points

    # Stats
    tracks = df_points['track'].unique()
    total_points = len(df_points)
    total_dates = df_dates['track'].nunique()
    total_periods = len(df_plan)

    cols = st.columns(4)
    with cols[0]:
        st.markdown(f'<div class="stat-card"><h3>{len(tracks)}</h3><p>Треків</p></div>',
                    unsafe_allow_html=True)
    with cols[1]:
        st.markdown(f'<div class="stat-card"><h3>{total_points}</h3><p>Точок</p></div>',
                    unsafe_allow_html=True)
    with cols[2]:
        n_dates = len(pd.to_datetime(df_dates['DateActivity']).dt.date.unique())
        st.markdown(f'<div class="stat-card"><h3>{n_dates}</h3><p>Унікальних дат</p></div>',
                    unsafe_allow_html=True)
    with cols[3]:
        st.markdown(f'<div class="stat-card"><h3>{total_periods}</h3><p>Періодів</p></div>',
                    unsafe_allow_html=True)

    st.markdown("")

    # Tabs for data preview
    tab_map, tab_pts, tab_dates, tab_plan = st.tabs([
        "🗺️ Карта та координати",
        "📍 Точки (sh1)",
        "📅 Дати (sh2)",
        "📊 План (sh3)"
    ])

    with tab_map:
        df_points = render_map_editor(df_points)
        st.session_state.df_points = df_points

    with tab_pts:
        st.markdown("#### Точки відвідування")
        edited_pts = st.data_editor(
            df_points[['track', 'FullName', 'Latitude', 'Longitude',
                        'zero_Latitude', 'zero_Longitude']],
            use_container_width=True,
            num_rows="dynamic",
            key="pts_editor"
        )
        if st.button("💾 Зберегти зміни координат", key="save_pts"):
            st.session_state.df_points.loc[edited_pts.index, 'Latitude'] = edited_pts['Latitude']
            st.session_state.df_points.loc[edited_pts.index, 'Longitude'] = edited_pts['Longitude']
            st.session_state.df_points.loc[edited_pts.index, 'zero_Latitude'] = edited_pts['zero_Latitude']
            st.session_state.df_points.loc[edited_pts.index, 'zero_Longitude'] = edited_pts['zero_Longitude']
            st.success("Координати оновлено!")

    with tab_dates:
        st.markdown("#### Дати виїздів")
        df_dates_show = df_dates.copy()
        df_dates_show['DateActivity'] = pd.to_datetime(df_dates_show['DateActivity'])
        df_dates_show['Дата'] = df_dates_show['DateActivity'].dt.strftime('%Y-%m-%d')
        date_summary = df_dates_show.groupby('track').agg(
            Днів=('Дата', 'nunique'),
            Перша_дата=('DateActivity', 'min'),
            Остання_дата=('DateActivity', 'max'),
            Записів=('DateActivity', 'count')
        ).reset_index()
        date_summary['Перша_дата'] = date_summary['Перша_дата'].dt.strftime('%Y-%m-%d')
        date_summary['Остання_дата'] = date_summary['Остання_дата'].dt.strftime('%Y-%m-%d')
        st.dataframe(date_summary, use_container_width=True)

    with tab_plan:
        st.markdown("#### Плановий пробіг")
        st.dataframe(df_plan, use_container_width=True)

    # Run button
    st.markdown("---")

    # Validation
    missing_coords = (df_points['Latitude'].isna().sum() +
                      df_points['Longitude'].isna().sum() +
                      df_points['zero_Latitude'].isna().sum() +
                      df_points['zero_Longitude'].isna().sum())

    if missing_coords > 0:
        st.warning(f"⚠️ Є {missing_coords} пропущених координат. Визначте їх на карті або в таблиці.")

    col_run, col_info = st.columns([1, 2])
    with col_run:
        run_clicked = st.button("🚀 Запустити оптимізацію", type="primary",
                                use_container_width=True)
    with col_info:
        st.caption(f"Параметри: TOLERANCE={params['tolerance']}, MAX_STOPS={params['max_stops']}, "
                   f"N_SAMPLES={params['n_samples']}, SEED={params['seed']}")

    if run_clicked:
        log_messages = []
        log_container = st.empty()
        progress_bar = st.progress(0, text="Ініціалізація...")

        def log_callback(msg):
            log_messages.append(msg)
            log_container.markdown(
                '<div class="log-box">' + "<br>".join(log_messages[-40:]) + '</div>',
                unsafe_allow_html=True
            )

        with st.spinner("Оптимізація маршрутів..."):
            start_time = time.time()
            progress_bar.progress(10, text="Завантаження відстаней...")

            xlsx_bytes, all_rows, summary_rows = run_optimization(
                st.session_state.df_points, df_dates, df_plan, params,
                log_callback=log_callback
            )

            elapsed = time.time() - start_time
            progress_bar.progress(100, text=f"Готово за {elapsed:.0f} сек!")

        if xlsx_bytes is not None:
            st.session_state.result_xlsx = xlsx_bytes
            st.session_state.summary_rows = summary_rows
            st.session_state.all_rows = all_rows
            st.success(f"✅ Оптимізацію завершено за {elapsed:.1f} сек!")

    # Results display
    if 'result_xlsx' in st.session_state and st.session_state.result_xlsx is not None:
        st.markdown("---")
        st.markdown("### 📋 Результати")

        summary = st.session_state.summary_rows
        if summary:
            tolerance_pct = params['tolerance'] * 100

            # Summary table
            sum_data = []
            for s in summary:
                ok = abs(s["dev_pct"]) <= tolerance_pct
                sum_data.append({
                    "Трек": s["track"],
                    "Період": s["period"],
                    "План, км": f'{s["plan"]:.0f}',
                    "Факт, км": f'{s["fact"]:.1f}',
                    "Відхилення": f'{s["dev_pct"]:+.1f}%',
                    "Статус": "✓" if ok else "✗"
                })

            st.dataframe(pd.DataFrame(sum_data), use_container_width=True)

            ok_cnt = sum(1 for s in summary if abs(s["dev_pct"]) <= tolerance_pct)
            total_cnt = len(summary)

            rcols = st.columns(3)
            with rcols[0]:
                st.markdown(f'<div class="stat-card"><h3>{total_cnt}</h3><p>Періодів</p></div>',
                            unsafe_allow_html=True)
            with rcols[1]:
                st.markdown(f'<div class="stat-card"><h3 style="color:#2e7d32">{ok_cnt} ✓</h3>'
                            f'<p>В межах ±{tolerance_pct:.0f}%</p></div>',
                            unsafe_allow_html=True)
            with rcols[2]:
                fail_cnt = total_cnt - ok_cnt
                color = "#c62828" if fail_cnt > 0 else "#2e7d32"
                st.markdown(f'<div class="stat-card"><h3 style="color:{color}">{fail_cnt} ✗</h3>'
                            f'<p>Поза межами</p></div>',
                            unsafe_allow_html=True)

        st.markdown("")
        st.download_button(
            label="📥 Завантажити v4_optimized.xlsx",
            data=st.session_state.result_xlsx,
            file_name="v4_optimized.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
            use_container_width=True,
        )


if __name__ == "__main__":
    main()
