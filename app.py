import tempfile
import os
import math
import sqlite3
import zipfile
import io
import re
from pathlib import Path

import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# ============================================================
# CONFIG (set via Render environment variables)
# ============================================================

DB_URL = os.environ.get("ILLUSTRIA_DB_URL")   # Google Drive direct-download ZIP
DB_PATH = os.environ.get(
    "ILLUSTRIA_DB_PATH",
    "/opt/render/project/src/data/illustria.db"
)

# ============================================================
# FASTAPI APP (THIS LINE FIXES YOUR ERROR)
# ============================================================

app = FastAPI(title="Illustria Weather API")

# Allow frontend JS (Cloudflare Pages) to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten later
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ============================================================
# DATABASE Download to file
# ============================================================

def _download_to_file(url: str, out_path: Path) -> None:
    """
    Stream download to disk (low memory).
    Works for GitHub release asset URLs (no HTML interstitial).
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                if chunk:
                    f.write(chunk)


def _looks_like_sqlite(p: Path) -> bool:
    try:
        with open(p, "rb") as f:
            header = f.read(16)
        return header[:15] == b"SQLite format 3"
    except Exception:
        return False


def ensure_db_present():
    if not DB_URL:
        raise RuntimeError("ILLUSTRIA_DB_URL environment variable is not set")

    db_path = Path(DB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # If cached file exists but isn't SQLite, delete it
    if db_path.exists() and not _looks_like_sqlite(db_path):
        db_path.unlink()

    # If valid DB exists, keep it
    if db_path.exists() and _looks_like_sqlite(db_path) and db_path.stat().st_size > 1_000_000:
        return

    # Stream download to a temp file on disk
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        tmp = td / ("payload.zip" if DB_URL.lower().endswith(".zip") else "payload.bin")

        _download_to_file(DB_URL, tmp)

        if DB_URL.lower().endswith(".zip"):
            with zipfile.ZipFile(tmp) as z:
                db_files = [n for n in z.namelist() if n.lower().endswith(".db")]
                if not db_files:
                    raise RuntimeError("ZIP did not contain a .db file")

                # Extract DB by streaming from zip member to destination file
                with z.open(db_files[0]) as src, open(db_path, "wb") as dst:
                    while True:
                        buf = src.read(1024 * 1024)  # 1MB
                        if not buf:
                            break
                        dst.write(buf)
        else:
            # payload is the db
            tmp.replace(db_path)

    # Final validation
    if not _looks_like_sqlite(db_path):
        with open(db_path, "rb") as f:
            head = f.read(64)
        raise RuntimeError(f"Downloaded file is not SQLite. Head={head!r}")

 def connect():
    ensure_db_present()

    # Open read-only to avoid journaling and reduce overhead
    uri = f"file:{DB_PATH}?mode=ro"
    con = sqlite3.connect(uri, uri=True, check_same_thread=False)
    con.row_factory = sqlite3.Row

    # Reduce SQLite memory use
    con.execute("PRAGMA cache_size = -20000;")  # ~20MB cache (negative = KB)
    con.execute("PRAGMA temp_store = MEMORY;")
    con.execute("PRAGMA mmap_size = 0;")        # disable mmap (can spike memory)
    return con

# ============================================================
# HELPERS
# ============================================================

def haversine_miles(lat1, lon1, lat2, lon2):
    R = 3958.7613
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    )
    return 2 * R * math.asin(math.sqrt(a))


def slot_index(month: int, day: int, tod: int) -> int:
    return ((month - 1) * 30 + (day - 1)) * 3 + tod

# ============================================================
# API ENDPOINTS
# ============================================================

@app.get("/")
def root():
    return {"name": "Illustria API", "ok": True, "health": "/api/health"}

@app.get("/api/dbinfo")
def dbinfo():
    with connect() as con:
        tables = [r["name"] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
        ).fetchall()]
        out = {"tables": tables, "columns": {}}
        for t in tables:
            cols = [r["name"] for r in con.execute(f"PRAGMA table_info({t});").fetchall()]
            out["columns"][t] = cols
        return out

@app.get("/api/health")
def health():
    """
    Sanity check: confirms DB is present and readable.
    """
    with connect() as con:
        con.execute("SELECT 1;").fetchone()
    return {"ok": True}


@app.get("/api/city/{city_id}")
def get_city(city_id: int):
    with connect() as con:
        row = con.execute(
            """
            SELECT city_id, name, lat, lon,
                   elev_ft_refined AS elev_ft,
                   trewartha, biomes,
                   dist_to_coast_mi, relief_100mi_ft,
                   terrain_type, terrain_flavor
            FROM cities
            WHERE city_id = ?;
            """,
            (city_id,),
        ).fetchone()

        if not row:
            raise HTTPException(404, "City not found")

        return dict(row)


@app.get("/api/nearest")
def nearest_city(lat: float, lon: float):
    with connect() as con:
        rows = con.execute(
            "SELECT city_id, name, lat, lon FROM cities WHERE lat IS NOT NULL AND lon IS NOT NULL;"
        ).fetchall()

        best = None
        for r in rows:
            d = haversine_miles(lat, lon, r["lat"], r["lon"])
            if best is None or d < best[0]:
                best = (d, r)

        if not best:
            raise HTTPException(404, "No cities found")

        d, r = best
        return {
            "city_id": r["city_id"],
            "name": r["name"],
            "lat": r["lat"],
            "lon": r["lon"],
            "distance_mi": d,
        }


@app.get("/api/forecast")
def forecast(
    city_id: int,
    month: int,
    day: int,
    tod: int = 0,
    days: int = 7,
):
    start = slot_index(month, day, tod)
    end = start + days * 3

    with connect() as con:
        rows = con.execute(
            """
            SELECT month, day, tod,
                   condition, temp_f, wind_mph, prcp_in, cloud_oktas
            FROM weather
            WHERE city_id = ?
              AND slot_index >= ?
              AND slot_index < ?
            ORDER BY slot_index;
            """,
            (city_id, start, end),
        ).fetchall()

        return {
            "city_id": city_id,
            "start": {"month": month, "day": day, "tod": tod},
            "days": days,
            "rows": [dict(r) for r in rows],
        }
