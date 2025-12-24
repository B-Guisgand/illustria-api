import os
import math
import sqlite3
import zipfile
import tempfile
from pathlib import Path

import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# ============================================================
# CONFIG (Render env vars)
# ============================================================

DB_URL = os.environ.get("ILLUSTRIA_DB_URL")  # should be a direct .zip asset URL
DB_PATH = os.environ.get("ILLUSTRIA_DB_PATH", "/opt/render/project/src/data/illustria.db")

# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(title="Illustria Weather API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten later
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"name": "Illustria API", "ok": True, "health": "/api/health"}


# ============================================================
# DB download/extract (LOW MEMORY)
# ============================================================

def _looks_like_sqlite(p: Path) -> bool:
    try:
        with open(p, "rb") as f:
            header = f.read(16)
        return header[:15] == b"SQLite format 3"
    except Exception:
        return False


def _download_to_file(url: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1MB
                if chunk:
                    f.write(chunk)


def ensure_db_present() -> None:
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

    # Stream download to temp file then extract
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        tmp_zip = td_path / "payload.zip"

        _download_to_file(DB_URL, tmp_zip)

        with zipfile.ZipFile(tmp_zip) as z:
            db_files = [n for n in z.namelist() if n.lower().endswith(".db")]
            if not db_files:
                raise RuntimeError("ZIP did not contain a .db file")

            tmp_db = td_path / "extracted.db"
            with z.open(db_files[0]) as src, open(tmp_db, "wb") as dst:
                 while True:
                     buf = src.read(1024 * 1024)
                     if not buf:
                         break
                     dst.write(buf)

            # Atomic replace so we never leave a half-written DB at DB_PATH
            tmp_db.replace(db_path)


    # Final validation
    if not _looks_like_sqlite(db_path):
        with open(db_path, "rb") as f:
            head = f.read(64)
        raise RuntimeError(f"Extracted file is not SQLite. Head={head!r}")


def connect() -> sqlite3.Connection:
    """
    Open a read-only SQLite connection.
    If the DB on disk is corrupted/truncated, delete it and re-download once.
    """
    def _open() -> sqlite3.Connection:
        uri = f"file:{DB_PATH}?mode=ro"
        con = sqlite3.connect(uri, uri=True, check_same_thread=False)
        con.row_factory = sqlite3.Row
        # Reduce memory use
        con.execute("PRAGMA cache_size = -20000;")  # ~20MB
        con.execute("PRAGMA mmap_size = 0;")
        con.execute("PRAGMA temp_store = MEMORY;")
        return con

    ensure_db_present()

    try:
        return _open()
    except sqlite3.DatabaseError as e:
        msg = str(e).lower()

        # If the DB file is corrupted or truncated, wipe and re-download once.
        if "malformed" in msg or "disk image is malformed" in msg or "file is not a database" in msg:
            try:
                p = Path(DB_PATH)
                if p.exists():
                    p.unlink()
            except Exception:
                pass

            ensure_db_present()
            return _open()

        raise


def table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    rows = con.execute(f"PRAGMA table_info({table});").fetchall()
    return {r["name"] for r in rows}


def require_cols(con: sqlite3.Connection, table: str, cols: list[str]) -> None:
    existing = table_columns(con, table)
    missing = [c for c in cols if c not in existing]
    if missing:
        raise HTTPException(
            status_code=501,
            detail=f"DB is missing columns in '{table}': {missing}. "
                   f"Present columns: {sorted(existing)}"
        )


# ============================================================
# HELPERS
# ============================================================

def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 3958.7613
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = (math.sin(dphi / 2) ** 2) + math.cos(p1) * math.cos(p2) * (math.sin(dlmb / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(a))


def slot_index(month: int, day: int, tod: int) -> int:
    return ((month - 1) * 30 + (day - 1)) * 3 + tod


# ============================================================
# API
# ============================================================

@app.get("/api/health")
def health():
    with connect() as con:
        con.execute("SELECT 1;").fetchone()
    return {"ok": True}


@app.get("/api/city/{city_id}")
def get_city(city_id: int):
    with connect() as con:
        # PATCH: require & return continent/country
        require_cols(con, "cities", ["continent", "country", "svg_x", "svg_y"])

        row = con.execute(
            """
            SELECT city_id, name, lat, lon,
                   svg_x, svg_y
                   elev_ft_refined AS elev_ft,
                   trewartha, biomes,
                   continent, country,
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

@app.get("/api/nearest_svg")
def nearest_city_svg(svg_x: float, svg_y: float):
    with connect() as con:
        require_cols(con, "cities", ["svg_x", "svg_y"])
        rows = con.execute(
            "SELECT city_id, name, svg_x, svg_y FROM cities WHERE svg_x IS NOT NULL AND svg_y IS NOT NULL;"
        ).fetchall()

    best = None
    for r in rows:
        dx = (r["svg_x"] - svg_x)
        dy = (r["svg_y"] - svg_y)
        d2 = dx*dx + dy*dy
        if best is None or d2 < best[0]:
            best = (d2, r)

    if not best:
        raise HTTPException(404, "No cities found")

    _, r = best
    return dict(r)


@app.get("/api/forecast")
def forecast(city_id: int, month: int, day: int, tod: int = 0, days: int = 7):
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


@app.get("/api/cities")
def list_cities(q: str | None = None, limit: int = 2000, offset: int = 0):
    """
    Returns a list of cities for browsing/search.
    If q is provided, does a simple name LIKE match.
    """
    limit = max(1, min(limit, 5000))
    offset = max(0, offset)

    with connect() as con:
        cols = table_columns(con, "cities")

        # Prefer refined elev if present
        elev_expr = (
            "elev_ft_refined AS elev_ft"
            if "elev_ft_refined" in cols
            else ("elev_ft_noisy AS elev_ft" if "elev_ft_noisy" in cols else "NULL AS elev_ft")
        )

        # Optional geo columns
        continent_expr = "continent" if "continent" in cols else "NULL AS continent"
        country_expr = "country" if "country" in cols else "NULL AS country"

        base_sql = f"""
            SELECT city_id, name, lat, lon,
                   {elev_expr},
                   {continent_expr},
                   {country_expr}
            FROM cities
        """

        params: list[object] = []
        if q:
            base_sql += " WHERE name LIKE ? "
            params.append(f"%{q}%")

        base_sql += " ORDER BY city_id LIMIT ? OFFSET ? "
        params.extend([limit, offset])

        rows = con.execute(base_sql, params).fetchall()
        return {"count": len(rows), "rows": [dict(r) for r in rows]}


@app.get("/api/continents")
def list_continents():
    """
    List distinct continent names present in cities table.
    Requires 'continent' column in cities table.
    """
    with connect() as con:
        require_cols(con, "cities", ["continent"])
        rows = con.execute(
            "SELECT continent, COUNT(*) AS city_count "
            "FROM cities "
            "WHERE continent IS NOT NULL AND TRIM(continent) != '' "
            "GROUP BY continent "
            "ORDER BY city_count DESC, continent ASC;"
        ).fetchall()
        return {"count": len(rows), "rows": [dict(r) for r in rows]}


@app.get("/api/countries")
def list_countries(continent: str | None = None):
    """
    List distinct countries (optionally filtered by continent).
    Requires 'country' column (and 'continent' if filtering).
    """
    with connect() as con:
        require_cols(con, "cities", ["country"])
        if continent is not None:
            require_cols(con, "cities", ["continent"])
            rows = con.execute(
                "SELECT country, COUNT(*) AS city_count "
                "FROM cities "
                "WHERE country IS NOT NULL AND TRIM(country) != '' "
                "  AND continent = ? "
                "GROUP BY country "
                "ORDER BY city_count DESC, country ASC;",
                (continent,),
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT country, COUNT(*) AS city_count "
                "FROM cities "
                "WHERE country IS NOT NULL AND TRIM(country) != '' "
                "GROUP BY country "
                "ORDER BY city_count DESC, country ASC;"
            ).fetchall()

        return {"count": len(rows), "rows": [dict(r) for r in rows]}


@app.get("/api/cities/by_continent")
def cities_by_continent(continent: str, limit: int = 5000, offset: int = 0):
    """
    Cities in a continent.
    Requires 'continent' column.
    """
    limit = max(1, min(limit, 5000))
    offset = max(0, offset)

    with connect() as con:
        require_cols(con, "cities", ["continent", "svg_x", "svg_y"])
        rows = con.execute(
            "SELECT city_id, name, lat, lon, svg_x, svg_y "
            "FROM cities "
            "WHERE continent = ? "
            "ORDER BY city_id "
            "LIMIT ? OFFSET ?;",
            (continent, limit, offset),
        ).fetchall()
        return {"continent": continent, "count": len(rows), "rows": [dict(r) for r in rows]}


@app.get("/api/cities/by_country")
def cities_by_country(country: str, limit: int = 5000, offset: int = 0):
    """
    Cities in a country.
    Requires 'country' column.
    """
    limit = max(1, min(limit, 5000))
    offset = max(0, offset)

    with connect() as con:
        require_cols(con, "cities", ["country", "svg_x", "svg_y"])
        rows = con.execute(
            "SELECT city_id, name, lat, lon, svg_x, svg_y "
            "FROM cities "
            "WHERE country = ? "
            "ORDER BY city_id "
            "LIMIT ? OFFSET ?;",
            (country, limit, offset),
        ).fetchall()
        return {"country": country, "count": len(rows), "rows": [dict(r) for r in rows]}
