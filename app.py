import os
import math
import sqlite3
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

DB_PATH = os.environ.get("ILLUSTRIA_DB_PATH", "./illustria.db")

app = FastAPI(title="Illustria Weather API")

# Allow your Cloudflare Pages site to call the API from the browser
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten later
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

def connect():
    if not os.path.exists(DB_PATH):
        raise RuntimeError(f"DB file not found at {DB_PATH}")
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def haversine_miles(lat1, lon1, lat2, lon2):
    R = 3958.7613
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlmb/2)**2
    return 2*R*math.asin(math.sqrt(a))

@app.get("/api/health")
def health():
    return {"ok": True}

@app.get("/api/city/{city_id}")
def city(city_id: int):
    with connect() as con:
        row = con.execute("""
        SELECT city_id, name, lat, lon,
               elev_ft_refined AS elev_ft,
               trewartha, biomes,
               dist_to_coast_mi, relief_100mi_ft,
               terrain_type, terrain_flavor
        FROM cities
        WHERE city_id = ?;
        """, (city_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Unknown city_id")
        return dict(row)

@app.get("/api/nearest")
def nearest(lat: float, lon: float):
    with connect() as con:
        cities = con.execute("SELECT city_id, name, lat, lon FROM cities WHERE lat IS NOT NULL AND lon IS NOT NULL;").fetchall()
        best = None
        for r in cities:
            d = haversine_miles(lat, lon, r["lat"], r["lon"])
            if best is None or d < best[0]:
                best = (d, r["city_id"], r["name"], r["lat"], r["lon"])
        if not best:
            raise HTTPException(404, "No cities found")
        return {"distance_mi": best[0], "city_id": best[1], "name": best[2], "lat": best[3], "lon": best[4]}

def slot_index(month: int, day: int, tod: int) -> int:
    return ((month-1)*30 + (day-1))*3 + tod

@app.get("/api/forecast")
def forecast(city_id: int, month: int, day: int, tod: int = 0, days: int = 7):
    start = slot_index(month, day, tod)
    end = start + days*3
    with connect() as con:
        rows = con.execute("""
        SELECT month, day, tod, condition, temp_f, wind_mph, prcp_in,
               cloud_oktas
        FROM weather
        WHERE city_id = ?
          AND slot_index >= ?
          AND slot_index < ?
        ORDER BY slot_index;
        """, (city_id, start, end)).fetchall()
        return {"city_id": city_id, "start": {"month": month, "day": day, "tod": tod}, "days": days, "rows": [dict(r) for r in rows]}
