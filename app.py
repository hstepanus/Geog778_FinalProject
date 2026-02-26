import io
import csv
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from db import make_engine, fetch_all
from pipeline import build_metrics
from settings import OUT_SCHEMA

app = FastAPI(title="NOVEC Clean-Energy Adoption Explorer (EV + Solar + Charging)")
engine = make_engine()

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ---------- UI ----------
@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# ---------- Pipeline ----------
@app.post("/api/pipeline/build")
def api_build():
    result = build_metrics(engine)
    return result

# ---------- Map / Data APIs ----------
@app.get("/api/novec/boundary")
def novec_boundary():
    rows, cols = fetch_all(engine, f"""
        SELECT jsonb_build_object(
            'type','FeatureCollection',
            'features', jsonb_agg(
                jsonb_build_object(
                    'type','Feature',
                    'geometry', ST_AsGeoJSON(geom)::jsonb,
                    'properties', jsonb_build_object('id', id)
                )
            )
        ) AS fc
        FROM {OUT_SCHEMA}.novec_union;
    """)
    return JSONResponse(rows[0][0])

@app.get("/api/zips")
def zips_geojson(
    min_score: float = Query(0.0, ge=0.0, le=1.0),
    tech: str = Query("all", pattern="^(all|ev|solar|charging)$"),
    min_value: float = Query(0.0, ge=0.0),
):
    """
    Returns aggregated ZIP polygons with metrics.
    Filtering supports:
      - min_score (0..1)
      - tech: all|ev|solar|charging
      - min_value: threshold applied to chosen tech metric (density per km2)
    """
    tech_col = {
        "all": "adoption_score",
        "ev": "evpts_per_km2",
        "solar": "solar_per_km2",
        "charging": "super_per_km2"
    }[tech]

    sql = f"""
    SELECT jsonb_build_object(
        'type','FeatureCollection',
        'features', COALESCE(jsonb_agg(
            jsonb_build_object(
                'type','Feature',
                'geometry', ST_AsGeoJSON(geom)::jsonb,
                'properties', jsonb_build_object(
                    'zip', zip,
                    'area_km2', area_km2,
                    'solar_customers', solar_customers,
                    'ev_accounts_pts', ev_accounts_pts,
                    'superchargers', superchargers,
                    'ev_by_zip_count', ev_by_zip_count,
                    'solar_per_km2', solar_per_km2,
                    'evpts_per_km2', evpts_per_km2,
                    'super_per_km2', super_per_km2,
                    'adoption_score', adoption_score
                )
            )
        ), '[]'::jsonb)
    ) AS fc
    FROM {OUT_SCHEMA}.zip_energy_metrics
    WHERE adoption_score >= :min_score
      AND ({tech_col} >= :min_value);
    """
    rows, _ = fetch_all(engine, sql, {"min_score": min_score, "min_value": min_value})
    return JSONResponse(rows[0][0])

@app.get("/api/summary")
def summary():
    """
    Returns quick dashboard stats for the current dataset.
    """
    rows, cols = fetch_all(engine, f"""
    SELECT
      COUNT(*) AS zip_count,
      SUM(solar_customers) AS total_solar_customers,
      SUM(ev_accounts_pts) AS total_ev_points,
      SUM(superchargers) AS total_superchargers,
      AVG(adoption_score) AS avg_score
    FROM {OUT_SCHEMA}.zip_energy_metrics;
    """)
    r = dict(zip(cols, rows[0]))
    return JSONResponse(r)

@app.get("/api/export/csv")
def export_csv(
    min_score: float = Query(0.0, ge=0.0, le=1.0),
):
    """
    Export filtered summary table as CSV (no geometry).
    """
    rows, cols = fetch_all(engine, f"""
    SELECT
      zip, area_km2,
      solar_customers, ev_accounts_pts, superchargers, ev_by_zip_count,
      solar_per_km2, evpts_per_km2, super_per_km2,
      adoption_score
    FROM {OUT_SCHEMA}.zip_energy_metrics
    WHERE adoption_score >= :min_score
    ORDER BY adoption_score DESC;
    """, {"min_score": min_score})

    buff = io.StringIO()
    writer = csv.writer(buff)
    writer.writerow(cols)
    for row in rows:
        writer.writerow(list(row))
    buff.seek(0)

    return StreamingResponse(
        iter([buff.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=zip_energy_metrics.csv"}
    )