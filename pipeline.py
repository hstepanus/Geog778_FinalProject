#!/usr/bin/env python3
"""
pipeline.py — NOVEC Clean-Energy Adoption Explorer
Build ZIP-level privacy aggregation + hotspot analysis (Moran's I / LISA / Gi*)

Creates/updates:
  - analysis.novec_union
  - analysis.zip_in_novec
  - analysis.zip_energy_metrics  (final metrics layer)
  - Adds hotspot columns to analysis.zip_energy_metrics and fills them:
      moran_global_i, moran_global_p, moran_global_z
      lisa_i, lisa_p, lisa_z, lisa_q, lisa_cluster
      gistar_z, gistar_p, gistar_cat

Requirements:
  pip install sqlalchemy psycopg2-binary geopandas shapely pyproj numpy scipy libpysal esda
Run:
  python pipeline.py
"""

import os
import numpy as np
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text

from libpysal.weights import Queen
from esda.moran import Moran, Moran_Local
from esda.getisord import G_Local

# -----------------------
# CONFIG — EDIT THESE
# -----------------------
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "postgres")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "postgres")

SRC_SCHEMA = os.getenv("SRC_SCHEMA", "public")
OUT_SCHEMA = os.getenv("OUT_SCHEMA", "analysis")

T_SOLAR = f"{SRC_SCHEMA}.solar_customers"
T_EVPTS = f"{SRC_SCHEMA}.ev_accounts"
T_SUPER = f"{SRC_SCHEMA}.superchargers"
T_ZIPS  = f"{SRC_SCHEMA}.zip_codes"
T_EVZIP = f"{SRC_SCHEMA}.ev_by_zip"
T_NOVEC = f"{SRC_SCHEMA}.novec_service_area"

# Column names (update if your schema differs)
GEOM_COL = "geom"
ZIP_POLY_KEY = os.getenv("ZIP_POLY_KEY", "zip")         # zip_codes key
EVBY_ZIP_KEY = os.getenv("EVBY_ZIP_KEY", "zip")         # ev_by_zip key
EVBY_COUNT_COL = os.getenv("EVBY_COUNT_COL", "ev_count")# ev_by_zip EV total count column

# Projected CRS for area/density + contiguity stability (Northern VA)
ANALYSIS_EPSG = int(os.getenv("ANALYSIS_EPSG", "26918"))  # NAD83 / UTM 18N

# Hotspot settings
HOTSPOT_VALUE_COL = os.getenv("HOTSPOT_VALUE_COL", "adoption_score")  # or evpts_per_km2, solar_per_km2, super_per_km2
ALPHA = float(os.getenv("HOTSPOT_ALPHA", "0.05"))
PERMUTATIONS = int(os.getenv("HOTSPOT_PERMS", "999"))

# -----------------------
# DB Helpers
# -----------------------
def make_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, future=True, pool_pre_ping=True)

def run_sql(engine, sql: str, params=None):
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})

# -----------------------
# Hotspot label helpers
# -----------------------
def _cluster_label_lisa(q: int, p: float, alpha=0.05) -> str:
    """
    LISA quadrant classification (Anselin):
      q=1 HH, q=2 LH, q=3 LL, q=4 HL
    """
    if p is None or np.isnan(p) or p > alpha:
        return "Not Significant"
    return {1: "High-High", 2: "Low-High", 3: "Low-Low", 4: "High-Low"}.get(int(q), "Not Significant")

def _cluster_label_gistar(z: float, p: float, alpha=0.05) -> str:
    if p is None or np.isnan(p) or p > alpha:
        return "Not Significant"
    return "Hot Spot" if z >= 0 else "Cold Spot"

def _ensure_hotspot_columns(engine, table_fq: str):
    run_sql(engine, f"""
    ALTER TABLE {table_fq}
      ADD COLUMN IF NOT EXISTS moran_global_i double precision,
      ADD COLUMN IF NOT EXISTS moran_global_p double precision,
      ADD COLUMN IF NOT EXISTS moran_global_z double precision,

      ADD COLUMN IF NOT EXISTS lisa_i double precision,
      ADD COLUMN IF NOT EXISTS lisa_p double precision,
      ADD COLUMN IF NOT EXISTS lisa_z double precision,
      ADD COLUMN IF NOT EXISTS lisa_q integer,
      ADD COLUMN IF NOT EXISTS lisa_cluster text,

      ADD COLUMN IF NOT EXISTS gistar_z double precision,
      ADD COLUMN IF NOT EXISTS gistar_p double precision,
      ADD COLUMN IF NOT EXISTS gistar_cat text;
    """)

# -----------------------
# 1) Build metrics layer
# -----------------------
def build_zip_energy_metrics(engine):
    run_sql(engine, f"CREATE SCHEMA IF NOT EXISTS {OUT_SCHEMA};")

    # Union NOVEC boundary, transform to analysis EPSG
    run_sql(engine, f"""
    DROP TABLE IF EXISTS {OUT_SCHEMA}.novec_union;
    CREATE TABLE {OUT_SCHEMA}.novec_union AS
    SELECT 1 AS id,
           ST_UnaryUnion(ST_Collect(ST_MakeValid(geom)))::geometry(MultiPolygon, {ANALYSIS_EPSG}) AS geom
    FROM (
        SELECT ST_Transform(
            CASE WHEN ST_SRID({GEOM_COL})=0 THEN ST_SetSRID({GEOM_COL}, 4326) ELSE {GEOM_COL} END,
            {ANALYSIS_EPSG}
        ) AS geom
        FROM {T_NOVEC}
    ) t;

    CREATE INDEX IF NOT EXISTS novec_union_gix ON {OUT_SCHEMA}.novec_union USING GIST (geom);
    """)

    # ZIPs intersecting NOVEC, clip to boundary (privacy aggregation unit)
    run_sql(engine, f"""
    DROP TABLE IF EXISTS {OUT_SCHEMA}.zip_in_novec;
    CREATE TABLE {OUT_SCHEMA}.zip_in_novec AS
    WITH novec AS (SELECT geom FROM {OUT_SCHEMA}.novec_union)
    SELECT
        z.*,
        ST_Intersection(
            ST_MakeValid(ST_Transform(
                CASE WHEN ST_SRID(z.{GEOM_COL})=0 THEN ST_SetSRID(z.{GEOM_COL}, 4326) ELSE z.{GEOM_COL} END,
                {ANALYSIS_EPSG}
            )),
            (SELECT geom FROM novec)
        )::geometry(Polygon, {ANALYSIS_EPSG}) AS geom_clip
    FROM {T_ZIPS} z
    WHERE ST_Intersects(
        ST_Transform(
            CASE WHEN ST_SRID(z.{GEOM_COL})=0 THEN ST_SetSRID(z.{GEOM_COL}, 4326) ELSE z.{GEOM_COL} END,
            {ANALYSIS_EPSG}
        ),
        (SELECT geom FROM novec)
    );

    DELETE FROM {OUT_SCHEMA}.zip_in_novec WHERE geom_clip IS NULL OR ST_IsEmpty(geom_clip);

    CREATE INDEX IF NOT EXISTS zip_in_novec_gix ON {OUT_SCHEMA}.zip_in_novec USING GIST (geom_clip);
    CREATE INDEX IF NOT EXISTS zip_in_novec_zip_idx ON {OUT_SCHEMA}.zip_in_novec (({ZIP_POLY_KEY}::text));
    """)

    # Build ZIP metrics (counts + densities + score)
    run_sql(engine, f"""
    DROP TABLE IF EXISTS {OUT_SCHEMA}.zip_energy_metrics;
    CREATE TABLE {OUT_SCHEMA}.zip_energy_metrics AS
    WITH z AS (
        SELECT
            {ZIP_POLY_KEY}::text AS zip,
            geom_clip AS geom,
            ST_Area(geom_clip) AS area_m2
        FROM {OUT_SCHEMA}.zip_in_novec
    ),
    solar AS (
        SELECT z.zip, COUNT(*)::int AS solar_customers
        FROM z
        LEFT JOIN (
            SELECT ST_Transform(
                CASE WHEN ST_SRID({GEOM_COL})=0 THEN ST_SetSRID({GEOM_COL}, 4326) ELSE {GEOM_COL} END,
                {ANALYSIS_EPSG}
            ) AS geom
            FROM {T_SOLAR}
        ) s ON ST_Contains(z.geom, s.geom)
        GROUP BY z.zip
    ),
    evpts AS (
        SELECT z.zip, COUNT(*)::int AS ev_accounts_pts
        FROM z
        LEFT JOIN (
            SELECT ST_Transform(
                CASE WHEN ST_SRID({GEOM_COL})=0 THEN ST_SetSRID({GEOM_COL}, 4326) ELSE {GEOM_COL} END,
                {ANALYSIS_EPSG}
            ) AS geom
            FROM {T_EVPTS}
        ) e ON ST_Contains(z.geom, e.geom)
        GROUP BY z.zip
    ),
    superc AS (
        SELECT z.zip, COUNT(*)::int AS superchargers
        FROM z
        LEFT JOIN (
            SELECT ST_Transform(
                CASE WHEN ST_SRID({GEOM_COL})=0 THEN ST_SetSRID({GEOM_COL}, 4326) ELSE {GEOM_COL} END,
                {ANALYSIS_EPSG}
            ) AS geom
            FROM {T_SUPER}
        ) c ON ST_Contains(z.geom, c.geom)
        GROUP BY z.zip
    ),
    evzip AS (
        SELECT {EVBY_ZIP_KEY}::text AS zip,
               COALESCE({EVBY_COUNT_COL}, 0)::numeric AS ev_by_zip_count
        FROM {T_EVZIP}
    )
    SELECT
        z.zip,
        z.geom,
        z.area_m2,
        (z.area_m2/1e6) AS area_km2,

        COALESCE(solar.solar_customers,0) AS solar_customers,
        COALESCE(evpts.ev_accounts_pts,0) AS ev_accounts_pts,
        COALESCE(superc.superchargers,0)  AS superchargers,
        COALESCE(evzip.ev_by_zip_count,0) AS ev_by_zip_count,

        COALESCE(solar.solar_customers,0)/NULLIF((z.area_m2/1e6),0) AS solar_per_km2,
        COALESCE(evpts.ev_accounts_pts,0)/NULLIF((z.area_m2/1e6),0) AS evpts_per_km2,
        COALESCE(superc.superchargers,0)/NULLIF((z.area_m2/1e6),0)  AS super_per_km2,

        0::numeric AS adoption_score
    FROM z
    LEFT JOIN solar  ON z.zip=solar.zip
    LEFT JOIN evpts  ON z.zip=evpts.zip
    LEFT JOIN superc ON z.zip=superc.zip
    LEFT JOIN evzip  ON z.zip=evzip.zip
    ;

    CREATE INDEX IF NOT EXISTS zip_energy_metrics_gix ON {OUT_SCHEMA}.zip_energy_metrics USING GIST (geom);
    CREATE INDEX IF NOT EXISTS zip_energy_metrics_zip_idx ON {OUT_SCHEMA}.zip_energy_metrics (zip);
    """)

    # Weighted overlay score using min-max normalization
    run_sql(engine, f"""
    WITH stats AS (
        SELECT
            MIN(evpts_per_km2) AS min_ev, MAX(evpts_per_km2) AS max_ev,
            MIN(solar_per_km2) AS min_sol, MAX(solar_per_km2) AS max_sol,
            MIN(super_per_km2) AS min_sc, MAX(super_per_km2) AS max_sc
        FROM {OUT_SCHEMA}.zip_energy_metrics
    )
    UPDATE {OUT_SCHEMA}.zip_energy_metrics z
    SET adoption_score =
        0.45 * ((z.evpts_per_km2 - s.min_ev) / NULLIF((s.max_ev - s.min_ev),0)) +
        0.35 * ((z.solar_per_km2 - s.min_sol) / NULLIF((s.max_sol - s.min_sol),0)) +
        0.20 * ((z.super_per_km2 - s.min_sc) / NULLIF((s.max_sc - s.min_sc),0))
    FROM stats s;
    """)

# -----------------------
# 2) Hotspot analysis + writeback
# -----------------------
def compute_hotspots_and_writeback(engine):
    table_fq = f"{OUT_SCHEMA}.zip_energy_metrics"
    _ensure_hotspot_columns(engine, table_fq)

    # Pull geometry + value into GeoPandas
    gdf = gpd.read_postgis(
        f"""
        SELECT
          zip::text AS id,
          {HOTSPOT_VALUE_COL}::double precision AS value,
          geom
        FROM {table_fq}
        WHERE geom IS NOT NULL;
        """,
        engine,
        geom_col="geom",
    )

    if gdf.empty:
        raise RuntimeError("No geometries found in zip_energy_metrics to run hotspots.")

    gdf["value"] = gdf["value"].fillna(0.0).astype(float)

    # Queen contiguity weights (polygons)
    w = Queen.from_dataframe(gdf, geom_col="geom", ids=gdf["id"].tolist())

    islands = list(w.islands)
    if islands:
        gdf_use = gdf[~gdf["id"].isin(islands)].copy()
        w_use = Queen.from_dataframe(gdf_use, geom_col="geom", ids=gdf_use["id"].tolist())
    else:
        gdf_use = gdf
        w_use = w

    w_use.transform = "R"
    y = gdf_use["value"].to_numpy()

    # Global Moran's I
    moran_g = Moran(y, w_use, permutations=PERMUTATIONS)

    # Local Moran (LISA)
    lisa = Moran_Local(y, w_use, permutations=PERMUTATIONS)
    lisa_df = pd.DataFrame({
        "id": gdf_use["id"].values,
        "lisa_i": lisa.Is,
        "lisa_p": lisa.p_sim,
        "lisa_z": lisa.z_sim,
        "lisa_q": lisa.q,
    })
    lisa_df["lisa_cluster"] = [
        _cluster_label_lisa(q, p, alpha=ALPHA) for q, p in zip(lisa_df["lisa_q"], lisa_df["lisa_p"])
    ]

    # Getis-Ord Gi*
    gi = G_Local(y, w_use, star=True, permutations=PERMUTATIONS)
    gi_df = pd.DataFrame({
        "id": gdf_use["id"].values,
        "gistar_z": gi.Zs,
        "gistar_p": gi.p_sim,
    })
    gi_df["gistar_cat"] = [
        _cluster_label_gistar(z, p, alpha=ALPHA) for z, p in zip(gi_df["gistar_z"], gi_df["gistar_p"])
    ]

    out = lisa_df.merge(gi_df, on="id", how="left")
    out["moran_global_i"] = float(moran_g.I)
    out["moran_global_p"] = float(moran_g.p_sim)
    out["moran_global_z"] = float(moran_g.z_sim)

    # Add islands back (null stats, labeled)
    if islands:
        island_df = pd.DataFrame({
            "id": islands,
            "lisa_i": np.nan,
            "lisa_p": np.nan,
            "lisa_z": np.nan,
            "lisa_q": None,
            "lisa_cluster": "Island (No Neighbors)",
            "gistar_z": np.nan,
            "gistar_p": np.nan,
            "gistar_cat": "Island (No Neighbors)",
            "moran_global_i": float(moran_g.I),
            "moran_global_p": float(moran_g.p_sim),
            "moran_global_z": float(moran_g.z_sim),
        })
        out = pd.concat([out, island_df], ignore_index=True)

    # Batch UPDATE using VALUES with bound params
    update_sql_template = f"""
    UPDATE {table_fq} t
    SET
      moran_global_i = v.moran_global_i,
      moran_global_p = v.moran_global_p,
      moran_global_z = v.moran_global_z,

      lisa_i = v.lisa_i,
      lisa_p = v.lisa_p,
      lisa_z = v.lisa_z,
      lisa_q = v.lisa_q,
      lisa_cluster = v.lisa_cluster,

      gistar_z = v.gistar_z,
      gistar_p = v.gistar_p,
      gistar_cat = v.gistar_cat
    FROM (
      VALUES
      {{values_block}}
    ) AS v(
      id,
      moran_global_i, moran_global_p, moran_global_z,
      lisa_i, lisa_p, lisa_z, lisa_q, lisa_cluster,
      gistar_z, gistar_p, gistar_cat
    )
    WHERE t.zip::text = v.id;
    """

    records = out.to_dict(orient="records")

    def chunks(lst, n=400):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]

    with engine.begin() as conn:
        for batch in chunks(records, 400):
            params = {}
            rows_sql = []

            for i, r in enumerate(batch):
                def pn(name): return f"{name}_{i}"

                params[pn("id")] = r["id"]

                params[pn("mgi")] = r["moran_global_i"]
                params[pn("mgp")] = r["moran_global_p"]
                params[pn("mgz")] = r["moran_global_z"]

                params[pn("li")] = r["lisa_i"]
                params[pn("lp")] = r["lisa_p"]
                params[pn("lz")] = r["lisa_z"]
                params[pn("lq")] = r["lisa_q"]
                params[pn("lc")] = r["lisa_cluster"]

                params[pn("gz")] = r["gistar_z"]
                params[pn("gp")] = r["gistar_p"]
                params[pn("gc")] = r["gistar_cat"]

                rows_sql.append(
                    f"(:{pn('id')}, :{pn('mgi')}, :{pn('mgp')}, :{pn('mgz')}, "
                    f":{pn('li')}, :{pn('lp')}, :{pn('lz')}, :{pn('lq')}, :{pn('lc')}, "
                    f":{pn('gz')}, :{pn('gp')}, :{pn('gc')})"
                )

            sql = update_sql_template.replace("{values_block}", ",\n".join(rows_sql))
            conn.execute(text(sql), params)

    return {
        "global_morans_i": float(moran_g.I),
        "global_p_sim": float(moran_g.p_sim),
        "global_z_sim": float(moran_g.z_sim),
        "alpha": ALPHA,
        "permutations": PERMUTATIONS,
        "value_col": HOTSPOT_VALUE_COL,
        "islands": islands,
        "n_features": int(len(gdf)),
        "n_used_for_weights": int(len(gdf_use)),
    }

# -----------------------
# Main
# -----------------------
def main():
    engine = make_engine()

    print("[1/2] Building ZIP energy metrics table...")
    build_zip_energy_metrics(engine)
    print("      ✅ metrics built: analysis.zip_energy_metrics")

    print("[2/2] Running hotspot analysis (PySAL) and writing back to PostGIS...")
    hs = compute_hotspots_and_writeback(engine)
    print("      ✅ hotspots updated on analysis.zip_energy_metrics")
    print("      Global Moran's I:", hs["global_morans_i"])
    print("      p_sim:", hs["global_p_sim"], "z_sim:", hs["global_z_sim"])
    if hs["islands"]:
        print("      Islands:", hs["islands"])

    print("\nDONE ✅")

if __name__ == "__main__":
    main()