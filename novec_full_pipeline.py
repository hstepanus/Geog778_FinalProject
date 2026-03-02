"""
NOVEC Clean-Energy Adoption Explorer
Full Data → PostGIS → ZIP Aggregation → Hotspot Analysis Pipeline

Uses uploaded files:
  - Active EV accounts 11-17-23.csv
  - Full List of Solar Customers.csv
  - FullListExport.csv
  - va_ev_by_zip.xlsx
"""

import os
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Point
from sqlalchemy import create_engine, text

from libpysal.weights import Queen
from esda.moran import Moran, Moran_Local
from esda.getisord import G_Local

# ---------------------------------------------------
# CONFIG — UPDATE ONLY IF NEEDED
# ---------------------------------------------------
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "postgres"

ANALYSIS_EPSG = 26918   # UTM 18N (Northern VA)
OUT_SCHEMA = "analysis"
SRC_SCHEMA = "public"

# Input file paths (your uploaded files)
EV_FILE = "/mnt/data/Active EV accounts 11-17-23.csv"
SOLAR_FILE = "/mnt/data/Full List of Solar Customers.csv"
CHARGER_FILE = "/mnt/data/FullListExport.csv"
EV_ZIP_FILE = "/mnt/data/va_ev_by_zip.xlsx"

# ---------------------------------------------------
# DATABASE CONNECTION
# ---------------------------------------------------
def make_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)

engine = make_engine()

# ---------------------------------------------------
# 1️⃣ LOAD CSV/XLSX INTO POSTGIS
# ---------------------------------------------------
def load_point_csv_to_postgis(csv_path, table_name):
    df = pd.read_csv(csv_path)

    # Try common lat/long field names
    lat_cols = [c for c in df.columns if "lat" in c.lower()]
    lon_cols = [c for c in df.columns if "lon" in c.lower() or "lng" in c.lower()]

    if not lat_cols or not lon_cols:
        raise ValueError(f"No lat/lon columns found in {csv_path}")

    lat_col = lat_cols[0]
    lon_col = lon_cols[0]

    df = df.dropna(subset=[lat_col, lon_col])
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
        crs="EPSG:4326"
    )

    gdf = gdf.to_crs(epsg=ANALYSIS_EPSG)

    gdf.to_postgis(
        table_name,
        engine,
        schema=SRC_SCHEMA,
        if_exists="replace",
        index=False
    )

    print(f"Loaded {table_name} to PostGIS")


def load_excel_to_postgis(xlsx_path, table_name):
    df = pd.read_excel(xlsx_path)
    df.to_sql(table_name, engine, schema=SRC_SCHEMA, if_exists="replace", index=False)
    print(f"Loaded {table_name} to PostGIS")


print("Loading EV accounts...")
load_point_csv_to_postgis(EV_FILE, "ev_accounts")

print("Loading Solar customers...")
load_point_csv_to_postgis(SOLAR_FILE, "solar_customers")

print("Loading Chargers...")
load_point_csv_to_postgis(CHARGER_FILE, "superchargers")

print("Loading EV by ZIP...")
load_excel_to_postgis(EV_ZIP_FILE, "ev_by_zip")

# ---------------------------------------------------
# 2️⃣ BUILD ZIP AGGREGATION
# ---------------------------------------------------
def build_zip_metrics():
    with engine.begin() as conn:

        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {OUT_SCHEMA};"))

        conn.execute(text(f"""
        DROP TABLE IF EXISTS {OUT_SCHEMA}.zip_energy_metrics;
        CREATE TABLE {OUT_SCHEMA}.zip_energy_metrics AS
        WITH z AS (
            SELECT zip::text,
                   geom,
                   ST_Area(geom) AS area_m2
            FROM public.zip_codes
        ),
        solar AS (
            SELECT z.zip, COUNT(*) solar_customers
            FROM z
            LEFT JOIN public.solar_customers s
            ON ST_Contains(z.geom, s.geometry)
            GROUP BY z.zip
        ),
        evpts AS (
            SELECT z.zip, COUNT(*) ev_accounts_pts
            FROM z
            LEFT JOIN public.ev_accounts e
            ON ST_Contains(z.geom, e.geometry)
            GROUP BY z.zip
        ),
        chargers AS (
            SELECT z.zip, COUNT(*) superchargers
            FROM z
            LEFT JOIN public.superchargers c
            ON ST_Contains(z.geom, c.geometry)
            GROUP BY z.zip
        )
        SELECT
            z.zip,
            z.geom,
            z.area_m2,
            COALESCE(solar.solar_customers,0) solar_customers,
            COALESCE(evpts.ev_accounts_pts,0) ev_accounts_pts,
            COALESCE(chargers.superchargers,0) superchargers,
            COALESCE(evpts.ev_accounts_pts,0)/NULLIF((z.area_m2/1e6),0) ev_density
        FROM z
        LEFT JOIN solar ON z.zip=solar.zip
        LEFT JOIN evpts ON z.zip=evpts.zip
        LEFT JOIN chargers ON z.zip=chargers.zip;
        """))

    print("ZIP aggregation complete")

build_zip_metrics()

# ---------------------------------------------------
# 3️⃣ HOTSPOT ANALYSIS
# ---------------------------------------------------
def run_hotspots():

    gdf = gpd.read_postgis(
        f"SELECT zip, ev_density, geom FROM {OUT_SCHEMA}.zip_energy_metrics;",
        engine,
        geom_col="geom"
    )

    gdf["ev_density"] = gdf["ev_density"].fillna(0)

    w = Queen.from_dataframe(gdf)
    w.transform = "R"

    y = gdf["ev_density"].values

    # Global Moran's I
    moran = Moran(y, w, permutations=999)

    # Local Moran
    lisa = Moran_Local(y, w, permutations=999)

    # Gi*
    gi = G_Local(y, w, star=True, permutations=999)

    gdf["lisa_i"] = lisa.Is
    gdf["lisa_p"] = lisa.p_sim
    gdf["lisa_cluster"] = [
        "Hot Spot" if (q==1 and p<0.05) else
        "Cold Spot" if (q==3 and p<0.05) else
        "Not Significant"
        for q,p in zip(lisa.q, lisa.p_sim)
    ]

    gdf["gistar_z"] = gi.Zs
    gdf["gistar_p"] = gi.p_sim
    gdf["gistar_cat"] = [
        "Hot Spot" if (z>0 and p<0.05) else
        "Cold Spot" if (z<0 and p<0.05) else
        "Not Significant"
        for z,p in zip(gi.Zs, gi.p_sim)
    ]

    # Write back to PostGIS
    gdf.to_postgis(
        "zip_energy_metrics",
        engine,
        schema=OUT_SCHEMA,
        if_exists="replace",
        index=False
    )

    print("Hotspot analysis complete")
    print("Global Moran's I:", moran.I)
    print("p-value:", moran.p_sim)

run_hotspots()

print("🚀 FULL PIPELINE COMPLETE")