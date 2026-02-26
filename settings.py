import os

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

ANALYSIS_EPSG = int(os.getenv("ANALYSIS_EPSG", "26918"))  # NAD83 / UTM 18N (meters)

# IMPORTANT: Set these to your real column names if needed
ZIP_POLY_KEY = os.getenv("ZIP_POLY_KEY", "zip")     # column in zip_codes
EVBY_ZIP_KEY = os.getenv("EVBY_ZIP_KEY", "zip")     # column in ev_by_zip
EVBY_COUNT_COL = os.getenv("EVBY_COUNT_COL", "ev_count")  # column in ev_by_zip with EV totals