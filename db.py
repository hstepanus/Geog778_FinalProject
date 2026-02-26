from sqlalchemy import create_engine, text
from settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS

def make_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, future=True, pool_pre_ping=True)

def run_sql(engine, sql: str, params=None):
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})

def fetch_all(engine, sql: str, params=None):
    with engine.connect() as conn:
        res = conn.execute(text(sql), params or {})
        return res.fetchall(), list(res.keys())