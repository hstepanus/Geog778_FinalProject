# hotspot.py
import numpy as np
import pandas as pd
import geopandas as gpd

from sqlalchemy import text

from libpysal.weights import Queen
from esda.moran import Moran, Moran_Local
from esda.getisord import G_Local


def _ensure_columns(engine, table_fq: str):
    """
    Adds hotspot fields if they don't exist yet.
    """
    sql = f"""
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
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def _cluster_label_lisa(q: int, p: float, alpha=0.05):
    """
    LISA quadrant classification (Anselin):
      q=1 HH, q=2 LH, q=3 LL, q=4 HL
    Mark non-significant if p > alpha.
    """
    if p is None or np.isnan(p) or p > alpha:
        return "Not Significant"
    return {1: "High-High", 2: "Low-High", 3: "Low-Low", 4: "High-Low"}.get(int(q), "Not Significant")


def _cluster_label_gistar(z: float, p: float, alpha=0.05):
    """
    Simple Gi* categorization.
    """
    if p is None or np.isnan(p) or p > alpha:
        return "Not Significant"
    if z >= 0:
        return "Hot Spot"
    return "Cold Spot"


def compute_hotspots_and_writeback(
    engine,
    schema: str = "analysis",
    table: str = "zip_energy_metrics",
    geom_col: str = "geom",
    id_col: str = "zip",
    value_col: str = "adoption_score",
    alpha: float = 0.05,
):
    """
    Reads ZIP polygons from PostGIS, computes:
      - Global Moran's I
      - Local Moran (LISA)
      - Getis-Ord Gi* (G_Local with star=True)
    Then updates {schema}.{table} with new columns.

    Notes:
      - Requires polygon contiguity to exist (Queen).
      - Assumes geometries are valid polygons and already clipped to NOVEC.
      - Handles islands (no neighbors) by dropping them from PySAL computation,
        then writes NULL results back for those ZIPs.
    """
    table_fq = f"{schema}.{table}"
    _ensure_columns(engine, table_fq)

    # Pull data (keep it small: only what we need)
    gdf = gpd.read_postgis(
        f"""
        SELECT
          {id_col}::text AS id,
          {value_col}::double precision AS value,
          {geom_col} AS geom
        FROM {table_fq}
        WHERE {geom_col} IS NOT NULL;
        """,
        engine,
        geom_col="geom",
    )

    if gdf.empty:
        return {"status": "error", "message": "No geometries found to analyze."}

    # Clean values: replace NaN with 0 (or you can drop them)
    gdf["value"] = gdf["value"].fillna(0.0).astype(float)

    # Build contiguity weights (Queen)
    w = Queen.from_dataframe(gdf, geom_col="geom", ids=gdf["id"].tolist())

    # Identify islands (ZIPs with no neighbors)
    islands = list(w.islands)
    if islands:
        # Drop islands for analysis to avoid singular weight issues
        gdf_use = gdf[~gdf["id"].isin(islands)].copy()
        w_use = Queen.from_dataframe(gdf_use, geom_col="geom", ids=gdf_use["id"].tolist())
    else:
        gdf_use = gdf
        w_use = w

    # Row-standardize weights (common default)
    w_use.transform = "R"

    y = gdf_use["value"].to_numpy()

    # -------- Global Moran's I --------
    moran_g = Moran(y, w_use, permutations=999)

    # -------- Local Moran (LISA) --------
    lisa = Moran_Local(y, w_use, permutations=999)

    # lisa.Is: local I
    # lisa.p_sim: simulated p-values
    # lisa.z_sim: simulated z
    # lisa.q: quadrant
    lisa_df = pd.DataFrame({
        "id": gdf_use["id"].values,
        "lisa_i": lisa.Is,
        "lisa_p": lisa.p_sim,
        "lisa_z": lisa.z_sim,
        "lisa_q": lisa.q
    })
    lisa_df["lisa_cluster"] = [
        _cluster_label_lisa(q, p, alpha=alpha) for q, p in zip(lisa_df["lisa_q"], lisa_df["lisa_p"])
    ]

    # -------- Getis-Ord Gi* (local G) --------
    # G_Local with star=True gives Gi* statistic
    gi = G_Local(y, w_use, star=True, permutations=999)

    gi_df = pd.DataFrame({
        "id": gdf_use["id"].values,
        "gistar_z": gi.Zs,       # standardized z-scores
        "gistar_p": gi.p_sim     # permutation p-values
    })
    gi_df["gistar_cat"] = [
        _cluster_label_gistar(z, p, alpha=alpha) for z, p in zip(gi_df["gistar_z"], gi_df["gistar_p"])
    ]

    # Combine local results
    out = lisa_df.merge(gi_df, on="id", how="left")

    # Add global stats to every row (so the app can display them)
    out["moran_global_i"] = float(moran_g.I)
    out["moran_global_p"] = float(moran_g.p_sim)
    out["moran_global_z"] = float(moran_g.z_sim)

    # Re-introduce islands as NULL results (keeps table complete)
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

    # Write back by batch UPDATE using VALUES
    update_sql = f"""
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
    WHERE t.{id_col}::text = v.id;
    """

    # Build VALUES block safely with parameters in chunks
    records = out.to_dict(orient="records")

    def chunks(lst, n=500):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]

    with engine.begin() as conn:
        for batch in chunks(records, 400):
            params = {}
            rows_sql = []
            for i, r in enumerate(batch):
                # param names
                def p(k): return f"{k}_{i}"

                params[p("id")] = r["id"]
                params[p("mgi")] = r["moran_global_i"]
                params[p("mgp")] = r["moran_global_p"]
                params[p("mgz")] = r["moran_global_z"]

                params[p("li")] = r["lisa_i"]
                params[p("lp")] = r["lisa_p"]
                params[p("lz")] = r["lisa_z"]
                params[p("lq")] = r["lisa_q"]
                params[p("lc")] = r["lisa_cluster"]

                params[p("gz")] = r["gistar_z"]
                params[p("gp")] = r["gistar_p"]
                params[p("gc")] = r["gistar_cat"]

                rows_sql.append(
                    f"(:{p('id')}, :{p('mgi')}, :{p('mgp')}, :{p('mgz')}, "
                    f":{p('li')}, :{p('lp')}, :{p('lz')}, :{p('lq')}, :{p('lc')}, "
                    f":{p('gz')}, :{p('gp')}, :{p('gc')})"
                )

            sql = update_sql.replace("{values_block}", ",\n".join(rows_sql))
            conn.execute(text(sql), params)

    return {
        "status": "ok",
        "table": table_fq,
        "value_col": value_col,
        "alpha": alpha,
        "global_morans_i": float(moran_g.I),
        "global_p_sim": float(moran_g.p_sim),
        "global_z_sim": float(moran_g.z_sim),
        "islands": islands,
        "n_features": int(len(gdf)),
        "n_used_for_weights": int(len(gdf_use))
    }