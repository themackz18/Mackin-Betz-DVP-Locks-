"""
scraper.py - Mackin Betz DVP Locks (Fixed & Complete)
"""

import os
import json
import logging
import pandas as pd
from datetime import datetime
from itertools import combinations
from collections import Counter

logger = logging.getLogger(__name__)

FALLBACK_CSV = "data/fallback.csv"

EDGE_LOCK = 6.0
EDGE_LEAN = 3.0
EDGE_DART = 1.5
MIN_DVP = 15
MIN_PROJ = 5.0

PAYOUTS = {2: 3, 3: 5, 4: 10, 5: 20}

PROP_CATS = ["PTS", "REB", "AST", "STL", "BLK", "PR", "PA", "RA", "PRA", "3PM"]

COL_MAP = {
    "Player": "Name", "PLAYER": "Name",
    "DvP": "DVP", "DVP": "DVP", "Dvp": "DVP",
    "Salary": "Salary", "SAL": "Salary",
    "Projection": "Projection", "FPTS": "Projection", "Proj": "Projection",
    "Pts/$1k": "Value", "VALUE": "Value", "Value": "Value", "Pts/$1K": "Value",
    "Team": "Team", "TEAM": "Team",
    "Opp": "Opp", "OPP": "Opp", "Opponent": "Opp",
    "Spread": "Spread", "SPREAD": "Spread",
    "Total": "Total", "TOTAL": "Total",
    "O/U": "OU", "OU": "OU",
    "Minutes": "MINS", "MIN": "MINS", "MINS": "MINS",
    "PTS": "PTS", "Pts": "PTS",
    "AST": "AST", "Ast": "AST",
    "REB": "REB", "Reb": "REB",
    "STL": "STL", "Stl": "STL",
    "BLK": "BLK", "Blk": "BLK",
    "3PM": "3PM", "3P Made": "3PM",
    "Pos": "Pos", "POS": "Pos", "Position": "Pos",
}

def fetch_projections_csv():
    if os.path.exists(FALLBACK_CSV):
        df = pd.read_csv(FALLBACK_CSV)
        logger.info(f"Loaded {len(df)} rows")
        return df
    raise RuntimeError("fallback.csv not found - upload to data/ folder")


def normalize_columns(df):
    df = df.rename(columns={k: v for k, v in COL_MAP.items() if k in df.columns})
    for col in ["DVP", "Projection", "Salary", "Value", "PTS", "AST", "REB", "STL", "BLK", "MINS", "3PM", "Spread", "Total", "OU"]:
        if col not in df.columns:
            df[col] = 0.0
    numeric = ["DVP", "Projection", "Salary", "Value", "PTS", "AST", "REB", "STL", "BLK", "MINS", "3PM", "Total"]
    for col in numeric:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df


def build_combo_stats(df):
    df["PR"] = df["PTS"] + df["REB"]
    df["PA"] = df["PTS"] + df["AST"]
    df["RA"] = df["REB"] + df["AST"]
    df["PRA"] = df["PTS"] + df["REB"] + df["AST"]
    dvp_max = df["DVP"].max() if not df["DVP"].empty else 30
    df["DVP_norm"] = df["DVP"] / dvp_max
    val = df["Value"].where(df["Value"] > 0, 5.0)
    df["conf_score"] = df["Projection"] * df["DVP_norm"] * (val / 5.0)
    return df


def best_cat(row):
    cats = {c: row.get(c, 0) for c in PROP_CATS if row.get(c, 0) > 0}
    return max(cats, key=cats.get) if cats else "PTS"


def detect_games(df):
    games = set()
    opp_map = dict(zip(df["Team"], df["Opp"]))
    for team in df["Team"].dropna().unique():
        opp = opp_map.get(team)
        if opp and (opp, team) not in games:
            games.add((team, opp))
    return list(games)


def fmt_player(row):
    bc = best_cat(row)
    return {
        "name": str(row.get("Name", "")),
        "team": str(row.get("Team", "")),
        "opp": str(row.get("Opp", "")),
        "pos": str(row.get("Pos", "")),
        "dvp": int(row.get("DVP", 0)),
        "proj": round(float(row.get("Projection", 0)), 1),
        "val": round(float(row.get("Value", 0)), 1),
        "mins": round(float(row.get("MINS", 0)), 1),
        "best_cat": bc,
        "best_val": round(float(row.get(bc, 0)), 1),
        "conf_score": round(float(row.get("conf_score", 0)), 2),
    }


def build_same_game_p4s(df, games):
    results = []
    for t1, t2 in games:
        gdf = df[df["Team"].isin([t1, t2])].copy().sort_values("conf_score", ascending=False)
        spread = str(gdf["Spread"].iloc[0]) if not gdf.empty else "N/A"
        total = str(gdf["Total"].iloc[0]) if not gdf.empty else "N/A"
        alpha = gdf.head(4)
        alt = gdf.iloc[4:8]
        results.append({
            "game": f"{t1} vs {t2}",
            "spread": spread,
            "total": total,
            "alpha": [fmt_player(r) for _, r in alpha.iterrows()],
            "alpha_conf": round(float(alpha["conf_score"].sum()), 1) if not alpha.empty else 0,
            "alpha_proj": round(float(alpha["Projection"].sum()), 1) if not alpha.empty else 0,
            "alpha_dvp": round(float(alpha["DVP"].mean()), 0) if not alpha.empty else 0,
            "alt": [fmt_player(r) for _, r in alt.iterrows()],
        })
    results.sort(key=lambda x: x.get("alpha_conf", 0), reverse=True)
    return results


def build_category_leaders(df, games):
    cat_labels = {"PTS": "Points", "AST": "Assists", "REB": "Rebounds", "STL": "Steals", "BLK": "Blocks", "3PM": "3-Pointers", "PR": "Pts+Reb", "PA": "Pts+Ast", "PRA": "Pts+Reb+Ast"}
    result = []
    for t1, t2 in games:
        gdf = df[df["Team"].isin([t1, t2])].copy()
        if gdf.empty:
            continue
        game_cats = []
        for col, label in cat_labels.items():
            if col not in gdf.columns or gdf[col].sum() == 0:
                continue
            top = gdf.nlargest(5, col)
            game_cats.append({
                "category": label,
                "leaders": [{"name": str(r["Name"]), "team": str(r["Team"]), "dvp": int(r["DVP"]), "val": round(float(r[col]), 1)} for _, r in top.iterrows()]
            })
        result.append({"game": f"{t1} vs {t2}", "spread": str(gdf["Spread"].iloc[0]), "total": str(gdf["Total"].iloc[0]), "categories": game_cats})
    return result


def build_top_locks(df):
    top = df.nlargest(12, "conf_score")
    locks = []
    for _, r in top.iterrows():
        bc = best_cat(r)
        locks.append({
            "name": str(r.get("Name", "")),
            "team": str(r.get("Team", "")),
            "opp": str(r.get("Opp", "")),
            "dvp": int(r.get("DVP", 0)),
            "proj": round(float(r.get("Projection", 0)), 1),
            "val": round(float(r.get("Value", 0)), 1),
            "best_cat": bc,
            "best_val": round(float(r.get(bc, 0)), 1),
            "conf": round(float(r.get("conf_score", 0)), 2),
        })
    return locks


def run_daily_scrape(output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    os.makedirs("data", exist_ok=True)

    raw_df = fetch_projections_csv()
    df = normalize_columns(raw_df)
    df = build_combo_stats(df)
    df = df.dropna(subset=["Name", "Team", "Opp"])

    df.to_csv(FALLBACK_CSV, index=False)

    games = detect_games(df)
    same_game = build_same_game_p4s(df, games)
    cat_leaders = build_category_leaders(df, games)
    top_locks = build_top_locks(df)

    report = {
        "generated_at": datetime.now().strftime("%B %d, %Y at %I:%M %p ET"),
