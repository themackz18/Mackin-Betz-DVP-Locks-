"""
scraper.py - Mackin Betz DVP Locks (Fixed & Minimal)
"""

import os
import json
import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

FALLBACK_CSV = "data/fallback.csv"


def fetch_projections_csv():
    if os.path.exists(FALLBACK_CSV):
        df = pd.read_csv(FALLBACK_CSV)
        logger.info(f"Loaded {len(df)} rows")
        return df
    raise RuntimeError("fallback.csv not found - upload to data/ folder")


def normalize_columns(df):
    col_map = {
        "Player": "Name", "PLAYER": "Name",
        "DvP": "DVP", "DVP": "DVP",
        "Salary": "Salary", "SAL": "Salary",
        "Projection": "Projection", "FPTS": "Projection",
        "Pts/$1k": "Value", "VALUE": "Value", "Value": "Value",
        "Team": "Team", "TEAM": "Team",
        "Opp": "Opp", "OPP": "Opp",
        "Spread": "Spread",
        "Total": "Total",
        "Minutes": "MINS", "MIN": "MINS",
        "PTS": "PTS", "AST": "AST", "REB": "REB",
        "STL": "STL", "BLK": "BLK",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    for col in ["DVP", "Projection", "Salary", "Value", "PTS", "AST", "REB", "STL", "BLK", "MINS"]:
        if col not in df.columns:
            df[col] = 0.0

    numeric = ["DVP", "Projection", "Salary", "Value", "PTS", "AST", "REB", "STL", "BLK", "MINS"]
    for col in numeric:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df


def build_combo_stats(df):
    df["PR"] = df["PTS"] + df["REB"]
    df["PA"] = df["PTS"] + df["AST"]
    df["RA"] = df["REB"] + df["AST"]
    df["PRA"] = df["PTS"] + df["REB"] + df["AST"]
    dvp_max = df["DVP"].max() if not df.empty else 30
    df["DVP_norm"] = df["DVP"] / dvp_max
    val = df["Value"].where(df["Value"] > 0, 5.0)
    df["conf_score"] = df["Projection"] * df["DVP_norm"] * (val / 5.0)
    return df


def best_cat(row):
    for c in ["PTS", "REB", "AST", "STL", "BLK", "PRA"]:
        if row.get(c, 0) > 0:
            return c
    return "PTS"


def detect_games(df):
    games = set()
    opp_map = dict(zip(df["Team"], df["Opp"]))
    for team in df["Team"].dropna().unique():
        opp = opp_map.get(team)
        if opp and (opp, team) not in games:
            games.add((team, opp))
    return list(games)


def build_same_game_p4s(df, games):
    results = []
    for t1, t2 in games:
        gdf = df[df["Team"].isin([t1, t2])].copy().sort_values("conf_score", ascending=False)
        alpha = gdf.head(4)
        results.append({
            "game": f"{t1} vs {t2}",
            "alpha": [{
                "name": str(r.get("Name", "")),
                "team": str(r.get("Team", "")),
                "dvp": int(r.get("DVP", 0)),
                "proj": round(float(r.get("Projection", 0)), 1),
                "val": round(float(r.get("Value", 0)), 1),
            } for _, r in alpha.iterrows()],
            "alpha_conf": round(float(alpha["conf_score"].sum()), 1) if not alpha.empty else 0,
            "alpha_proj": round(float(alpha["Projection"].sum()), 1) if not alpha.empty else 0,
        })
    return results


def build_category_leaders(df, games):
    result = []
    for t1, t2 in games:
        gdf = df[df["Team"].isin([t1, t2])].copy()
        if gdf.empty:
            continue
        game_cats = []
        for col, label in {"PTS": "Points", "AST": "Assists", "REB": "Rebounds"}.items():
            if col in gdf.columns:
                top = gdf.nlargest(5, col)
                game_cats.append({
                    "category": label,
                    "leaders": [{"name": str(r["Name"]), "team": str(r["Team"]), "dvp": int(r["DVP"]), "val": round(float(r[col]), 1)} for _, r in top.iterrows()]
                })
        result.append({"game": f"{t1} vs {t2}", "categories": game_cats})
    return result


def build_top_locks(df):
    top = df.nlargest(12, "conf_score")
    return [{
        "name": str(r.get("Name", "")),
        "team": str(r.get("Team", "")),
        "dvp": int(r.get("DVP", 0)),
        "proj": round(float(r.get("Projection", 0)), 1),
        "val": round(float(r.get("Value", 0)), 1),
        "conf": round(float(r.get("conf_score", 0)), 2),
    } for _, r in top.iterrows()]


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
        "slate_date": datetime.now().strftime("%A, %B %d"),
        "game_count": len(games),
        "same_game_p4": same_game,
        "category_leaders": cat_leaders,
        "top_locks": top_locks,
    }

    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    logger.info(f"Report saved - {len(games)} games")
    return report
