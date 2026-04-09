"""
scraper.py - Power4 NBA Report using fallback.csv
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


def fetch_projections_csv() -> pd.DataFrame:
    """Load data from the uploaded fallback.csv."""
    if os.path.exists(FALLBACK_CSV):
        logger.info(f"Loading data from {FALLBACK_CSV}")
        df = pd.read_csv(FALLBACK_CSV)
        logger.info(f"✅ Loaded {len(df)} player rows")
        return df
    else:
        logger.error("fallback.csv not found!")
        raise RuntimeError("No data available. Upload fallback.csv to data/ folder.")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Improved column normalization for lineups.com CSV."""
    col_map = {
        "Player": "Name", "PLAYER": "Name",
        "Salary": "Salary", "SAL": "Salary",
        "Projection": "Projection", "FPTS": "Projection", "Proj": "Projection",
        "Pts/$1k": "Pts/$1k", "VALUE": "Pts/$1k", "Value": "Pts/$1k",
        "Team": "Team", "TEAM": "Team",
        "Opp": "Opp", "OPP": "Opp", "Opponent": "Opp",
        "DVP": "DVP",
        "Spread": "Spread",
        "Total": "Total",
        "Minutes": "Minutes", "MIN": "Minutes",
        "PTS": "PTS", "Pts": "PTS",
        "AST": "AST", "Ast": "AST",
        "REB": "REB", "Reb": "REB",
        "STL": "STL", "Stl": "STL",
        "BLK": "BLK", "Blk": "BLK",
        "Pos": "Pos", "Position": "Pos",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # Ensure required columns exist
    required = ["Name", "Team", "Opp", "DVP", "Projection", "Salary", "Pts/$1k", "PTS", "AST", "REB"]
    for col in required:
        if col not in df.columns:
            df[col] = 0.0

    # Coerce numeric columns
    numeric_cols = ["DVP", "Projection", "Salary", "Pts/$1k", "PTS", "AST", "REB", "STL", "BLK", "Minutes"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    return df


def build_combo_stats(df: pd.DataFrame) -> pd.DataFrame:
    df["PR"]  = df["PTS"] + df["REB"]
    df["PA"]  = df["PTS"] + df["AST"]
    df["RA"]  = df["REB"] + df["AST"]
    df["PRA"] = df["PTS"] + df["REB"] + df["AST"]
    df["DVP_norm"]   = df["DVP"] / 30
    df["conf_score"] = df["Projection"] * df["DVP_norm"] * (df.get("Pts/$1k", 5) / 5)
    return df


def best_cat(row):
    cats = {
        "PTS": row["PTS"], "REB": row["REB"], "AST": row["AST"],
        "PR":  row["PR"],  "PA":  row["PA"],  "RA":  row["RA"],
        "PRA": row["PRA"],
    }
    return max(cats, key=cats.get)


def detect_games(df: pd.DataFrame):
    games = set()
    teams = df["Team"].dropna().unique()
    opps  = dict(zip(df["Team"], df["Opp"]))
    for team in teams:
        opp = opps.get(team)
        if opp and (opp, team) not in games:
            games.add((team, opp))
    return list(games)


def build_same_game_p4s(df: pd.DataFrame, games: list) -> list:
    results = []
    for t1, t2 in games:
        mask = df["Team"].isin([t1, t2])
        gdf  = df[mask].copy().sort_values("conf_score", ascending=False)

        spread = gdf["Spread"].iloc[0] if "Spread" in gdf.columns else "N/A"
        total  = gdf["Total"].iloc[0]  if "Total"  in gdf.columns else "N/A"

        alpha = gdf.head(4)
        alt   = gdf.iloc[4:8]

        def fmt_players(pool):
            players = []
            for _, r in pool.iterrows():
                bc = best_cat(r)
                bv = r[bc]
                players.append({
                    "name": r["Name"],
                    "team": r["Team"],
                    "dvp": int(r["DVP"]),
                    "proj": round(r["Projection"], 1),
                    "val": round(r.get("Pts/$1k", 0), 1),
                    "best_cat": bc,
                    "best_val": round(bv, 1),
                    "conf_score": round(r["conf_score"], 1),
                })
            return players

        results.append({
            "game": f"{t1} vs {t2}",
            "spread": spread,
            "total": total,
            "alpha": fmt_players(alpha),
            "alpha_conf": round(alpha["conf_score"].sum(), 1),
            "alpha_proj": round(alpha["Projection"].sum(), 1),
            "alpha_dvp": round(alpha["DVP"].mean(), 0),
            "alt": fmt_players(alt),
            "alt_conf": round(alt["conf_score"].sum(), 1),
            "alt_proj": round(alt["Projection"].sum(), 1),
        })
    return results


def build_multi_game_p4s(df: pd.DataFrame, games: list) -> list:
    game_keys = [f"{t1}/{t2}" for t1, t2 in games]
    df2 = df.copy()
    df2["game"] = df2["Team"].map({t: f"{t}/{o}" for t, o in zip(df["Team"], df["Opp"])})

    top_per_game = []
    for gk in game_keys:
        gdf = df2[df2["game"] == gk].sort_values("conf_score", ascending=False).head(3)
        top_per_game.extend(gdf.to_dict('records'))

    pool_df = pd.DataFrame(top_per_game)
    seen = set()
    combos = []

    for gc in combinations(game_keys, 4):
        for slot_combo in [(0,0,0,0),(0,0,0,1),(0,0,1,0),(0,1,0,0),(1,0,0,0)]:
            selected = []
            for i, g in enumerate(gc):
                idx = slot_combo[i] if i < len(slot_combo) else 0
                pool = pool_df[pool_df["game"] == g].sort_values("conf_score", ascending=False)
                if len(pool) > idx:
                    selected.append(pool.iloc[idx])
            if len(selected) != 4:
                continue
            sel_df = pd.DataFrame(selected)
            key = tuple(sorted(sel_df["Name"].tolist()))
            if key in seen:
                continue
            seen.add(key)

            players = []
            for _, r in sel_df.iterrows():
                bc = best_cat(r)
                players.append({
                    "name": r["Name"],
                    "team": r["Team"],
                    "game": r["game"].replace("/", " vs "),
                    "dvp": int(r["DVP"]),
                    "proj": round(r["Projection"], 1),
                    "val": round(r.get("Pts/$1k", 0), 1),
                    "best_cat": bc,
                    "best_val": round(r[bc], 1),
                })
            combos.append({
                "games_covered": [g.replace("/", " vs ") for g in gc],
                "conf_score": round(sel_df["conf_score"].sum(), 1),
                "total_proj": round(sel_df["Projection"].sum(), 1),
                "avg_dvp": round(sel_df["DVP"].mean(), 1),
                "players": players,
            })

    combos.sort(key=lambda x: x["conf_score"], reverse=True)
    return combos[:10]


def build_category_leaders(df: pd.DataFrame, games: list) -> list:
    cats = {
        "PTS": "Points", "AST": "Assists", "REB": "Rebounds",
        "PR": "Pts+Reb", "PA": "Pts+Ast", "RA": "Reb+Ast", "PRA": "Pts+Reb+Ast"
    }
    result = []
    for t1, t2 in games:
        mask = df["Team"].isin([t1, t2])
        gdf = df[mask].copy()
        spread = gdf["Spread"].iloc[0] if "Spread" in gdf.columns else "N/A"
        total = gdf["Total"].iloc[0] if "Total" in gdf.columns else "N/A"
        game_cats = []
        for col, label in cats.items():
            top = gdf.nlargest(6, col)[["Name", "Team", "DVP", col]].to_dict("records")
            game_cats.append({
                "category": label,
                "leaders": [
                    {"name": r["Name"], "team": r["Team"],
                     "dvp": int(r["DVP"]), "val": round(r[col], 1)}
                    for r in top
                ],
            })
        result.append({
            "game": f"{t1} vs {t2}",
            "spread": spread,
            "total": total,
            "categories": game_cats,
        })
    return result


def run_daily_scrape(output_path: str):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    raw_df = fetch_projections_csv()
    df = normalize_columns(raw_df)
    df = build_combo_stats(df)
    df = df.dropna(subset=["Name", "Team", "Opp"])

    # Save fallback copy
    os.makedirs("data", exist_ok=True)
    df.to_csv(FALLBACK_CSV, index=False)

    games = detect_games(df)
    same_game = build_same_game_p4s(df, games)
    multi_game = build_multi_game_p4s(df, games)
    cat_leaders = build_category_leaders(df, games)

    # Top locks
    name_counts = Counter()
    for combo in multi_game[:6]:
        for p in combo.get("players", []):
            name_counts[p["name"]] += 1
    top_locks = [
        {"name": n, "count": c, "pct": round(c / min(6, len(multi_game)) * 100)}
        for n, c in name_counts.most_common(6)
    ]

    report = {
        "generated_at": datetime.now().strftime("%B %d, %Y at %I:%M %p ET"),
        "slate_date": datetime.now().strftime("%A, %B %d"),
        "game_count": len(games),
        "same_game_p4": same_game,
        "multi_game_p4": multi_game,
        "category_leaders": cat_leaders,
        "top_locks": top_locks,
    }

    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    logger.info(f"✅ Report saved with {len(games)} games")
    return report
