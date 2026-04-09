"""
scraper.py
Fetches NBA fantasy projections from lineups.com and runs
the full Power 4 / multi-game analysis engine.
"""

import os
import json
import time
import logging
import requests
import pandas as pd
from datetime import datetime
from itertools import combinations
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

LINEUPS_URL = "https://www.lineups.com/nba/nba-fantasy-basketball-projections"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# Fallback: use local CSV if scrape fails
FALLBACK_CSV = "data/fallback.csv"


def fetch_projections_csv() -> pd.DataFrame:
    """Improved scraper for lineups.com projections page."""
    logger.info(f"Fetching projections from {LINEUPS_URL}")
    session = requests.Session()
    session.headers.update(HEADERS)

    for attempt in range(2):  # retry once
        try:
            r = session.get(LINEUPS_URL, timeout=30)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            
            # Find all tables and pick the most promising one (largest with player-like columns)
            tables = soup.find_all("table")
            logger.info(f"Found {len(tables)} tables on page")
            
            for i, table in enumerate(tables):
                try:
                    df = pd.read_html(str(table))[0]
                    if len(df) > 20 and any(col in str(df.columns).upper() for col in ["PLAYER", "NAME", "PROJECTION", "SALARY"]):
                        logger.info(f"✅ Successfully scraped table {i}: {len(df)} rows")
                        logger.info(f"Sample columns: {list(df.columns[:10])}")
                        return df
                except Exception as table_err:
                    logger.debug(f"Table {i} failed to parse: {table_err}")
                    continue
                    
        except Exception as e:
            logger.warning(f"Attempt {attempt+1} failed: {e}")
            if attempt == 0:
                time.sleep(3)  # short backoff before retry

    # Final fallback: use saved CSV
    if os.path.exists(FALLBACK_CSV):
        logger.warning("Using fallback CSV from disk.")
        return pd.read_csv(FALLBACK_CSV)

    raise RuntimeError("All data sources failed. No data available.")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to match our expected schema."""
    col_map = {
        "Player": "Name", "PLAYER": "Name",
        "Salary": "Salary", "SAL": "Salary",
        "Projection": "Projection", "FPTS": "Projection", "Proj": "Projection",
        "Pts/$1k": "Pts/$1k", "VALUE": "Pts/$1k", "Value": "Pts/$1k",
        "FPPM": "FPPM",
        "USG%": "USG%", "USG": "USG%",
        "Team": "Team", "TEAM": "Team",
        "Opp": "Opp", "OPP": "Opp", "Opponent": "Opp",
        "DVP": "DVP",
        "Spread": "Spread",
        "Total": "Total",
        "O/U": "O/U",
        "Minutes": "Minutes", "MIN": "Minutes", "Mins": "Minutes",
        "PTS": "PTS", "Pts": "PTS",
        "AST": "AST", "Ast": "AST",
        "REB": "REB", "Reb": "REB",
        "STL": "STL", "Stl": "STL",
        "BLK": "BLK", "Blk": "BLK",
        "Pos": "Pos", "Position": "Pos",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # Ensure required columns exist
    required = ["Name", "Team", "Opp", "DVP", "Projection", "Salary",
                "Pts/$1k", "PTS", "AST", "REB"]
    for col in required:
        if col not in df.columns:
            df[col] = 0

    # Coerce numeric
    numeric_cols = ["DVP", "Projection", "Salary", "Pts/$1k", "PTS", "AST",
                    "REB", "STL", "BLK", "Minutes", "FPPM"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


# ── Analysis Engine (unchanged from your original) ────────────────────────

def build_combo_stats(df: pd.DataFrame) -> pd.DataFrame:
    df["PR"]  = df["PTS"] + df["REB"]
    df["PA"]  = df["PTS"] + df["AST"]
    df["RA"]  = df["REB"] + df["AST"]
    df["PRA"] = df["PTS"] + df["REB"] + df["AST"]
    df["DVP_norm"]   = df["DVP"] / 30
    df["conf_score"] = (
        df["Projection"] * df["DVP_norm"] * (df["Pts/$1k"] / 5)
    )
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
                bc  = best_cat(r)
                bv  = r[bc]
                players.append({
                    "name":       r["Name"],
                    "team":       r["Team"],
                    "dvp":        int(r["DVP"]),
                    "proj":       round(r["Projection"], 1),
                    "val":        round(r["Pts/$1k"], 1),
                    "best_cat":   bc,
                    "best_val":   round(bv, 1),
                    "conf_score": round(r["conf_score"], 1),
                })
            return players

        results.append({
            "game":      f"{t1} vs {t2}",
            "t1": t1, "t2": t2,
            "spread":    spread,
            "total":     total,
            "alpha":     fmt_players(alpha),
            "alpha_conf": round(alpha["conf_score"].sum(), 1),
            "alpha_proj": round(alpha["Projection"].sum(), 1),
            "alpha_dvp":  round(alpha["DVP"].mean(), 0),
            "alt":       fmt_players(alt),
            "alt_conf":  round(alt["conf_score"].sum(), 1),
            "alt_proj":  round(alt["Projection"].sum(), 1),
        })
    return results


def build_multi_game_p4s(df: pd.DataFrame, games: list) -> list:
    game_keys  = [f"{t1}/{t2}" for t1, t2 in games]
    df2        = df.copy()
    df2["game"] = df2["Team"].map(
        {t: f"{t}/{o}" for t, o in zip(df["Team"], df["Opp"])}
    )

    top_per_game = []
    for gk in game_keys:
        gdf = df2[df2["game"] == gk].sort_values("conf_score", ascending=False).head(3)
        for _, r in gdf.iterrows():
            top_per_game.append(r)
    pool_df = pd.DataFrame(top_per_game)

    seen   = set()
    combos = []

    for gc in combinations(game_keys, 4):
        for slot_combo in [(0,0,0,0),(0,0,0,1),(0,0,1,0),(0,1,0,0),(1,0,0,0)]:
            selected = []
            for i, g in enumerate(gc):
                idx  = slot_combo[i] if i < len(slot_combo) else 0
                pool = pool_df[pool_df["game"] == g].sort_values(
                    "conf_score", ascending=False
                )
                if len(pool) > idx:
                    selected.append(pool.iloc[idx])
            if len(selected) != 4:
                continue
            sel_df = pd.DataFrame(selected)
            key    = tuple(sorted(sel_df["Name"].tolist()))
            if key in seen:
                continue
            seen.add(key)

            players = []
            for _, r in sel_df.iterrows():
                bc = best_cat(r)
                players.append({
                    "name":     r["Name"],
                    "team":     r["Team"],
                    "game":     r["game"].replace("/", " vs "),
                    "dvp":      int(r["DVP"]),
                    "proj":     round(r["Projection"], 1),
                    "val":      round(r["Pts/$1k"], 1),
                    "best_cat": bc,
                    "best_val": round(r[bc], 1),
                })
            combos.append({
                "games_covered": [g.replace("/", " vs ") for g in gc],
                "conf_score":    round(sel_df["conf_score"].sum(), 1),
                "total_proj":    round(sel_df["Projection"].sum(), 1),
                "avg_dvp":       round(sel_df["DVP"].mean(), 1),
                "players":       players,
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
        gdf  = df[mask].copy()
        spread = gdf["Spread"].iloc[0] if "Spread" in gdf.columns else "N/A"
        total  = gdf["Total"].iloc[0]  if "Total"  in gdf.columns else "N/A"
        game_cats = []
        for col, label in cats.items():
            top = gdf.nlargest(6, col)[["Name", "Team", "DVP", col]].to_dict("records")
            game_cats.append({
                "category": label,
                "col":      col,
                "leaders":  [
                    {"name": r["Name"], "team": r["Team"],
                     "dvp": int(r["DVP"]), "val": round(r[col], 1)}
                    for r in top
                ],
            })
        result.append({
            "game": f"{t1} vs {t2}",
            "spread": spread, "total": total,
            "categories": game_cats,
        })
    return result


# ── Main entry point ───────────────────────────────────────────────────────

def run_daily_scrape(output_path: str):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    raw_df = fetch_projections_csv()
    df     = normalize_columns(raw_df)
    df     = build_combo_stats(df)
    df     = df.dropna(subset=["Name", "Team", "Opp"])

    # Save fallback copy
    os.makedirs("data", exist_ok=True)
    df.to_csv(FALLBACK_CSV, index=False)

    games        = detect_games(df)
    same_game    = build_same_game_p4s(df, games)
    multi_game   = build_multi_game_p4s(df, games)
    cat_leaders  = build_category_leaders(df, games)

    # Top locks
    from collections import Counter
    name_counts = Counter()
    for combo in multi_game[:6]:
        for p in combo["players"]:
            name_counts[p["name"]] += 1
    top_locks = [
        {"name": n, "count": c, "pct": round(c / min(6, len(multi_game)) * 100)}
        for n, c in name_counts.most_common(6)
    ]

    report = {
        "generated_at": datetime.now().strftime("%B %d, %Y at %I:%M %p ET"),
        "slate_date":   datetime.now().strftime("%A, %B %d"),
        "game_count":   len(games),
        "same_game_p4": same_game,
        "multi_game_p4": multi_game,
        "category_leaders": cat_leaders,
        "top_locks":    top_locks,
    }

    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    logger.info(f"✅ Report saved to {output_path} with {len(games)} games")
    return report
