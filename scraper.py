# scraper.py - Mackin Betz DVP Locks

import os
import json
import logging
import pandas as pd
from datetime import datetime
from itertools import combinations

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
    "Player": "Name",
    "PLAYER": "Name",
    "DvP": "DVP",
    "DVP": "DVP",
    "Dvp": "DVP",
    "Salary": "Salary",
    "SAL": "Salary",
    "Projection": "Projection",
    "FPTS": "Projection",
    "Proj": "Projection",
    "Pts/$1k": "Value",
    "VALUE": "Value",
    "Value": "Value",
    "Pts/$1K": "Value",
    "Team": "Team",
    "TEAM": "Team",
    "Opp": "Opp",
    "OPP": "Opp",
    "Opponent": "Opp",
    "Spread": "Spread",
    "SPREAD": "Spread",
    "Total": "Total",
    "TOTAL": "Total",
    "O/U": "OU",
    "OU": "OU",
    "Minutes": "MINS",
    "MIN": "MINS",
    "MINS": "MINS",
    "PTS": "PTS",
    "Pts": "PTS",
    "AST": "AST",
    "Ast": "AST",
    "REB": "REB",
    "Reb": "REB",
    "STL": "STL",
    "Stl": "STL",
    "BLK": "BLK",
    "Blk": "BLK",
    "FT": "FT",
    "FGA": "FGA",
    "FGM": "FGM",
    "FG%": "FGpct",
    "eFG%": "eFGpct",
    "PER": "PER",
    "USG%": "USG",
    "FPPM": "FPPM",
    "Pos": "Pos",
    "POS": "Pos",
    "Position": "Pos",
    "3PM": "3PM",
    "3P Made": "3PM",
}


def fetch_projections_csv():
    if os.path.exists(FALLBACK_CSV):
        df = pd.read_csv(FALLBACK_CSV)
        logger.info("Loaded %d rows", len(df))
        return df
    raise RuntimeError("fallback.csv not found - upload to data/ folder")


def normalize_columns(df):
    df = df.rename(columns={k: v for k, v in COL_MAP.items() if k in df.columns})

    for col in [
        "DVP",
        "Projection",
        "Salary",
        "Value",
        "PTS",
        "AST",
        "REB",
        "STL",
        "BLK",
        "FT",
        "FGA",
        "FGM",
        "FGpct",
        "eFGpct",
        "PER",
        "USG",
        "FPPM",
        "MINS",
        "Spread",
        "Total",
        "OU",
        "3PM",
    ]:
        if col not in df.columns:
            df[col] = 0.0

    numeric = [
        "DVP",
        "Projection",
        "Salary",
        "Value",
        "PTS",
        "AST",
        "REB",
        "STL",
        "BLK",
        "FT",
        "FGA",
        "FGM",
        "PER",
        "USG",
        "FPPM",
        "MINS",
        "Total",
        "3PM",
    ]
    for col in numeric:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    for col in ["Spread", "OU"]:
        df[col] = df[col].astype(str).str.replace(r"[^0-9.-]", "", regex=True)
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    return df


def build_combo_stats(df):
    df["PR"] = df["PTS"] + df["REB"]
    df["PA"] = df["PTS"] + df["AST"]
    df["RA"] = df["REB"] + df["AST"]
    df["PRA"] = df["PTS"] + df["REB"] + df["AST"]

    dvp_max = df["DVP"].max() if df["DVP"].max() > 0 else 30
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
        "pts": round(float(row.get("PTS", 0)), 1),
        "reb": round(float(row.get("REB", 0)), 1),
        "ast": round(float(row.get("AST", 0)), 1),
        "stl": round(float(row.get("STL", 0)), 1),
        "blk": round(float(row.get("BLK", 0)), 1),
        "threepm": round(float(row.get("3PM", 0)), 1),
        "pr": round(float(row.get("PR", 0)), 1),
        "pa": round(float(row.get("PA", 0)), 1),
        "ra": round(float(row.get("RA", 0)), 1),
        "pra": round(float(row.get("PRA", 0)), 1),
        "best_cat": bc,
        "best_val": round(float(row.get(bc, 0)), 1),
        "conf_score": round(float(row.get("conf_score", 0)), 2),
        "usg": round(float(row.get("USG", 0)), 1),
        "total": round(float(row.get("Total", 0)), 1),
        "spread": str(row.get("Spread", "N/A")),
    }


def build_same_game_p4s(df, games):
    results = []
    for t1, t2 in games:
        gdf = df[df["Team"].isin([t1, t2])].copy().sort_values("conf_score", ascending=False)
        spread = str(gdf["Spread"].iloc[0]) if len(gdf) > 0 else "N/A"
        total = str(gdf["Total"].iloc[0]) if len(gdf) > 0 else "N/A"
        ou = str(gdf["OU"].iloc[0]) if len(gdf) > 0 else "N/A"

        alpha = gdf.head(4)
        alt = gdf.iloc[4:8]

        results.append(
            {
                "game": f"{t1} vs {t2}",
                "spread": spread,
                "total": total,
                "ou": ou,
                "alpha": [fmt_player(row) for _, row in alpha.iterrows()],
                "alt": [fmt_player(row) for _, row in alt.iterrows()],
            }
        )
    return results


def run_daily_scrape(output_path="data/report.json"):
    logger.info("Starting daily scrape...")

    df = fetch_projections_csv()
    df = normalize_columns(df)
    df = build_combo_stats(df)

    games = detect_games(df)

    # Category leaders
    category_leaders = []
    for cat in PROP_CATS:
        top = df.nlargest(5, cat)
        category_leaders.append(
            {
                "category": cat,
                "players": [fmt_player(row) for _, row in top.iterrows()],
            }
        )

    # Top locks, leans, darts
    top_locks = df[df["conf_score"] >= EDGE_LOCK].nlargest(15, "conf_score")
    value_plays = df[df["conf_score"] >= EDGE_LEAN].nlargest(20, "conf_score")

    same_game_p4 = build_same_game_p4s(df, games)

    # Build slips (parlays)
    slips = {"2": [], "3": [], "4": [], "5": []}
    high_conf = df[df["conf_score"] >= EDGE_DART].nlargest(12, "conf_score")

    for size in [2, 3, 4, 5]:
        for combo in combinations(high_conf.iterrows(), size):
            players = [fmt_player(row) for _, row in combo]
            total_proj = sum(p["proj"] for p in players)
            slips[str(size)].append(
                {
                    "players": players,
                    "total_proj": round(total_proj, 1),
                    "payout": PAYOUTS.get(size, 0),
                }
            )
        slips[str(size)] = slips[str(size)][:8]  # limit to top 8 per size

    report = {
        "generated_at": datetime.now().isoformat(),
        "slate_date": datetime.now().strftime("%Y-%m-%d"),
        "game_count": len(games),
        "same_game_p4": same_game_p4,
        "slips": slips,
        "category_leaders": category_leaders,
        "top_locks": [fmt_player(row) for _, row in top_locks.iterrows()],
        "value_plays": [fmt_player(row) for _, row in value_plays.iterrows()],
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    logger.info("Report saved to %s", output_path)
    return report


if __name__ == "__main__":
    run_daily_scrape()
