import os
import json
import logging
import pandas as pd
from datetime import datetime
from itertools import combinations

logger = logging.getLogger(__name__)

FALLBACK_CSV = "data/fallback.csv"
REPORT_PATH = "data/report.json"

EDGE_LOCK = 6.0
EDGE_LEAN = 3.0
EDGE_DART = 1.5

PAYOUTS = {2: 3, 3: 5, 4: 10, 5: 20}

PROP_CATS = ["PTS", "REB", "AST", "STL", "BLK", "PR", "PA", "RA", "PRA", "3PM", "FG_ATT"]

def fetch_projections_csv():
    if os.path.exists(FALLBACK_CSV):
        df = pd.read_csv(FALLBACK_CSV)
        logger.info(f"Loaded {len(df)} rows from fallback.csv")
        return df
    raise RuntimeError("fallback.csv not found - upload to data/ folder")

def normalize_columns(df):
    col_map = {
        "Player": "Name", "PLAYER": "Name",
        "DvP": "DVP", "DVP": "DVP",
        "Projection": "Projection", "FPTS": "Projection", "Proj": "Projection",
        "Value": "Value", "Pts/$1k": "Value",
        "Team": "Team", "Opp": "Opp",
        "MINS": "MINS",
        "PTS": "PTS", "AST": "AST", "REB": "REB",
        "STL": "STL", "BLK": "BLK", "3PM": "3PM", "FGA": "FG_ATT"
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # Ensure all real stat columns exist
    for col in ["DVP", "Projection", "Value", "MINS", "PTS", "AST", "REB", "STL", "BLK", "3PM", "FG_ATT"]:
        if col not in df.columns:
            df[col] = 0.0

    numeric = ["DVP", "Projection", "Value", "MINS", "PTS", "AST", "REB", "STL", "BLK", "3PM", "FG_ATT"]
    for col in numeric:
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
    cats = {c: float(row.get(c, 0)) for c in PROP_CATS if float(row.get(c, 0)) > 0}
    return max(cats, key=cats.get) if cats else "PRA"

def fmt_player(row):
    grade = "A" if row.get("conf_score", 0) >= EDGE_LOCK else "B" if row.get("conf_score", 0) >= EDGE_LEAN else "C"
    confidence = int(min(95, max(60, row.get("conf_score", 0) * 8)))

    return {
        "name": str(row.get("Name", "")),
        "team": str(row.get("Team", "")),
        "opp": str(row.get("Opp", "")),
        "dvp": round(float(row.get("DVP", 0)), 1),
        "proj": round(float(row.get("Projection", 0)), 1),
        "val": round(float(row.get("Value", 0)), 1),
        "pts": round(float(row.get("PTS", 0)), 1),
        "reb": round(float(row.get("REB", 0)), 1),
        "ast": round(float(row.get("AST", 0)), 1),
        "stl": round(float(row.get("STL", 0)), 1),
        "blk": round(float(row.get("BLK", 0)), 1),
        "threepm": round(float(row.get("3PM", 0)), 1),
        "threepm_avg": round(float(row.get("3PM", 0)), 1),
        "threepm_att": round(float(row.get("FG_ATT", 0)) * 0.35, 1),  # rough attempt estimate
        "fg_att": round(float(row.get("FG_ATT", 0)), 1),
        "h2h": "—",
        "recent_sim": "—",
        "grade": grade,
        "confidence": confidence,
        "target_prop": best_cat(row),
        "best_val": round(float(row.get(best_cat(row), 0)), 1),
    }

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
            "alpha": [fmt_player(row) for _, row in alpha.iterrows()]
        })
    return results

def build_diverse_slips(df):
    slips = {"2": [], "3": [], "4": [], "5": []}
    high_conf = df[df["conf_score"] >= EDGE_DART].nlargest(15, "conf_score")

    for size in [2, 3, 4, 5]:
        for combo in combinations(high_conf.iterrows(), size):
            players = [fmt_player(row) for _, row in combo]
            total_proj = sum(p["proj"] for p in players)
            target_prop = players[0]["target_prop"] if players else "PRA"
            slips[str(size)].append({
                "players": players,
                "total_proj": round(total_proj, 1),
                "payout": PAYOUTS.get(size, 0),
                "target_prop": target_prop
            })
        slips[str(size)] = slips[str(size)][:8]
    return slips

def build_category_leaders(df):
    leaders = []
    for cat in ["PTS", "REB", "AST", "PRA", "3PM", "STL", "BLK"]:
        top = df.nlargest(5, cat)
        leaders.append({
            "category": cat,
            "players": [fmt_player(row) for _, row in top.iterrows()]
        })
    return leaders

def run_daily_scrape(output_path=REPORT_PATH):
    logger.info("Starting daily scrape with real stats and diverse categories...")

    df = fetch_projections_csv()
    df = normalize_columns(df)
    df = build_combo_stats(df)

    report = {
        "generated_at": datetime.now().isoformat(),
        "slate_date": datetime.now().strftime("%Y-%m-%d"),
        "game_count": len(detect_games(df)),
        "same_game_p4": build_same_game_p4s(df, detect_games(df)),
        "slips": build_diverse_slips(df),
        "category_leaders": build_category_leaders(df),
        "top_locks": [fmt_player(row) for _, row in df[df["conf_score"] >= EDGE_LOCK].nlargest(12, "conf_score").iterrows()],
        "value_plays": [fmt_player(row) for _, row in df[df["conf_score"] >= EDGE_LEAN].nlargest(18, "conf_score").iterrows()],
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    logger.info("Report saved with real stats (PTS, REB, AST, 3PM, FG_ATT) and diverse slips")
    return report

if __name__ == "__main__":
    run_daily_scrape()
