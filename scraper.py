import os
import json
import logging
import pandas as pd
from datetime import datetime
from itertools import combinations

# ========================= CONFIG =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

FALLBACK_CSV = "data/fallback.csv"

# Edge thresholds for tiers
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
    "FT": "FT", "FGA": "FGA", "FGM": "FGM",
    "FG%": "FGpct", "eFG%": "eFGpct",
    "PER": "PER", "USG%": "USG", "FPPM": "FPPM",
    "Pos": "Pos", "POS": "Pos", "Position": "Pos",
    "3PM": "3PM", "3P Made": "3PM",
}


def fetch_projections_csv() -> pd.DataFrame:
    """Load projections from fallback CSV or raise error."""
    if os.path.exists(FALLBACK_CSV):
        df = pd.read_csv(FALLBACK_CSV)
        logger.info("Loaded %d rows from fallback.csv", len(df))
        return df
    raise RuntimeError(
        f"fallback.csv not found at '{FALLBACK_CSV}'. "
        "Please upload your projections CSV to the data/ folder."
    )


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names and ensure required columns exist with proper types."""
    df = df.rename(columns={k: v for k, v in COL_MAP.items() if k in df.columns})

    # Ensure all expected columns exist
    for col in ["DVP", "Projection", "Salary", "Value", "PTS", "AST", "REB",
                "STL", "BLK", "FT", "FGA", "FGM", "FGpct", "eFGpct",
                "PER", "USG", "FPPM", "MINS", "Spread", "Total", "OU", "3PM"]:
        if col not in df.columns:
            df[col] = 0.0

    # Convert numeric columns
    numeric_cols = ["DVP", "Projection", "Salary", "Value", "PTS", "AST", "REB",
                    "STL", "BLK", "FT", "FGA", "FGM", "PER", "USG", "FPPM",
                    "MINS", "Total", "3PM"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Clean Spread and OU
    for col in ["Spread", "OU"]:
        df[col] = df[col].astype(str).str.replace(r"[^0-9.-]", "", regex=True)
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    return df


def build_combo_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Add combo stats (PR, PA, etc.) and confidence score."""
    df["PR"] = df["PTS"] + df["REB"]
    df["PA"] = df["PTS"] + df["AST"]
    df["RA"] = df["REB"] + df["AST"]
    df["PRA"] = df["PTS"] + df["REB"] + df["AST"]

    dvp_max = df["DVP"].max() if df["DVP"].max() > 0 else 30
    df["DVP_norm"] = df["DVP"] / dvp_max

    val = df["Value"].where(df["Value"] > 0, 5.0)
    df["conf_score"] = df["Projection"] * df["DVP_norm"] * (val / 5.0)

    return df


def best_cat(row: pd.Series) -> str:
    """Return the category with the highest projected value for a player."""
    cats = {c: row.get(c, 0) for c in PROP_CATS if row.get(c, 0) > 0}
    return max(cats, key=cats.get) if cats else "PTS"


def detect_games(df: pd.DataFrame):
    """Detect unique matchups."""
    games = set()
    opp_map = dict(zip(df["Team"], df["Opp"]))
    for team in df["Team"].dropna().unique():
        opp = opp_map.get(team)
        if opp and (opp, team) not in games:
            games.add((team, opp))
    return list(games)


def fmt_player(row: pd.Series) -> dict:
    """Format a player row into a clean dictionary for output."""
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


# ====================== MAIN BUILD FUNCTIONS ======================

def build_same_game_p4s(df: pd.DataFrame, games):
    """Build same-game player 4-packs (alpha + alt)."""
    results = []
    for t1, t2 in games:
        gdf = df[df["Team"].isin([t1, t2])].copy().sort_values("conf_score", ascending=False)
        if gdf.empty:
            continue

        spread = str(gdf["Spread"].iloc[0]) if not gdf.empty else "N/A"
        total = str(gdf["Total"].iloc[0]) if not gdf.empty else "N/A"
        ou = str(gdf["OU"].iloc[0]) if not gdf.empty else "N/A"

        alpha = gdf.head(4)
        alt = gdf.iloc[4:8]

        results.append({
            "game": f"{t1} vs {t2}",
            "spread": spread,
            "total": total,
            "ou": ou,
            "alpha": [fmt_player(r) for _, r in alpha.iterrows()],
            "alpha_conf": round(float(alpha["conf_score"].sum()), 1),
            "alpha_proj": round(float(alpha["Projection"].sum()), 1),
            "alpha_dvp": round(float(alpha["DVP"].mean()), 0),
            "alt": [fmt_player(r) for _, r in alt.iterrows()],
            "alt_conf": round(float(alt["conf_score"].sum()), 1) if len(alt) else 0,
            "alt_proj": round(float(alt["Projection"].sum()), 1) if len(alt) else 0,
        })

    results.sort(key=lambda x: x["alpha_conf"], reverse=True)
    return results


def build_slips(df: pd.DataFrame):
    """Build multi-leg player prop slips (2-5 legs)."""
    pool = df[
        (df["Projection"] >= MIN_PROJ) &
        (df["DVP"] >= MIN_DVP)
    ].copy().sort_values("conf_score", ascending=False).head(40).reset_index(drop=True)

    slips_by_legs = {n: [] for n in range(2, 6)}
    seen_keys = set()

    for n_legs in range(2, 6):
        rows_list = [dict(row) for _, row in pool.iterrows()]

        for combo in combinations(rows_list, n_legs):
            rows = list(combo)
            cats = [best_cat(r) for r in rows]
            if len(set(cats)) < n_legs:   # Must be different categories
                continue

            names = tuple(sorted(r["Name"] for r in rows))
            if names in seen_keys:
                continue
            seen_keys.add(names)

            total_conf = sum(float(r.get("conf_score", 0)) for r in rows)
            avg_total = sum(float(r.get("Total", 0)) for r in rows) / n_legs
            corr_bonus = 0.5 if avg_total >= 230 else 0.0
            slip_score = round(total_conf + corr_bonus, 2)

            avg_conf = total_conf / n_legs
            if avg_conf >= EDGE_LOCK:
                tier = "LOCK"
            elif avg_conf >= EDGE_LEAN:
                tier = "LEAN"
            else:
                tier = "DART"

            players = []
            for r in rows:
                bc = best_cat(r)
                players.append({
                    "name": str(r.get("Name", "")),
                    "team": str(r.get("Team", "")),
                    "opp": str(r.get("Opp", "")),
                    "dvp": int(r.get("DVP", 0)),
                    "proj": round(float(r.get("Projection", 0)), 1),
                    "cat": bc,
                    "cat_val": round(float(r.get(bc, 0)), 1),
                    "conf": round(float(r.get("conf_score", 0)), 2),
                    "mins": round(float(r.get("MINS", 0)), 1),
                    "usg": round(float(r.get("USG", 0)), 1),
                    "total": round(float(r.get("Total", 0)), 1),
                })

            slips_by_legs[n_legs].append({
                "legs": n_legs,
                "score": slip_score,
                "tier": tier,
                "payout": PAYOUTS.get(n_legs, 1),
                "avg_dvp": round(sum(float(r.get("DVP", 0)) for r in rows) / n_legs, 1),
                "avg_proj": round(sum(float(r.get("Projection", 0)) for r in rows) / n_legs, 1),
                "players": players,
            })

        slips_by_legs[n_legs].sort(key=lambda x: x["score"], reverse=True)
        slips_by_legs[n_legs] = slips_by_legs[n_legs][:15]   # Top 15 per leg count

    return slips_by_legs


def build_category_leaders(df: pd.DataFrame, games):
    """Build top performers per category per game."""
    cat_labels = {
        "PTS": "Points", "AST": "Assists", "REB": "Rebounds",
        "STL": "Steals", "BLK": "Blocks", "3PM": "3-Pointers",
        "PR": "Pts+Reb", "PA": "Pts+Ast", "RA": "Reb+Ast", "PRA": "Pts+Reb+Ast",
    }
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
                "leaders": [
                    {
                        "name": str(r["Name"]),
                        "team": str(r["Team"]),
                        "dvp": int(r["DVP"]),
                        "val": round(float(r[col]), 1),
                        "conf": round(float(r["conf_score"]), 1),
                    }
                    for _, r in top.iterrows()
                ],
            })

        result.append({
            "game": f"{t1} vs {t2}",
            "spread": str(gdf["Spread"].iloc[0]),
            "total": str(gdf["Total"].iloc[0]),
            "categories": game_cats,
        })
    return result


def build_top_locks(df: pd.DataFrame):
    """Top 12 highest confidence plays."""
    top = df.nlargest(12, "conf_score")
    locks = []
    for _, r in top.iterrows():
        bc = best_cat(r)
        locks.append({
            "name": str(r["Name"]),
            "team": str(r["Team"]),
            "opp": str(r.get("Opp", "")),
            "dvp": int(r["DVP"]),
            "proj": round(float(r["Projection"]), 1),
            "val": round(float(r.get("Value", 0)), 1),
            "mins": round(float(r.get("MINS", 0)), 1),
            "usg": round(float(r.get("USG", 0)), 1),
            "pts": round(float(r.get("PTS", 0)), 1),
            "reb": round(float(r.get("REB", 0)), 1),
            "ast": round(float(r.get("AST", 0)), 1),
            "stl": round(float(r.get("STL", 0)), 1),
            "blk": round(float(r.get("BLK", 0)), 1),
            "threepm": round(float(r.get("3PM", 0)), 1),
            "pra": round(float(r.get("PRA", 0)), 1),
            "best_cat": bc,
            "best_val": round(float(r.get(bc, 0)), 1),
            "conf": round(float(r["conf_score"]), 2),
            "total": round(float(r.get("Total", 0)), 1),
            "spread": str(r.get("Spread", "N/A")),
        })
    return locks


def build_value_plays(df: pd.DataFrame):
    """High DVP + high value plays."""
    vdf = df[
        (df["DVP"] >= 22) &
        (df["Value"] >= 5.5) &
        (df["Projection"] >= 25)
    ].copy().sort_values(["DVP", "Value"], ascending=False).head(10)

    result = []
    for _, r in vdf.iterrows():
        bc = best_cat(r)
        result.append({
            "name": str(r["Name"]),
            "team": str(r["Team"]),
            "opp": str(r.get("Opp", "")),
            "dvp": int(r["DVP"]),
            "proj": round(float(r["Projection"]), 1),
            "val": round(float(r.get("Value", 0)), 1),
            "salary": int(r.get("Salary", 0)),
            "best_cat": bc,
            "best_val": round(float(r.get(bc, 0)), 1),
            "conf": round(float(r["conf_score"]), 2),
            "total": round(float(r.get("Total", 0)), 1),
        })
    return result


def run_daily_scrape(output_path: str = "output/daily_report.json"):
    """Main function to run the full scrape and generate JSON report."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    os.makedirs("data", exist_ok=True)

    raw_df = fetch_projections_csv()
    df = normalize_columns(raw_df)
    df = build_combo_stats(df)
    df = df.dropna(subset=["Name", "Team", "Opp"]).reset_index(drop=True)

    # Save cleaned version as new fallback
    df.to_csv(FALLBACK_CSV, index=False)

    games = detect_games(df)

    report = {
        "generated_at": datetime.now().strftime("%B %d, %Y at %I:%M %p ET"),
        "slate_date": datetime.now().strftime("%A, %B %d"),
        "game_count": len(games),
        "same_game_p4": build_same_game_p4s(df, games),
        "slips": {str(k): v for k, v in build_slips(df).items()},
        "category_leaders": build_category_leaders(df, games),
        "top_locks": build_top_locks(df),
        "value_plays": build_value_plays(df),
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    logger.info("Report successfully saved to %s (%d games)", output_path, len(games))
    return report


if __name__ == "__main__":
    run_daily_scrape("output/daily_report.json")
