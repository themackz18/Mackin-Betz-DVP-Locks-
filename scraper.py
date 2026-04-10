“””
scraper.py - Mackin Betz DVP Locks
Full rewrite: PrizePicks-focused, category-diverse slip builder, 2-5 leg combos
“””

import os
import json
import logging
import pandas as pd
from datetime import datetime
from itertools import combinations
from collections import defaultdict

logger = logging.getLogger(**name**)
FALLBACK_CSV = “data/fallback.csv”

# ── Confidence thresholds ──────────────────────────────────────────────────────

EDGE_LOCK   = 6.0   # projection massively over line → 🔒 Lock
EDGE_LEAN   = 3.0   # solid edge → ✅ Lean
EDGE_DART   = 1.5   # playable edge → 🎯 Dart
MIN_DVP     = 15    # minimum DVP rank to be included in slips
MIN_PROJ    = 5.0   # minimum projection value to consider

# Payout multipliers per leg count (PrizePicks Power Play)

PAYOUTS = {2: 3, 3: 5, 4: 10, 5: 20}

# ── Prop categories we care about ─────────────────────────────────────────────

PROP_CATS = [“PTS”, “REB”, “AST”, “STL”, “BLK”, “PR”, “PA”, “RA”, “PRA”, “3PM”]

# ── Column normalisation map ───────────────────────────────────────────────────

COL_MAP = {
“Player”: “Name”, “PLAYER”: “Name”,
“DvP”: “DVP”, “DVP”: “DVP”, “Dvp”: “DVP”,
“Salary”: “Salary”, “SAL”: “Salary”,
“Projection”: “Projection”, “FPTS”: “Projection”, “Proj”: “Projection”,
“Pts/$1k”: “Value”, “VALUE”: “Value”, “Value”: “Value”, “Pts/$1K”: “Value”,
“Team”: “Team”, “TEAM”: “Team”,
“Opp”: “Opp”, “OPP”: “Opp”, “Opponent”: “Opp”,
“Spread”: “Spread”, “SPREAD”: “Spread”,
“Total”: “Total”, “TOTAL”: “Total”,
“O/U”: “OU”, “OU”: “OU”,
“Minutes”: “MINS”, “MIN”: “MINS”, “MINS”: “MINS”,
“PTS”: “PTS”, “Pts”: “PTS”,
“AST”: “AST”, “Ast”: “AST”,
“REB”: “REB”, “Reb”: “REB”,
“STL”: “STL”, “Stl”: “STL”,
“BLK”: “BLK”, “Blk”: “BLK”,
“FT”: “FT”,
“FGA”: “FGA”,
“FGM”: “FGM”,
“FG%”: “FGpct”,
“eFG%”: “eFGpct”,
“PER”: “PER”,
“USG%”: “USG”,
“FPPM”: “FPPM”,
“Pos”: “Pos”, “POS”: “Pos”, “Position”: “Pos”,
“3PM”: “3PM”, “3P Made”: “3PM”,
}

# ── Load ───────────────────────────────────────────────────────────────────────

def fetch_projections_csv() -> pd.DataFrame:
if os.path.exists(FALLBACK_CSV):
logger.info(f”Loading {FALLBACK_CSV}”)
df = pd.read_csv(FALLBACK_CSV)
logger.info(f”✅ {len(df)} rows loaded”)
return df
raise RuntimeError(“fallback.csv not found — upload to data/ folder”)

# ── Normalise ──────────────────────────────────────────────────────────────────

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
df = df.rename(columns={k: v for k, v in COL_MAP.items() if k in df.columns})

```
# Ensure all stat cols exist
for col in ["DVP", "Projection", "Salary", "Value", "PTS", "AST", "REB",
            "STL", "BLK", "FT", "FGA", "FGM", "FGpct", "eFGpct",
            "PER", "USG", "FPPM", "MINS", "Spread", "Total", "OU", "3PM"]:
    if col not in df.columns:
        df[col] = 0.0

numeric = ["DVP", "Projection", "Salary", "Value", "PTS", "AST", "REB",
           "STL", "BLK", "FT", "FGA", "FGM", "PER", "USG", "FPPM",
           "MINS", "Total", "3PM"]
for col in numeric:
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

# Spread / OU — strip signs and convert
for col in ["Spread", "OU"]:
    df[col] = df[col].astype(str).str.replace(r"[^0-9.\-]", "", regex=True)
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

return df
```

# ── Derived stats ──────────────────────────────────────────────────────────────

def build_combo_stats(df: pd.DataFrame) -> pd.DataFrame:
df[“PR”]  = df[“PTS”] + df[“REB”]
df[“PA”]  = df[“PTS”] + df[“AST”]
df[“RA”]  = df[“REB”] + df[“AST”]
df[“PRA”] = df[“PTS”] + df[“REB”] + df[“AST”]

```
# Normalised DVP (1–30 → 0–1, higher = better matchup)
dvp_max = df["DVP"].max() if df["DVP"].max() > 0 else 30
df["DVP_norm"] = df["DVP"] / dvp_max

# Confidence score: projection quality × matchup quality
val = df["Value"].where(df["Value"] > 0, 5.0)
df["conf_score"] = df["Projection"] * df["DVP_norm"] * (val / 5.0)

return df
```

# ── Best single category for a player ─────────────────────────────────────────

def best_cat(row) -> str:
cats = {c: row.get(c, 0) for c in PROP_CATS if row.get(c, 0) > 0}
return max(cats, key=cats.get) if cats else “PTS”

# ── Confidence tier label ──────────────────────────────────────────────────────

def conf_tier(edge: float) -> str:
if edge >= EDGE_LOCK:
return “LOCK”
if edge >= EDGE_LEAN:
return “LEAN”
if edge >= EDGE_DART:
return “DART”
return “SKIP”

# ── Detect games ───────────────────────────────────────────────────────────────

def detect_games(df: pd.DataFrame) -> list:
games = set()
opp_map = dict(zip(df[“Team”], df[“Opp”]))
for team in df[“Team”].dropna().unique():
opp = opp_map.get(team)
if opp and (opp, team) not in games:
games.add((team, opp))
return list(games)

# ── Format player dict ─────────────────────────────────────────────────────────

def fmt_player(row) -> dict:
bc = best_cat(row)
return {
“name”:       row[“Name”],
“team”:       row[“Team”],
“opp”:        row.get(“Opp”, “”),
“pos”:        row.get(“Pos”, “”),
“dvp”:        int(row[“DVP”]),
“proj”:       round(row[“Projection”], 1),
“val”:        round(row.get(“Value”, 0), 1),
“mins”:       round(row.get(“MINS”, 0), 1),
“pts”:        round(row.get(“PTS”, 0), 1),
“reb”:        round(row.get(“REB”, 0), 1),
“ast”:        round(row.get(“AST”, 0), 1),
“stl”:        round(row.get(“STL”, 0), 1),
“blk”:        round(row.get(“BLK”, 0), 1),
“threepm”:    round(row.get(“3PM”, 0), 1),
“pr”:         round(row.get(“PR”, 0), 1),
“pa”:         round(row.get(“PA”, 0), 1),
“ra”:         round(row.get(“RA”, 0), 1),
“pra”:        round(row.get(“PRA”, 0), 1),
“best_cat”:   bc,
“best_val”:   round(row.get(bc, 0), 1),
“conf_score”: round(row[“conf_score”], 2),
“usg”:        round(row.get(“USG”, 0), 1),
}

# ── Same-Game Power4s ──────────────────────────────────────────────────────────

def build_same_game_p4s(df: pd.DataFrame, games: list) -> list:
results = []
for t1, t2 in games:
gdf = df[df[“Team”].isin([t1, t2])].copy()
gdf = gdf.sort_values(“conf_score”, ascending=False)

```
    spread = gdf["Spread"].iloc[0] if len(gdf) else "N/A"
    total  = gdf["Total"].iloc[0]  if len(gdf) else "N/A"
    ou     = gdf["OU"].iloc[0]     if len(gdf) else "N/A"

    alpha = gdf.head(4)
    alt   = gdf.iloc[4:8]

    results.append({
        "game":       f"{t1} vs {t2}",
        "spread":     spread,
        "total":      total,
        "ou":         ou,
        "alpha":      [fmt_player(r) for _, r in alpha.iterrows()],
        "alpha_conf": round(alpha["conf_score"].sum(), 1),
        "alpha_proj": round(alpha["Projection"].sum(), 1),
        "alpha_dvp":  round(alpha["DVP"].mean(), 0),
        "alt":        [fmt_player(r) for _, r in alt.iterrows()],
        "alt_conf":   round(alt["conf_score"].sum(), 1) if len(alt) else 0,
        "alt_proj":   round(alt["Projection"].sum(), 1) if len(alt) else 0,
    })

results.sort(key=lambda x: x["alpha_conf"], reverse=True)
return results
```

# ── Slip builder (2–5 legs, category diverse) ─────────────────────────────────

def build_slips(df: pd.DataFrame) -> dict:
“””
Build 2–5 leg slips scored by:
- conf_score (DVP × projection × value)
- category diversity (no two players with identical best_cat)
- same-game correlation bonus (high-total games)
No hard player appearance cap — if edge is there, the slip is there.
“””

```
# Pool: must have meaningful projection and good DVP
pool = df[
    (df["Projection"] >= MIN_PROJ) &
    (df["DVP"] >= MIN_DVP)
].copy().sort_values("conf_score", ascending=False)

# Limit pool to top 40 to keep combos manageable
pool = pool.head(40).reset_index(drop=True)

slips_by_legs: dict = {n: [] for n in range(2, 6)}
seen_keys: set = set()

for n_legs in range(2, 6):
    for combo in combinations(pool.itertuples(), n_legs):
        # Category diversity: no duplicate best_cat
        cats = [best_cat(dict(zip(pool.columns, c[1:])) 
                         if hasattr(c, '_fields') else _row_to_dict(c, pool)) 
                for c in combo]

        # Build row dicts cleanly
        rows = [_ituple_to_dict(c) for c in combo]
        cats = [best_cat(r) for r in rows]

        if len(set(cats)) < n_legs:
            continue  # skip if any category repeats

        # Dedup by sorted player names
        names = tuple(sorted(r["Name"] for r in rows))
        if names in seen_keys:
            continue
        seen_keys.add(names)

        # Score: sum of conf_scores + correlation bonus
        total_conf = sum(r["conf_score"] for r in rows)
        avg_total  = sum(float(r.get("Total", 0)) for r in rows) / n_legs
        corr_bonus = 0.5 if avg_total >= 230 else 0.0  # high-total game bonus

        slip_score = round(total_conf + corr_bonus, 2)

        # Payout info
        payout = PAYOUTS.get(n_legs, 1)

        players = []
        for r in rows:
            bc = best_cat(r)
            players.append({
                "name":     r["Name"],
                "team":     r["Team"],
                "opp":      r.get("Opp", ""),
                "dvp":      int(r["DVP"]),
                "proj":     round(r["Projection"], 1),
                "cat":      bc,
                "cat_val":  round(r.get(bc, 0), 1),
                "conf":     round(r["conf_score"], 2),
                "mins":     round(r.get("MINS", 0), 1),
                "usg":      round(r.get("USG", 0), 1),
                "total":    r.get("Total", 0),
            })

        # Overall tier based on average conf per player
        avg_conf = total_conf / n_legs
        if avg_conf >= 8:
            tier = "LOCK"
        elif avg_conf >= 5:
            tier = "LEAN"
        else:
            tier = "DART"

        slips_by_legs[n_legs].append({
            "legs":       n_legs,
            "score":      slip_score,
            "tier":       tier,
            "payout":     payout,
            "avg_dvp":    round(sum(r["DVP"] for r in rows) / n_legs, 1),
            "avg_proj":   round(sum(r["Projection"] for r in rows) / n_legs, 1),
            "players":    players,
        })

    # Sort and keep top 15 per leg count
    slips_by_legs[n_legs].sort(key=lambda x: x["score"], reverse=True)
    slips_by_legs[n_legs] = slips_by_legs[n_legs][:15]

return slips_by_legs
```

def _ituple_to_dict(ituple) -> dict:
“”“Convert itertuples row to plain dict.”””
d = ituple._asdict()
d.pop(“Index”, None)
return d

# ── Category Leaders ───────────────────────────────────────────────────────────

def build_category_leaders(df: pd.DataFrame, games: list) -> list:
cat_labels = {
“PTS”: “Points”, “AST”: “Assists”, “REB”: “Rebounds”,
“STL”: “Steals”, “BLK”: “Blocks”, “3PM”: “3-Pointers”,
“PR”:  “Pts+Reb”, “PA”: “Pts+Ast”, “RA”: “Reb+Ast”, “PRA”: “Pts+Reb+Ast”,
}
result = []
for t1, t2 in games:
gdf = df[df[“Team”].isin([t1, t2])].copy()
if gdf.empty:
continue
spread = gdf[“Spread”].iloc[0]
total  = gdf[“Total”].iloc[0]
ou     = gdf[“OU”].iloc[0]
game_cats = []
for col, label in cat_labels.items():
if col not in gdf.columns or gdf[col].sum() == 0:
continue
top = gdf.nlargest(5, col)
game_cats.append({
“category”: label,
“col”:      col,
“leaders”: [
{
“name”: r[“Name”],
“team”: r[“Team”],
“dvp”:  int(r[“DVP”]),
“val”:  round(r[col], 1),
“conf”: round(r[“conf_score”], 1),
}
for _, r in top.iterrows()
],
})
result.append({
“game”:       f”{t1} vs {t2}”,
“spread”:     spread,
“total”:      total,
“ou”:         ou,
“categories”: game_cats,
})
return result

# ── Top Locks ──────────────────────────────────────────────────────────────────

def build_top_locks(df: pd.DataFrame) -> list:
“”“Top players by conf_score with full stat breakdown.”””
top = df.nlargest(12, “conf_score”)
locks = []
for _, r in top.iterrows():
bc = best_cat(r)
locks.append({
“name”:     r[“Name”],
“team”:     r[“Team”],
“opp”:      r.get(“Opp”, “”),
“dvp”:      int(r[“DVP”]),
“proj”:     round(r[“Projection”], 1),
“val”:      round(r.get(“Value”, 0), 1),
“mins”:     round(r.get(“MINS”, 0), 1),
“usg”:      round(r.get(“USG”, 0), 1),
“pts”:      round(r.get(“PTS”, 0), 1),
“reb”:      round(r.get(“REB”, 0), 1),
“ast”:      round(r.get(“AST”, 0), 1),
“stl”:      round(r.get(“STL”, 0), 1),
“blk”:      round(r.get(“BLK”, 0), 1),
“threepm”:  round(r.get(“3PM”, 0), 1),
“pra”:      round(r.get(“PRA”, 0), 1),
“best_cat”: bc,
“best_val”: round(r.get(bc, 0), 1),
“conf”:     round(r[“conf_score”], 2),
“total”:    round(r.get(“Total”, 0), 1),
“spread”:   r.get(“Spread”, “N/A”),
})
return locks

# ── Value Plays ────────────────────────────────────────────────────────────────

def build_value_plays(df: pd.DataFrame) -> list:
“”“Players with high DVP AND high value score — underpriced edges.”””
vdf = df[(df[“DVP”] >= 22) & (df[“Value”] >= 5.5) & (df[“Projection”] >= 25)].copy()
vdf = vdf.sort_values([“DVP”, “Value”], ascending=False).head(10)
result = []
for _, r in vdf.iterrows():
bc = best_cat(r)
result.append({
“name”:     r[“Name”],
“team”:     r[“Team”],
“opp”:      r.get(“Opp”, “”),
“dvp”:      int(r[“DVP”]),
“proj”:     round(r[“Projection”], 1),
“val”:      round(r.get(“Value”, 0), 1),
“salary”:   int(r.get(“Salary”, 0)),
“best_cat”: bc,
“best_val”: round(r.get(bc, 0), 1),
“conf”:     round(r[“conf_score”], 2),
“total”:    round(r.get(“Total”, 0), 1),
})
return result

# ── Main entry ─────────────────────────────────────────────────────────────────

def run_daily_scrape(output_path: str):
os.makedirs(os.path.dirname(output_path), exist_ok=True)
os.makedirs(“data”, exist_ok=True)

```
raw_df = fetch_projections_csv()
df = normalize_columns(raw_df)
df = build_combo_stats(df)
df = df.dropna(subset=["Name", "Team", "Opp"])
df.to_csv(FALLBACK_CSV, index=False)

games = detect_games(df)

same_game   = build_same_game_p4s(df, games)
slips       = build_slips(df)
cat_leaders = build_category_leaders(df, games)
top_locks   = build_top_locks(df)
value_plays = build_value_plays(df)

# Serialize slips (keys must be strings for JSON)
slips_out = {str(k): v for k, v in slips.items()}

report = {
    "generated_at": datetime.now().strftime("%B %d, %Y at %I:%M %p ET"),
    "slate_date":   datetime.now().strftime("%A, %B %d"),
    "game_count":   len(games),
    "same_game_p4": same_game,
    "slips":        slips_out,
    "category_leaders": cat_leaders,
    "top_locks":    top_locks,
    "value_plays":  value_plays,
}

with open(output_path, "w") as f:
    json.dump(report, f, indent=2)

logger.info(f"✅ Report saved — {len(games)} games, slips built")
return report
```
