import os
import json
import logging
import pandas as pd
import numpy as np
from itertools import combinations
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger(__name__)

DATA_DIR = "data"
FALLBACK_CSV = os.getenv("FALLBACK_CSV", "data/fallback.csv")
APIFY_CSV   = os.getenv("APIFY_CSV", "data/apify_prizepicks.csv")

OUTPUT_JSON = "data/mackin_report.json"
OUTPUT_IMG  = "data/mackin_cheatsheet.png"

SIM_RUNS = 1000
PAYOUTS = {4: 10, 6: 25, 8: 100}

def load_prizepicks_lines():
    lines = {}
    if not os.path.exists(APIFY_CSV):
        logger.warning(f"Apify CSV not found: {APIFY_CSV}")
        return lines
    try:
        df = pd.read_csv(APIFY_CSV)
        logger.info(f"Loaded {len(df)} rows from Apify CSV")
        stat_map = {
            "POINTS": "PTS", "REBOUNDS": "REB", "ASSISTS": "AST",
            "PTS+REBS+ASTS": "PRA", "FANTASY SCORE": "PRA",
            "PRA": "PRA"
        }
        for _, row in df.iterrows():
            name = str(row.get("player_name", row.get("Name", ""))).strip()
            if not name: continue
            stat_raw = str(row.get("stat", row.get("market", ""))).upper().strip()
            stat = stat_map.get(stat_raw, stat_raw.replace(" ", "_").replace("+", ""))
            try:
                line = float(row.get("line", row.get("Line", 0)))
                if line > 0:
                    lines.setdefault(name, {})[stat] = line
            except:
                continue
        logger.info(f"Extracted posted lines for {len(lines)} players")
        return lines
    except Exception as e:
        logger.error(f"Apify load error: {e}")
        return {}

def simulate_hit_rate(proj, line, std=3.5):
    sims = np.random.normal(proj, std, SIM_RUNS)
    return np.mean(sims > line)

def dvp_boost(dvp_rank):
    if dvp_rank >= 25: return 1.12
    if dvp_rank >= 20: return 1.07
    if dvp_rank <= 10: return 0.93
    return 1.0

def build_players(df, lines):
    players = []
    for _, r in df.iterrows():
        name = str(r.get("Name", "")).strip()
        if not name: continue

        team = str(r.get("Team", "N/A"))
        opp = str(r.get("Opp", "N/A"))
        game_key = f"{team} vs {opp}"
        dvp = float(r.get("DVP", 15))

        # Base projections from fallback.csv (primary source)
        base_pts = float(r.get("Projection", 0)) or float(r.get("PTS", 0))
        base_reb = float(r.get("REB", base_pts * 0.28))
        base_ast = float(r.get("AST", base_pts * 0.24))
        base_pra = base_pts * 1.55

        for stat, base_proj in [("PTS", base_pts), ("REB", base_reb), ("AST", base_ast), ("PRA", base_pra)]:
            if base_proj < 4: continue

            # Overlay real posted line from Apify if available
            posted_line = lines.get(name, {}).get(stat)
            line = posted_line if posted_line else (base_proj * 1.05)

            proj = base_proj * dvp_boost(dvp)
            std_dev = max(2.8, proj * 0.27)
            hit_rate = simulate_hit_rate(proj, line, std_dev)
            edge = ((proj - line) / line * 100) if line > 0 else 0

            confidence = min(10, int(hit_rate * 10 + 1.5))
            rec_pick = "OVER" if edge > 3 else "UNDER" if edge < -3 else "EVEN"

            players.append({
                "name": name,
                "team": team,
                "opp": opp,
                "game": game_key,
                "stat": stat,                    # This should now show PTS / REB / AST / PRA correctly
                "line": round(line, 1),
                "proj": round(proj, 1),
                "edge": round(edge, 1),
                "hit_rate": round(hit_rate * 100, 1),
                "dvp": round(dvp, 1),
                "confidence": confidence,
                "recommended_pick": rec_pick
            })
            if len(players) > 160:
                break
    logger.info(f"Built {len(players)} props from fallback + Apify lines")
    return players

# The rest of the functions stay the same (rank_props, build_slips, create_cheatsheet, run_daily_scrape)
# ... (copy the rest from the previous optimized processor.py I sent you)

def run_daily_scrape():
    try:
        df = pd.read_csv(FALLBACK_CSV)
        logger.info(f"Loaded fallback.csv with {len(df)} rows")
        lines = load_prizepicks_lines()
        players = build_players(df, lines)
        top_overs, top_unders = rank_props(players)   # You need to add this function too if missing

        # Build report structure...
        report = {
            "generated_at": datetime.now().isoformat(),
            "same_game_p4": [],  # populate as before
            "slips": {"2": build_slips(top_overs, 2), "3": build_slips(top_overs, 3), "4": build_slips(top_overs, 4), "5": build_slips(top_overs, 5)},
            "top_overs": top_overs[:60],
            "top_unders": top_unders,
            "top_locks": [p for p in top_overs if p["confidence"] >= 6][:25],
        }
        # Save JSON and cheatsheet...
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(OUTPUT_JSON, "w") as f:
            json.dump(report, f, indent=2)
        create_cheatsheet(top_overs, top_unders)
        logger.info("✅ Report generated successfully")
        return report
    except Exception as e:
        logger.error(f"run_daily_scrape failed: {e}")
        raise

# Add missing helper functions if not present (rank_props, build_slips, create_cheatsheet)
def rank_props(players):
    overs = sorted(players, key=lambda x: x["hit_rate"], reverse=True)[:60]
    unders = sorted([p for p in players if p["edge"] < 0], key=lambda x: x["edge"])[:30]
    return overs, unders

def build_slips(players, size):
    candidates = sorted(players, key=lambda x: x["hit_rate"], reverse=True)[:22]
    slips = []
    for combo in combinations(candidates, size):
        names = [p["name"] for p in combo]
        if len(set(names)) < size: continue
        prob = 1.0
        for p in combo:
            prob *= (p["hit_rate"] / 100.0)
        ev = prob * PAYOUTS.get(size, 10)
        slips.append({
            "players": names,
            "total_proj": round(sum(p["proj"] for p in combo), 1),
            "win_prob": round(prob * 100, 1),
            "ev": round(ev, 2)
        })
    return sorted(slips, key=lambda x: x["ev"], reverse=True)[:8]

def create_cheatsheet(top_over, top_under):
    # (keep the same PIL code from previous version)
    pass  # paste your existing create_cheatsheet here
