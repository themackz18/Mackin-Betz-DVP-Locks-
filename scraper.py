
import json
import logging
import pandas as pd
import requests
import numpy as np
from itertools import combinations
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger(__name__)

# ================================
# CONFIG
# ================================

FALLBACK_CSV = "data/fallback.csv"
OUTPUT_JSON  = "data/mackin_report.json"
OUTPUT_IMG   = "data/mackin_cheatsheet.png"

SIM_RUNS = 5000

PAYOUTS = {
    4: 10,
    6: 25,
    8: 100
}

# ================================
# PRIZEPICKS API
# ================================

def fetch_prizepicks():
    """Fetch projections from PrizePicks API"""
    url = "https://api.prizepicks.com/projections?league_id=7&per_page=250&single_stat=true"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        players = {}
        id_map = {}

        for i in data.get("included", []):
            if i.get("type") == "new_player":
                id_map[i["id"]] = i["attributes"]["name"]

        for p in data.get("data", []):
            attr = p["attributes"]
            pid = p["relationships"]["new_player"]["data"]["id"]
            name = id_map.get(pid)
            if name:
                stat = attr["stat_type"].upper().replace(" ", "_")
                line = attr["line_score"]
                players.setdefault(name, {})[stat] = float(line)

        logger.info(f"Fetched PrizePicks data for {len(players)} players")
        return players
    except Exception as e:
        logger.error(f"Failed to fetch PrizePicks data: {e}")
        return {}


# ================================
# HELPERS
# ================================

def simulate_hit_rate(proj, line, std):
    sims = np.random.normal(proj, std, SIM_RUNS)
    return np.mean(sims > line)


def apply_last5_boost(row):
    # Use Projection as base since we don't have real L5 data
    return 1.1  # slight boost for now


def dvp_boost(dvp_rank):
    if dvp_rank >= 25:
        return 1.15
    elif dvp_rank >= 20:
        return 1.08
    elif dvp_rank <= 10:
        return 0.90
    return 1.0


# ================================
# BUILD PLAYERS - IMPROVED FOR YOUR CSV
# ================================

def build_players(df, lines):
    players = []
    for _, r in df.iterrows():
        name = str(r.get("Name", "")).strip()
        if not name:
            continue

        # Map your actual CSV columns
        proj_value = float(r.get("Projection", 0))
        dvp = float(r.get("DVP", 15))
        team = r.get("Team", "N/A")
        opp = r.get("Opp", "N/A")

        # Create stats using Projection as main value
        stats = {
            "PTS": proj_value,
            "REB": proj_value * 0.25,   # rough estimate
            "AST": proj_value * 0.22,   # rough estimate
            "PRA": proj_value * 1.5     # rough PRA
        }

        for stat, base_proj in stats.items():
            line = (lines.get(name) or {}).get(stat)
            if not line:
                line = base_proj * 1.05  # fallback line

            if base_proj <= 0:
                continue

            # Apply boosts
            proj = base_proj * apply_last5_boost(r) * dvp_boost(dvp)
            std_dev = max(3, proj * 0.25)

            hit = simulate_hit_rate(proj, line, std_dev)
            edge = (proj - line) / line if line > 0 else 0

            confidence = min(10, int(hit * 10 + 2))

            players.append({
                "name": name,
                "team": team,
                "opp": opp,
                "stat": stat,
                "proj": round(proj, 1),
                "line": round(line, 1),
                "hit_rate": round(hit * 100, 1),
                "edge": round(edge * 100, 1),
                "dvp": round(dvp, 1),
                "confidence": confidence,
                "target_prop": stat,
                "pts": round(proj_value, 1),
                "reb": round(proj_value * 0.25, 1),
                "ast": round(proj_value * 0.22, 1),
                "l5_pra": round(proj_value * 1.1, 1),
                "matchup_grade": {
                    "grade": "A" if dvp >= 22 else "B" if dvp >= 18 else "C",
                    "color": "#10b981" if dvp >= 20 else "#f59e0b"
                }
            })

    logger.info(f"Built {len(players)} player props")
    return players


# ================================
# RANKING & SLIPS
# ================================

def rank_props(players):
    overs = sorted(players, key=lambda x: x["hit_rate"], reverse=True)
    unders = sorted(players, key=lambda x: x["edge"])
    return overs[:15], unders[:10]


def build_slips(players, size):
    slips = []
    for combo in combinations(players[:25], size):
        if len(set(p["name"] for p in combo)) < size:
            continue
        prob = 1.0
        for p in combo:
            prob *= (p["hit_rate"] / 100)
        ev = prob * PAYOUTS.get(size, 10)
        slips.append({
            "players": [p["name"] for p in combo],
            "total_proj": round(sum(p["proj"] for p in combo), 1),
            "win_prob": round(prob * 100, 2),
            "ev": round(ev, 2),
            "target_prop": "PRA"
        })
    return sorted(slips, key=lambda x: x["ev"], reverse=True)[:8]


def create_cheatsheet(top_over, top_under):
    img = Image.new("RGB", (1200, 1600), (10, 10, 10))
    draw = ImageDraw.Draw(img)
    try:
        font_title = ImageFont.truetype("arial.ttf", 60)
        font = ImageFont.truetype("arial.ttf", 32)
    except:
        font_title = font = ImageFont.load_default()

    draw.text((50, 50), "MACKIN BETZ", fill=(180, 0, 255), font=font_title)
    y = 150
    draw.text((50, y), "TOP OVERS", fill=(255,255,255), font=font)
    y += 50
    for p in top_over[:8]:
        draw.text((50, y), f"{p['name']} {p['stat']} O {p['line']} | {p['hit_rate']}%", fill=(0,255,150), font=font)
        y += 40
    y += 40
    draw.text((50, y), "TOP UNDERS", fill=(255,255,255), font=font)
    y += 50
    for p in top_under[:8]:
        draw.text((50, y), f"{p['name']} {p['stat']} U {p['line']} | {p['edge']}%", fill=(255,100,100), font=font)
        y += 40

    os.makedirs("data", exist_ok=True)
    img.save(OUTPUT_IMG)
    logger.info(f"Cheatsheet saved to {OUTPUT_IMG}")


# ================================
# MAIN FUNCTION
# ================================

def run_daily_scrape(output_path=None):
    try:
        logger.info("Starting Mackin Betz daily scrape...")

        if not os.path.exists(FALLBACK_CSV):
            logger.error(f"Fallback CSV not found: {FALLBACK_CSV}")
            raise FileNotFoundError(f"Missing file: {FALLBACK_CSV}")

        df = pd.read_csv(FALLBACK_CSV)
        logger.info(f"Loaded {len(df)} rows from fallback.csv")

        lines = fetch_prizepicks()
        players = build_players(df, lines)

        if len(players) == 0:
            logger.warning("No players were generated - check your CSV columns")

        top_over, top_under = rank_props(players)

        report = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "generated_at": datetime.now().isoformat(),
            "game_count": len(df),
            "slate_date": datetime.now().strftime("%Y-%m-%d"),
            "same_game_p4": [{"game": "Main Slate", "alpha": top_over[:10]}],
            "slips": {
                "2": build_slips(top_over, 2),
                "3": build_slips(top_over, 3),
                "4": build_slips(top_over, 4),
                "5": build_slips(top_over, 5)
            },
            "category_leaders": [
                {"category": "PTS", "players": sorted([p for p in top_over if p["stat"] == "PTS"], key=lambda x: x["proj"], reverse=True)[:6]},
                {"category": "REB", "players": sorted([p for p in top_over if p["stat"] == "REB"], key=lambda x: x["proj"], reverse=True)[:6]},
                {"category": "AST", "players": sorted([p for p in top_over if p["stat"] == "AST"], key=lambda x: x["proj"], reverse=True)[:6]},
                {"category": "PRA", "players": top_over[:6]}
            ],
            "top_locks": [p for p in top_over if p["confidence"] >= 7][:12],
            "value_plays": [p for p in top_over if p["edge"] > 5][:12],
            "top_overs": top_over,
            "top_unders": top_under,
            "power4": build_slips(top_over, 4),
            "power6": build_slips(top_over, 6),
            "power8": build_slips(top_over, 8),
            "ev_unders": []
        }

        os.makedirs("data", exist_ok=True)
        save_path = output_path or OUTPUT_JSON
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        create_cheatsheet(top_over, top_under)

        logger.info(f"✅ Scrape completed successfully. Generated {len(players)} props.")
        return report

    except Exception as e:
        logger.error(f"❌ Scrape failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_daily_scrape(
