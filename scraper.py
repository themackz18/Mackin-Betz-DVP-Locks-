import os
import json
import logging
import pandas as pd
import requests
import numpy as np
from itertools import combinations
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger(__name__)

# CONFIG
FALLBACK_CSV = "data/fallback.csv"
OUTPUT_JSON  = "data/mackin_report.json"
OUTPUT_IMG   = "data/mackin_cheatsheet.png"

SIM_RUNS = 5000
PAYOUTS = {4: 10, 6: 25, 8: 100}

def fetch_prizepicks():
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
        logger.info(f"Fetched {len(players)} players")
        return players
    except Exception as e:
        logger.error(f"PrizePicks error: {e}")
        return {}

def simulate_hit_rate(proj, line, std):
    sims = np.random.normal(proj, std, SIM_RUNS)
    return np.mean(sims > line)

def apply_last5_boost(row):
    l5 = row.get("L5_PTS", row.get("PTS", 0))
    season = row.get("PTS", 0)
    return 1.0 if season == 0 else min(max(l5 / season, 0.8), 1.2)

def dvp_boost(dvp_rank):
    if dvp_rank >= 25: return 1.15
    if dvp_rank >= 20: return 1.08
    if dvp_rank <= 10: return 0.90
    return 1.0

def build_players(df, lines):
    players = []
    for _, r in df.iterrows():
        name = r.get("Name")
        if not name: continue
        pts = r.get("PTS", 0)
        reb = r.get("REB", 0)
        ast = r.get("AST", 0)
        pra = pts + reb + ast
        dvp = r.get("DVP", 15)

        for stat, base in [("PTS", pts), ("REB", reb), ("AST", ast), ("PRA", pra)]:
            line = (lines.get(name) or {}).get(stat)
            if not line or base <= 0: continue

            proj = base * apply_last5_boost(r) * dvp_boost(dvp)
            std_dev = max(3, proj * 0.25)
            hit_rate = simulate_hit_rate(proj, line, std_dev)
            edge = (proj - line) / line

            players.append({
                "name": name,
                "team": r.get("Team"),
                "opp": r.get("Opp"),
                "stat": stat,
                "proj": round(proj, 1),
                "line": line,
                "hit_rate": round(hit_rate * 100, 1),
                "edge": round(edge * 100, 1),
                "dvp": dvp,
                "confidence": min(10, int(hit_rate * 10 + 2)),
                "target_prop": stat,
                "pts": pts, "reb": reb, "ast": ast,
                "l5_pra": round(r.get("L5_PTS",0) + r.get("L5_REB",0) + r.get("L5_AST",0),1),
                "matchup_grade": {"grade": "A" if dvp >= 25 else "B", "color": "#10b981"}
            })
    return players

def rank_props(players):
    overs = sorted(players, key=lambda x: x["hit_rate"], reverse=True)
    unders = sorted(players, key=lambda x: x["edge"])
    return overs[:12], unders[:12]

def build_slips(players, size):
    slips = []
    for combo in combinations(players[:20], size):
        if len({p["name"] for p in combo}) < size: continue
        prob = 1.0
        for p in combo: prob *= (p["hit_rate"] / 100)
        ev = prob * PAYOUTS.get(size, 10)
        slips.append({
            "players": [p["name"] for p in combo],
            "total_proj": round(sum(p["proj"] for p in combo), 1),
            "win_prob": round(prob * 100, 2),
            "ev": round(ev, 2),
            "target_prop": "PRA"
        })
    return sorted(slips, key=lambda x: x["ev"], reverse=True)[:6]

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
    logger.info(f"Cheatsheet saved: {OUTPUT_IMG}")

def run_daily_scrape(output_path=None):
    try:
        logger.info("Starting scrape...")
        if not os.path.exists(FALLBACK_CSV):
            raise FileNotFoundError(f"Missing {FALLBACK_CSV}")

        df = pd.read_csv(FALLBACK_CSV)
        lines = fetch_prizepicks()
        players = build_players(df, lines)
        top_over, top_under = rank_props(players)

        report = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "generated_at": datetime.now().isoformat(),
            "game_count": len(df),
            "slate_date": datetime.now().strftime("%Y-%m-%d"),
            "same_game_p4": [{"game": "Main Slate", "alpha": top_over[:8]}],
            "slips": {
                "2": build_slips(top_over, 2),
                "3": build_slips(top_over, 3),
                "4": build_slips(top_over, 4),
                "5": build_slips(top_over, 5)
            },
            "category_leaders": [
                {"category": "PTS", "players": sorted([p for p in top_over if p["stat"]=="PTS"], key=lambda x: x["proj"], reverse=True)[:5]},
                {"category": "REB", "players": sorted([p for p in top_over if p["stat"]=="REB"], key=lambda x: x["proj"], reverse=True)[:5]},
                {"category": "AST", "players": sorted([p for p in top_over if p["stat"]=="AST"], key=lambda x: x["proj"], reverse=True)[:5]},
                {"category": "PRA", "players": top_over[:5]}
            ],
            "top_locks": [p for p in top_over if p["confidence"] >= 8][:12],
            "value_plays": [p for p in top_over if p["edge"] > 8][:12],
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
        logger.info("✅ Scrape completed")
        return report
    except Exception as e:
        logger.error(f"Scrape failed: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_daily_scrape()
