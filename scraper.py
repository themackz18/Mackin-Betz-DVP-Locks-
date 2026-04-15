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
    url = "https://api.prizepicks.com/projections?league_id=7&per_page=250&single_stat=true"
    resp = requests.get(url, timeout=15)
    data = resp.json()

    players = {}
    id_map = {}

    for i in data["included"]:
        if i["type"] == "new_player":
            id_map[i["id"]] = i["attributes"]["name"]

    for p in data["data"]:
        attr = p["attributes"]
        pid  = p["relationships"]["new_player"]["data"]["id"]

        name = id_map.get(pid)
        stat = attr["stat_type"].upper().replace(" ", "_")
        line = attr["line_score"]

        if name:
            players.setdefault(name, {})[stat] = float(line)

    return players

# ================================
# MONTE CARLO
# ================================

def simulate_hit_rate(proj, line, std):
    sims = np.random.normal(proj, std, SIM_RUNS)
    return np.mean(sims > line)

# ================================
# LAST 5 FORM (CSV BASED)
# ================================

def apply_last5_boost(row):
    l5 = row.get("L5_PTS", row.get("PTS", 0))
    season = row.get("PTS", 0)

    if season == 0:
        return 1.0

    ratio = l5 / season
    return min(max(ratio, 0.8), 1.2)  # clamp boost

# ================================
# DVP BOOST
# ================================

def dvp_boost(dvp_rank):
    # assume 1-30 ranking (30 = best matchup)
    if dvp_rank >= 25:
        return 1.15
    elif dvp_rank >= 20:
        return 1.08
    elif dvp_rank <= 10:
        return 0.90
    return 1.0

# ================================
# BUILD PLAYERS
# ================================

def build_players(df, lines):
    players = []

    for _, r in df.iterrows():
        name = r["Name"]

        pts = r.get("PTS", 0)
        reb = r.get("REB", 0)
        ast = r.get("AST", 0)
        pra = pts + reb + ast

        dvp = r.get("DVP", 15)

        stats = {
            "PTS": pts,
            "REB": reb,
            "AST": ast,
            "PRA": pra
        }

        for stat, base_proj in stats.items():
            line = (lines.get(name) or {}).get(stat)

            if not line or base_proj <= 0:
                continue

            # APPLY BOOSTS
            proj = base_proj
            proj *= apply_last5_boost(r)
            proj *= dvp_boost(dvp)

            std_dev = max(3, proj * 0.25)

            hit = simulate_hit_rate(proj, line, std_dev)
            edge = (proj - line) / line

            players.append({
                "name": name,
                "team": r.get("Team"),
                "opp": r.get("Opp"),
                "stat": stat,
                "proj": round(proj, 1),
                "line": line,
                "hit_rate": round(hit * 100, 1),
                "edge": round(edge * 100, 1),
                "dvp": dvp
            })

    return players

# ================================
# RANKING
# ================================

def rank_props(players):
    overs = sorted(players, key=lambda x: x["hit_rate"], reverse=True)
    unders = sorted(players, key=lambda x: x["edge"])

    return overs[:10], unders[:10]

# ================================
# SLIP BUILDER
# ================================

def build_slips(players, size):
    slips = []

    for combo in combinations(players[:20], size):
        names = [p["name"] for p in combo]

        if len(set(names)) < size:
            continue

        prob = 1
        for p in combo:
            prob *= (p["hit_rate"] / 100)

        payout = PAYOUTS[size]
        ev = prob * payout

        slips.append({
            "players": combo,
            "win_prob": round(prob * 100, 2),
            "ev": round(ev, 2)
        })

    return sorted(slips, key=lambda x: x["ev"], reverse=True)[:5]

# ================================
# CHEAT SHEET IMAGE
# ================================

def create_cheatsheet(top_over, top_under):
    img = Image.new("RGB", (1200, 1600), (10, 10, 10))
    draw = ImageDraw.Draw(img)

    try:
        font_title = ImageFont.truetype("arial.ttf", 60)
        font = ImageFont.truetype("arial.ttf", 32)
    except:
        font_title = font = ImageFont.load_default()

    # Title
    draw.text((50, 50), "MACKIN BETS", fill=(180, 0, 255), font=font_title)

    y = 150

    draw.text((50, y), "TOP OVERS", fill=(255,255,255), font=font)
    y += 50

    for p in top_over[:8]:
        text = f"{p['name']} {p['stat']} O {p['line']} | {p['hit_rate']}%"
        draw.text((50, y), text, fill=(0,255,150), font=font)
        y += 40

    y += 40
    draw.text((50, y), "TOP UNDERS", fill=(255,255,255), font=font)
    y += 50

    for p in top_under[:8]:
        text = f"{p['name']} {p['stat']} U {p['line']} | {p['edge']}%"
        draw.text((50, y), text, fill=(255,100,100), font=font)
        y += 40

    img.save(OUTPUT_IMG)

# ================================
# MAIN
# ================================

def run_daily():
    df = pd.read_csv(FALLBACK_CSV)

    lines = fetch_prizepicks()

    players = build_players(df, lines)

    top_over, top_under = rank_props(players)

    power4 = build_slips(top_over, 4)
    power6 = build_slips(top_over, 6)
    power8 = build_slips(top_over, 8)

    report = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "top_overs": top_over,
        "top_unders": top_under,
        "power4": power4,
        "power6": power6,
        "power8": power8
    }

    os.makedirs("data", exist_ok=True)

    with open(OUTPUT_JSON, "w") as f:
        json.dump(report, f, indent=2)

    create_cheatsheet(top_over, top_under)

    return report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_daily()
