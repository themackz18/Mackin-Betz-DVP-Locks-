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

SIM_RUNS = 5000
PAYOUTS = {4: 10, 6: 25, 8: 100}

def load_prizepicks_lines():
    lines = {}
    if not os.path.exists(APIFY_CSV):
        logger.warning(f"Apify CSV not found at {APIFY_CSV}")
        return lines
    try:
        df = pd.read_csv(APIFY_CSV)
        logger.info(f"Loaded {len(df)} Apify rows")
        for _, row in df.iterrows():
            name = str(row.get("player_name", "")).strip()
            if not name: continue
            stat_raw = str(row.get("stat", "")).upper().strip()
            stat_map = {"POINTS": "PTS", "REBOUNDS": "REB", "ASSISTS": "AST", "PTS+REBS+ASTS": "PRA", "FANTASY SCORE": "PRA"}
            stat = stat_map.get(stat_raw, stat_raw.replace(" ", "_").replace("+", ""))
            try:
                line = float(row.get("line", 0))
                if line > 0:
                    lines.setdefault(name, {})[stat] = line
            except:
                continue
        logger.info(f"Extracted lines for {len(lines)} players")
        return lines
    except Exception as e:
        logger.error(f"Apify CSV error: {e}")
        return {}

def simulate_hit_rate(proj, line, std):
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
        base_proj = min(float(r.get("Projection", 0)), 32)
        dvp = float(r.get("DVP", 15))
        team = r.get("Team", "N/A")
        opp = r.get("Opp", "N/A")
        stats = {"PTS": min(base_proj, 32), "REB": float(r.get("REB", base_proj * 0.28)), "AST": float(r.get("AST", base_proj * 0.24)), "PRA": base_proj * 1.55}
        for stat, base in stats.items():
            line = (lines.get(name) or {}).get(stat) or (base * 1.05)
            if base < 3: continue
            proj = base * dvp_boost(dvp)
            std_dev = max(2.5, proj * 0.26)
            hit = simulate_hit_rate(proj, line, std_dev)
            edge = (proj - line) / line if line > 0 else 0
            confidence = min(10, int(hit * 10 + 1.5))
            recommended_pick = "OVER" if edge > 3 else "UNDER" if edge < -3 else "EVEN"
            players.append({
                "name": name, "team": team, "opp": opp, "game": f"{team} vs {opp}",
                "stat": stat, "line": round(line, 1), "proj": round(proj, 1),
                "edge": round(edge * 100, 1), "hit_rate": round(hit * 100, 1),
                "dvp": round(dvp, 1), "confidence": confidence,
                "recommended_pick": recommended_pick,
                "pts": round(stats.get("PTS", 0), 1),
                "reb": round(stats.get("REB", 0), 1),
                "ast": round(stats.get("AST", 0), 1),
                "l5_pra": round(base * 1.08, 1),
                "matchup_grade": {"grade": "A" if dvp >= 20 else "B" if dvp >= 15 else "C", "color": "#10b981" if dvp >= 18 else "#f59e0b"}
            })
    logger.info(f"Built {len(players)} props")
    return players

def rank_props(players):
    overs = sorted(players, key=lambda x: x["hit_rate"], reverse=True)
    unders = sorted([p for p in players if p["edge"] < 0], key=lambda x: x["edge"])
    return overs[:50], unders[:25]   # ← increased for more visibility

def build_slips(players, size):
    # Deduplicate so same player never appears twice in one slip
    slips = []
    for combo in combinations(players[:60], size):   # increased pool
        names = [p["name"] for p in combo]
        if len(set(names)) < size: continue   # ← no duplicates
        prob = 1.0
        for p in combo:
            prob *= (p["hit_rate"] / 100.0)
        ev = prob * PAYOUTS.get(size, 10)
        slips.append({
            "players": names,
            "total_proj": round(sum(p["proj"] for p in combo), 1),
            "win_prob": round(prob * 100, 2),
            "ev": round(ev, 2),
            "target_prop": "PRA"
        })
    return sorted(slips, key=lambda x: x["ev"], reverse=True)[:12]   # more slips

def create_cheatsheet(top_over, top_under):
    # (unchanged - your existing function)
    img = Image.new("RGB", (1050, 920), (20, 20, 28))
    draw = ImageDraw.Draw(img)
    try:
        font_title = ImageFont.truetype("arial.ttf", 48)
        font = ImageFont.truetype("arial.ttf", 27)
        font_small = ImageFont.truetype("arial.ttf", 23)
    except:
        font_title = font = font_small = ImageFont.load_default()
    draw.text((50, 40), "MACKIN BETZ CHEATSHEET", fill="#c026d3", font=font_title)
    draw.text((50, 100), datetime.now().strftime("%B %d, %Y • NBA Props"), fill="#94a3b8", font=font_small)
    y = 170
    draw.text((50, y), "🔥 TOP OVERS", fill="#4ade80", font=font)
    y += 50
    for p in top_over[:9]:
        pick = p["recommended_pick"][0]
        color = "#4ade80" if pick == "O" else "#f87171"
        draw.text((50, y), f"{p['name'][:20]}  •  {p['stat']} {pick}{p['line']}", fill=color, font=font)
        draw.text((720, y), f"{p['hit_rate']}%   {p['confidence']}/10   Edge {p['edge']}%", fill="#fcd34d", font=font_small)
        y += 40
    y += 30
    draw.text((50, y), "❄️ STRONG UNDERS", fill="#f87171", font=font)
    y += 50
    for p in top_under[:8]:
        pick = p["recommended_pick"][0]
        color = "#4ade80" if pick == "O" else "#f87171"
        draw.text((50, y), f"{p['name'][:20]}  •  {p['stat']} {pick}{p['line']}", fill=color, font=font)
        draw.text((720, y), f"{p['hit_rate']}%   {p['confidence']}/10   Edge {p['edge']}%", fill="#fcd34d", font=font_small)
        y += 40
    os.makedirs(DATA_DIR, exist_ok=True)
    img.save(OUTPUT_IMG)

def run_daily_scrape():
    # (your existing logic with the new build_players, rank_props, build_slips above)
    # ... paste the rest of run_daily_scrape exactly as it was in the previous full version ...
    # (the try block, df = pd.read_csv, lines = load_prizepicks_lines(), players = build_players, etc.)
    # I kept it short here for space — use the exact same run_daily_scrape from my last full processor.py
    pass  # ← replace this line with your full run_daily_scrape function from before
