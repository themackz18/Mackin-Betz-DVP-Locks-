import os
import json
import logging
import pandas as pd
import numpy as np
from itertools import combinations
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont
from collections import defaultdict

logger = logging.getLogger(__name__)

DATA_DIR = "data"
FALLBACK_CSV = os.getenv("FALLBACK_CSV", "data/fallback.csv")
APIFY_CSV   = os.getenv("APIFY_CSV", "data/apify_prizepicks.csv")

OUTPUT_JSON = "data/mackin_report.json"
OUTPUT_IMG  = "data/mackin_cheatsheet.png"

SIM_RUNS = 1200
PAYOUTS = {4: 10, 6: 25, 8: 100}

# ====================== PLAYOFF ADJUSTMENT ======================
PLAYOFF_MULTIPLIER = 0.83   # Stronger reduction for playoff minutes/pace
# ============================================================

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
            "PTS+REBS+ASTS": "PRA", "PRA": "PRA",
            "FANTASY SCORE": "FS", "FANTASY": "FS",
            "FIELD GOALS MADE": "FG", "FG MADE": "FG",
            "FIELD GOAL ATTEMPTS": "FGA", "FGA": "FGA",
            "DEFENSIVE REBOUNDS": "DREB", "DREB": "DREB",
            "DUNKS": "DUNKS", "DUNK": "DUNKS"
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

def simulate_hit_rate(proj, line, std=3.2):
    if line <= 0: return 0.5
    sims = np.random.normal(proj, std, SIM_RUNS)
    return np.mean(sims > line)

def dvp_boost(dvp):
    if dvp >= 24: return 1.08
    if dvp >= 19: return 1.05
    if dvp <= 11: return 0.95
    return 1.0

def get_matchup_grade(dvp):
    if dvp >= 23: return {"grade": "A+", "color": "#10b981"}
    if dvp >= 19: return {"grade": "A", "color": "#10b981"}
    if dvp >= 15: return {"grade": "B+", "color": "#f59e0b"}
    if dvp >= 11: return {"grade": "B", "color": "#f59e0b"}
    return {"grade": "C", "color": "#ef4444"}

def build_players(df, lines):
    players = []
    for _, r in df.iterrows():
        name = str(r.get("Name", "")).strip()
        if not name: continue
        team = str(r.get("Team", "N/A"))
        opp = str(r.get("Opp", "N/A"))
        game_key = f"{team} vs {opp}"
        dvp = float(r.get("DVP", 15.0))

        # Base from fallback.csv → strong playoff adjustment
        base_pts = float(r.get("Projection", 0)) or float(r.get("PTS", 0))
        base_reb = float(r.get("REB", base_pts * 0.32))
        base_ast = float(r.get("AST", base_pts * 0.26))
        base_pra = base_pts + base_reb + base_ast
        base_fs  = base_pra * 1.08 + float(r.get("STL", 1.0)) * 3 + float(r.get("BLK", 0.7)) * 3

        stat_bases = {
            "PTS": min(base_pts, 35),
            "REB": min(base_reb, 18),
            "AST": min(base_ast, 15),
            "PRA": min(base_pra, 58),
            "FS":  min(base_fs,  52),
            "FG":  min(base_pts / 2.3, 11),
            "FGA": min(base_pts / 1.9, 17),
            "DREB": min(base_reb * 0.74, 13),
            "DUNKS": min(float(r.get("DUNKS", 0.6)), 3.5)
        }

        for stat, base_proj in stat_bases.items():
            if base_proj < 3.5 and stat not in ["DUNKS", "FG", "FGA"]: continue
            if base_proj < 0.4 and stat == "DUNKS": continue

            posted = lines.get(name, {}).get(stat)
            line = posted if posted and posted > 0 else round(base_proj * 1.03, 1)

            proj = round(base_proj * PLAYOFF_MULTIPLIER * dvp_boost(dvp), 1)

            std = max(2.4, proj * 0.29)
            hit_rate = simulate_hit_rate(proj, line, std)
            edge = round(((proj - line) / line * 100) if line > 0 else 0, 1)

            confidence = min(10, int(hit_rate * 10 + 1.2))
            rec = "OVER" if edge > 7 else "UNDER" if edge < -7 else "EVEN"
            grade = get_matchup_grade(dvp)

            players.append({
                "name": name,
                "team": team,
                "opp": opp,
                "game": game_key,
                "stat": stat,
                "line": line,
                "proj": proj,
                "edge": edge,
                "hit_rate": round(hit_rate * 100, 1),
                "dvp": round(dvp, 1),
                "confidence": confidence,
                "recommended_pick": rec,
                "matchup_grade": grade
            })
            if len(players) > 190: break
    logger.info(f"Built {len(players)} playoff-adjusted props")
    return players

def rank_props(players):
    overs = sorted(players, key=lambda x: x["hit_rate"], reverse=True)[:70]
    unders = sorted([p for p in players if p["edge"] < 0], key=lambda x: x["edge"])[:35]
    return overs, unders

def build_slips(players, size):
    candidates = sorted(players, key=lambda x: (x["hit_rate"], x["edge"]), reverse=True)[:24]
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
    return sorted(slips, key=lambda x: x["ev"], reverse=True)[:10]

def create_cheatsheet(top_over, top_under):
    img = Image.new("RGB", (1100, 980), (20, 20, 28))
    draw = ImageDraw.Draw(img)
    try:
        font_title = ImageFont.truetype("arial.ttf", 46)
        font = ImageFont.truetype("arial.ttf", 26)
        small = ImageFont.truetype("arial.ttf", 22)
    except:
        font_title = font = small = ImageFont.load_default()
    draw.text((60, 40), "MACKIN BETZ CHEATSHEET", fill="#c026d3", font=font_title)
    draw.text((60, 110), datetime.now().strftime("%B %d, %Y • NBA PLAYOFFS"), fill="#94a3b8", font=small)
    y = 180
    draw.text((60, y), "🔥 TOP OVERS", fill="#4ade80", font=font)
    y += 55
    for p in top_over[:12]:
        draw.text((60, y), f"{p['name'][:23]} • {p['stat']} OVER {p['line']}", fill="#4ade80", font=font)
        draw.text((740, y), f"{p['hit_rate']}% Conf {p['confidence']}/10 Edge {p['edge']}%", fill="#fcd34d", font=small)
        y += 42
    y += 40
    draw.text((60, y), "❄️ STRONG UNDERS", fill="#f87171", font=font)
    y += 55
    for p in top_under[:9]:
        draw.text((60, y), f"{p['name'][:23]} • {p['stat']} UNDER {p['line']}", fill="#f87171", font=font)
        draw.text((740, y), f"{p['hit_rate']}% Conf {p['confidence']}/10 Edge {p['edge']}%", fill="#fcd34d", font=small)
        y += 42
    os.makedirs(DATA_DIR, exist_ok=True)
    img.save(OUTPUT_IMG)

def run_daily_scrape():
    try:
        df = pd.read_csv(FALLBACK_CSV)
        logger.info(f"Loaded fallback.csv with {len(df)} rows")
        lines = load_prizepicks_lines()
        players = build_players(df, lines)
        top_overs, top_unders = rank_props(players)

        # Improved P4 grouping
        game_groups = defaultdict(list)
        for p in top_overs:
            if p["hit_rate"] >= 74 and p["confidence"] >= 6:
                game_groups[p["game"]].append(p)
        same_game_p4 = [{"game": g, "alpha": sorted(ps, key=lambda x: x["hit_rate"], reverse=True)[:6]} 
                        for g, ps in game_groups.items() if len(ps) >= 3]

        report = {
            "generated_at": datetime.now().isoformat(),
            "same_game_p4": same_game_p4[:10],
            "slips": {
                "2": build_slips(top_overs, 2),
                "3": build_slips(top_overs, 3),
                "4": build_slips(top_overs, 4),
                "5": build_slips(top_overs, 5)
            },
            "top_overs": top_overs,
            "top_unders": top_unders,
            "top_locks": [p for p in top_overs if p["confidence"] >= 7][:28],
            "ev_unders": [p for p in top_unders if p["edge"] < -7][:18]
        }

        os.makedirs(DATA_DIR, exist_ok=True)
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        create_cheatsheet(top_overs, top_unders)
        logger.info("✅ Report generated with stronger playoff adjustment")
        return report
    except Exception as e:
        logger.error(f"run_daily_scrape failed: {e}", exc_info=True)
        raise
