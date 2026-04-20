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

DATA_DIR = "data"
FALLBACK_CSV = os.getenv("FALLBACK_CSV", "data/fallback.csv")          # Your lineups.com CSV (kept untouched)
APIFY_CSV   = os.getenv("APIFY_CSV", "data/apify_prizepicks.csv")      # New Apify export

OUTPUT_JSON = "data/mackin_report.json"
OUTPUT_IMG  = "data/mackin_cheatsheet.png"

SIM_RUNS = 5000
PAYOUTS = {4: 10, 6: 25, 8: 100}

def load_prizepicks_lines():
    """Load fresh PrizePicks lines from the Apify CSV you downloaded"""
    lines = {}
    if not os.path.exists(APIFY_CSV):
        logger.warning(f"Apify CSV not found at {APIFY_CSV}. Will use default lines.")
        return lines

    try:
        df = pd.read_csv(APIFY_CSV)
        logger.info(f"Loaded {len(df)} rows from Apify PrizePicks CSV")

        for _, row in df.iterrows():
            name = str(row.get("player_name", "")).strip()
            if not name:
                continue

            # Clean stat name
            stat_raw = str(row.get("stat", row.get("stat_short", ""))).upper().strip()
            stat_map = {
                "POINTS": "PTS",
                "REBOUNDS": "REB",
                "ASSISTS": "AST",
                "PTS+REBS+ASTS": "PRA",
                "FANTASY SCORE": "PRA",
                "PTS+REBS": "PRA",
                "PTS+ASTS": "PRA",
                "REBS+ASTS": "PRA",
                "POINTS (COMBO)": "PTS",
                "REBOUNDS (COMBO)": "REB",
                "ASSISTS (COMBO)": "AST",
            }
            stat = stat_map.get(stat_raw, stat_raw.replace(" ", "_").replace("+", "").replace("(", "").replace(")", ""))

            try:
                line = float(row.get("line", 0))
                if line > 0:
                    lines.setdefault(name, {})[stat] = line
            except (ValueError, TypeError):
                continue

        logger.info(f"Extracted PrizePicks lines for {len(lines)} players from Apify")
        return lines
    except Exception as e:
        logger.error(f"Failed to load Apify CSV: {e}")
        return {}

def fetch_prizepicks():
    """Fallback if Apify CSV is missing"""
    # Keep your original fallback if needed, but we now prefer the CSV
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

        base_proj = float(r.get("Projection", 0))
        base_proj = min(base_proj, 32)

        dvp = float(r.get("DVP", 15))
        team = r.get("Team", "N/A")
        opp = r.get("Opp", "N/A")
        game_key = f"{team} vs {opp}"

        stats = {
            "PTS": min(base_proj, 32),
            "REB": float(r.get("REB", base_proj * 0.28)),
            "AST": float(r.get("AST", base_proj * 0.24)),
            "PRA": base_proj * 1.55
        }

        for stat, base in stats.items():
            # Use Apify line if available, otherwise fallback
            line = (lines.get(name) or {}).get(stat) or (base * 1.05)
            if base < 3: continue

            proj = base * dvp_boost(dvp)
            std_dev = max(2.5, proj * 0.26)
            hit = simulate_hit_rate(proj, line, std_dev)
            edge = (proj - line) / line if line > 0 else 0

            confidence = min(10, int(hit * 10 + 1.5))
            recommended_pick = "OVER" if edge > 3 else "UNDER" if edge < -3 else "EVEN"

            players.append({
                "name": name,
                "team": team,
                "opp": opp,
                "game": game_key,
                "stat": stat,
                "line": round(line, 1),
                "proj": round(proj, 1),
                "edge": round(edge * 100, 1),
                "hit_rate": round(hit * 100, 1),
                "dvp": round(dvp, 1),
                "confidence": confidence,
                "recommended_pick": recommended_pick,
                "pts": round(stats.get("PTS", 0), 1),
                "reb": round(stats.get("REB", 0), 1),
                "ast": round(stats.get("AST", 0), 1),
                "l5_pra": round(base * 1.08, 1),
                "matchup_grade": {"grade": "A" if dvp >= 20 else "B" if dvp >= 15 else "C", "color": "#10b981" if dvp >= 18 else "#f59e0b"}
            })
    logger.info(f"Built {len(players)} player props using Apify lines where available")
    return players

def rank_props(players):
    overs = sorted(players, key=lambda x: x["hit_rate"], reverse=True)
    unders = sorted([p for p in players if p["edge"] < 0], key=lambda x: x["edge"])
    return overs[:25], unders[:15]

def build_slips(players, size):
    slips = []
    for combo in combinations(players[:40], size):
        if len(set(p["name"] for p in combo)) < size: continue
        prob = 1.0
        for p in combo:
            prob *= (p["hit_rate"] / 100.0)
        ev = prob * PAYOUTS.get(size, 10)
        slips.append({
            "players": [p["name"] for p in combo],
            "total_proj": round(sum(p["proj"] for p in combo), 1),
            "win_prob": round(prob * 100, 2),
            "ev": round(ev, 2),
            "target_prop": "PRA"
        })
    return sorted(slips, key=lambda x: x["ev"], reverse=True)[:10]

def create_cheatsheet(top_over, top_under):
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
    logger.info(f"Cheatsheet saved to {OUTPUT_IMG}")

def run_daily_scrape():
    try:
        if not os.path.exists(FALLBACK_CSV):
            raise FileNotFoundError(f"Missing main CSV (lineups.com) at {FALLBACK_CSV}")

        df = pd.read_csv(FALLBACK_CSV)
        lines = load_prizepicks_lines()                    # ← Uses your new Apify CSV

        players = build_players(df, lines)
        top_over, top_under = rank_props(players)

        from collections import defaultdict
        game_groups = defaultdict(list)
        for p in top_over:
            game_key = p.get("game", "Main Slate")
            game_groups[game_key].append(p)

        same_game_p4 = [{"game": g, "alpha": ps} for g, ps in game_groups.items()]

        cat_map = {"PTS": [], "REB": [], "AST": [], "PRA": []}
        for p in top_over:
            if p["stat"] in cat_map:
                cat_map[p["stat"]].append(p)

        category_leaders = [{"category": cat, "players": cat_map[cat][:6]} for cat in ["PTS", "REB", "AST", "PRA"]]

        report = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "generated_at": datetime.now().isoformat(),
            "game_count": len(df),
            "slate_date": datetime.now().strftime("%Y-%m-%d"),
            "same_game_p4": same_game_p4,
            "slips": {
                "2": build_slips(top_over, 2),
                "3": build_slips(top_over, 3),
                "4": build_slips(top_over, 4),
                "5": build_slips(top_over, 5)
            },
            "category_leaders": category_leaders,
            "top_locks": [p for p in top_over if p["confidence"] >= 6][:15],
            "value_plays": [p for p in top_over if p["edge"] > 4][:15],
            "top_overs": top_over,
            "top_unders": top_under,
            "power4": build_slips(top_over, 4),
            "power6": build_slips(top_over, 6),
            "power8": build_slips(top_over, 8),
            "ev_unders": [p for p in top_under if p["edge"] < -5][:12]
        }

        os.makedirs(DATA_DIR, exist_ok=True)
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        create_cheatsheet(top_over, top_under)
        logger.info("✅ Report generated successfully (Apify lines merged with lineups.com data)")
        return report
    except Exception as e:
        logger.error(f"Processing failed: {e}", exc_info=True)
        raise
