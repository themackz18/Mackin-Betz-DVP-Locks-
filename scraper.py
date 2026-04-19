import os
import json
import base64
import logging
import pandas as pd
import requests
import numpy as np
from itertools import combinations
from collections import defaultdict
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger(__name__)

FALLBACK_CSV = "data/fallback.csv"
OUTPUT_JSON = "data/mackin_report.json"
OUTPUT_IMG = "data/mackin_cheatsheet.png"
GITHUB_REPO = "themackz18/Mackin-Betz-DVP-Locks"

SIM_RUNS = 5000
PAYOUTS = {4: 10, 6: 25, 8: 100}


def push_to_github(file_path, commit_message):
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        logger.warning("GITHUB_TOKEN not set, skipping push")
        return

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{file_path}"

    # Get current SHA if file exists
    r = requests.get(url, headers=headers, timeout=15)
    sha = r.json().get("sha") if r.status_code == 200 else None

    with open(file_path, "rb") as f:
        content = base64.b64encode(f.read()).decode()

    payload = {
        "message": commit_message,
        "content": content,
        "branch": "main"
    }
    if sha:
        payload["sha"] = sha

    result = requests.put(url, headers=headers, json=payload, timeout=15)
    logger.info(f"GitHub push {file_path}: {result.status_code} ({result.reason})")


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
                id_map[i["id"]] = i["attributes"].get("name")

        for p in data.get("data", []):
            attr = p.get("attributes", {})
            rel = p.get("relationships", {}).get("new_player", {}).get("data", {})
            pid = rel.get("id")
            name = id_map.get(pid)
            if name and attr.get("stat_type") and attr.get("line_score") is not None:
                stat = attr["stat_type"].upper().replace(" ", "_")
                line = float(attr["line_score"])
                players.setdefault(name, {})[stat] = line

        logger.info(f"Fetched {len(players)} PrizePicks lines")
        return players
    except Exception as e:
        logger.error(f"PrizePicks fetch failed: {e}")
        return {}


def simulate_hit_rate(proj, line, std):
    if std <= 0:
        std = 2.5
    sims = np.random.normal(proj, std, SIM_RUNS)
    return np.mean(sims > line)


def dvp_boost(dvp_rank):
    if dvp_rank >= 25:
        return 1.12
    if dvp_rank >= 20:
        return 1.07
    if dvp_rank <= 10:
        return 0.93
    return 1.0


def build_players(df, lines):
    players = []
    for _, r in df.iterrows():
        name = str(r.get("Name", "")).strip().replace("\n", " ").replace('"', "")
        if not name:
            continue

        base_proj = min(float(r.get("Projection", 0)), 32)
        dvp = float(r.get("DVP", 15))
        team = str(r.get("Team", "N/A")).strip()
        opp = str(r.get("Opp", "N/A")).strip()
        game_key = f"{team} vs {opp}"

        stats = {
            "PTS": min(base_proj, 32),
            "REB": float(r.get("REB", base_proj * 0.28)),
            "AST": float(r.get("AST", base_proj * 0.24)),
            "PRA": base_proj * 1.55
        }

        for stat, base in stats.items():
            if base < 3:
                continue

            line = (lines.get(name) or {}).get(stat) or (base * 1.05)
            proj = base * dvp_boost(dvp)
            std_dev = max(2.5, proj * 0.26)

            hit = simulate_hit_rate(proj, line, std_dev)
            edge = (proj - line) / line if line > 0 else 0

            confidence = min(10, int(hit * 10 + 1.5))

            recommended_pick = (
                "OVER" if edge > 3 else
                "UNDER" if edge < -3 else
                "EVEN"
            )

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
                "matchup_grade": {
                    "grade": "A" if dvp >= 20 else "B" if dvp >= 15 else "C",
                    "color": "#10b981" if dvp >= 18 else "#f59e0b"
                }
            })

    logger.info(f"Built {len(players)} player props")
    return players


def rank_props(players):
    overs = sorted(players, key=lambda x: x["hit_rate"], reverse=True)
    unders = sorted(
        [p for p in players if p["edge"] < 0],
        key=lambda x: x["edge"]
    )
    return overs[:25], unders[:15]


def build_slips(players, size):
    if size > 5:  # Performance guard - combinations grow very fast
        logger.warning(f"build_slips called with large size {size}, limiting input")
        input_players = players[:30]
    else:
        input_players = players[:40]

    slips = []
    for combo in combinations(input_players, size):
        names = [p["name"] for p in combo]
        if len(set(names)) < size:  # Avoid duplicate players
            continue

        prob = 1.0
        for p in combo:
            prob *= p["hit_rate"] / 100.0

        ev = prob * PAYOUTS.get(size, 10)
        slips.append({
            "players": names,
            "total_proj": round(sum(p["proj"] for p in combo), 1),
            "win_prob": round(prob * 100, 2),
            "ev": round(ev, 2),
            "target_prop": "PRA"
        })

    return sorted(slips, key=lambda x: x["ev"], reverse=True)[:10]


def create_cheatsheet(top_over, top_under):
    img = Image.new("RGB", (1050, 920), (20, 20, 28))
    draw = ImageDraw.Draw(img)

    # Better font fallback for servers (common on Linux)
    try:
        font_title = ImageFont.truetype("arial.ttf", 48)
        font = ImageFont.truetype("arial.ttf", 27)
        font_small = ImageFont.truetype("arial.ttf", 23)
    except Exception:
        try:
            font_title = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 48)
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 27)
            font_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 23)
        except Exception:
            font_title = font = font_small = ImageFont.load_default()

    draw.text((50, 40), "MACKIN BETZ CHEATSHEET", fill="#c026d3", font=font_title)
    draw.text(
        (50, 100),
        datetime.now().strftime("%B %d, %Y - NBA Props"),
        fill="#94a3b8",
        font=font_small
    )

    y = 170
    draw.text((50, y), "TOP OVERS", fill="#4ade80", font=font)
    y += 50

    for p in top_over[:9]:
        pick_char = p["recommended_pick"][0]
        color = "#4ade80" if pick_char == "O" else "#f87171"
        draw.text(
            (50, y),
            f"{p['name'][:20]}  -  {p['stat']} {pick_char}{p['line']}",
            fill=color,
            font=font
        )
        draw.text(
            (720, y),
            f"{p['hit_rate']}%   {p['confidence']}/10",
            fill="#fcd34d",
            font=font_small
        )
        y += 40

    y += 30
    draw.text((50, y), "STRONG UNDERS", fill="#f87171", font=font)
    y += 50

    for p in top_under[:8]:
        pick_char = p["recommended_pick"][0]
        color = "#4ade80" if pick_char == "O" else "#f87171"
        draw.text(
            (50, y),
            f"{p['name'][:20]}  -  {p['stat']} {pick_char}{p['line']}",
            fill=color,
            font=font
        )
        draw.text(
            (720, y),
            f"{p['hit_rate']}%   {p['confidence']}/10",
            fill="#fcd34d",
            font=font_small
        )
        y += 40

    os.makedirs("data", exist_ok=True)
    img.save(OUTPUT_IMG)
    logger.info(f"Cheatsheet saved to {OUTPUT_IMG}")


def run_daily_scrape(output_path=None):
    try:
        if not os.path.exists(FALLBACK_CSV):
            raise FileNotFoundError(f"Missing fallback file: {FALLBACK_CSV}")

        df = pd.read_csv(FALLBACK_CSV)
        logger.info(f"Loaded {len(df)} rows from {FALLBACK_CSV}")

        lines = fetch_prizepicks()
        players = build_players(df, lines)
        top_over, top_under = rank_props(players)

        # Game grouping
        game_groups = defaultdict(list)
        for p in top_over:
            game_groups[p.get("game", "Main Slate")].append(p)

        same_game_p4 = [{"game": g, "alpha": ps} for g, ps in game_groups.items()]

        # Category leaders
        cat_map = {"PTS": [], "REB": [], "AST": [], "PRA": []}
        for p in top_over:
            if p["stat"] in cat_map:
                cat_map[p["stat"]].append(p)

        category_leaders = [
            {"category": cat, "players": cat_map[cat][:6]}
            for cat in ["PTS", "REB", "AST", "PRA"]
        ]

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

        os.makedirs("data", exist_ok=True)
        final_path = output_path or OUTPUT_JSON

        with open(final_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        create_cheatsheet(top_over, top_under)

        if os.environ.get("GITHUB_TOKEN"):
            push_to_github(FALLBACK_CSV, "Update fallback CSV")
            push_to_github(final_path, "Update report JSON")
            push_to_github(OUTPUT_IMG, "Update cheatsheet image")

        logger.info("Daily scrape completed successfully")
        return report

    except Exception as e:
        logger.error(f"Scrape failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    run_daily_scrape()
