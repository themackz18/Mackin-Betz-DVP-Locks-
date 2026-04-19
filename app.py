import os
import pandas as pd
import logging
from datetime import datetime
from flask import Flask, render_template, redirect

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

CSV_PATH = "data/fallback.csv"

def build_report():
    if not os.path.exists(CSV_PATH):
        logger.error(f"CSV not found at {CSV_PATH}")
        return get_empty_report()

    try:
        df = pd.read_csv(CSV_PATH)
        logger.info(f"Loaded {len(df)} rows from CSV")
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        return get_empty_report()

    props = []
    for _, row in df.iterrows():
        try:
            name = str(row.get("Name", "")).strip()
            if not name:
                continue
            proj = float(row.get("Projection", 0) or 0)
            opp = str(row.get("Opp", "N/A")).strip()

            for stat in ["PTS", "REB", "AST"]:
                line = proj
                if line < 2:
                    continue
                props.append({
                    "name": name,
                    "stat": stat,
                    "line": round(line, 1),
                    "proj": round(proj, 1),
                    "confidence": 6,
                    "recommended_pick": "OVER",
                    "matchup_grade": {"grade": "B"},
                    "pts": round(proj, 1),
                    "reb": round(proj * 0.3, 1),
                    "ast": round(proj * 0.25, 1),
                    "target_prop": stat,
                    "dvp": 18.0,
                    "l5_pra": round(proj * 1.55, 1),
                    "opp": opp
                })
        except:
            continue

    p4_games = []
    if props:
        p4_games.append({
            "game": "Power4 Slate",
            "alpha": props[:4]
        })

    report = {
        "generated_at": datetime.now().isoformat(),
        "game_count": len(df),
        "same_game_p4": p4_games,
        "category_leaders": [
            {"category": "PTS", "players": props[:8]},
            {"category": "REB", "players": props[8:16]},
            {"category": "AST", "players": props[16:24]}
        ],
        "top_locks": props[:15],
        "value_plays": props[:15],
        "top_overs": props,
        "top_unders": [],
        "ev_unders": [],
        "slips": {
            "2": [{"players": props[:2], "total_proj": 55.0, "target_prop": "PRA"}],
            "3": [],
            "4": [],
            "5": []
        }
    }
    return report

def get_empty_report():
    return {
        "generated_at": datetime.now().isoformat(),
        "game_count": 0,
        "same_game_p4": [],
        "category_leaders": [],
        "top_locks": [],
        "value_plays": [],
        "top_overs": [],
        "top_unders": [],
        "ev_unders": [],
        "slips": {"2": [], "3": [], "4": [], "5": []}
    }

@app.route("/")
def index():
    report = build_report()
    return render_template("index.html", report=report)

@app.route("/refresh")
def refresh():
    logger.info("Refresh requested - reloading CSV")
    return redirect("/")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
