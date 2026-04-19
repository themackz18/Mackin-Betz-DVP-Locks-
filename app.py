import os
import pandas as pd
import logging
from datetime import datetime
from flask import Flask, render_template, redirect

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

CSV_PATH = "data/fallback.csv"   # lowercase as you confirmed

def build_report():
    if not os.path.exists(CSV_PATH):
        logger.error(f"CSV not found at {CSV_PATH}")
        return {"generated_at": datetime.now().isoformat(), "game_count": 0, "top_overs": [], "top_locks": [], "value_plays": [], "slips": {"2": []}, "same_game_p4": [], "category_leaders": []}

    try:
        df = pd.read_csv(CSV_PATH)
        logger.info(f"Loaded {len(df)} rows from CSV")
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        return {"generated_at": datetime.now().isoformat(), "game_count": 0, "top_overs": [], "top_locks": [], "value_plays": [], "slips": {"2": []}, "same_game_p4": [], "category_leaders": []}

    props = []
    for _, row in df.iterrows():
        try:
            name = str(row.get("Name", "")).strip()
            if not name:
                continue
            proj = float(row.get("Projection", 0) or 0)
            team = str(row.get("Team", "N/A")).strip()
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
                    "ast": round(proj * 0.25, 1)
                })
        except:
            continue

    report = {
        "generated_at": datetime.now().isoformat(),
        "game_count": len(df),
        "top_overs": props,
        "top_locks": props[:15],
        "value_plays": props[:12],
        "slips": {"2": [{"players": props[:2]}]},
        "same_game_p4": [],
        "category_leaders": [
            {"category": "PTS", "players": props[:6]},
            {"category": "REB", "players": props[6:12]},
            {"category": "AST", "players": props[12:18]}
        ],
        "top_unders": [],
        "ev_unders": []
    }
    return report

@app.route("/")
def index():
    report = build_report()
    return render_template("index.html", report=report)

@app.route("/refresh")
def refresh():
    return redirect("/")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
