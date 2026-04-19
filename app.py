import os
import pandas as pd
import json
import logging
import requests
from datetime import datetime
from flask import Flask, render_template, redirect

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

CSV_URL = "https://raw.githubusercontent.com/themackz18/Mackin-Betz-DVP-Locks/main/data/fallback.csv"

def build_report_from_csv():
    try:
        df = pd.read_csv(CSV_URL)
        logger.info(f"Loaded {len(df)} rows from fallback.csv")
    except Exception as e:
        logger.error(f"Failed to load CSV: {e}")
        df = pd.DataFrame()

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
                line = proj  # fallback line = projection for now
                if line < 2:
                    continue
                props.append({
                    "name": name,
                    "stat": stat,
                    "line": round(line, 1),
                    "proj": round(proj, 1),
                    "confidence": 6,
                    "recommended_pick": "OVER"
                })
        except:
            continue

    report = {
        "generated_at": datetime.now().isoformat(),
        "game_count": len(df),
        "top_overs": props,
        "top_locks": props[:15],
        "value_plays": props[:12],
        "slips": {"2": []},
        "same_game_p4": [],
        "category_leaders": [],
        "top_unders": [],
        "ev_unders": []
    }
    return report

@app.route("/")
def index():
    report = build_report_from_csv()
    return render_template("index.html", report=report)

@app.route("/refresh")
def refresh():
    logger.info("Refresh requested - reloading CSV")
    return redirect("/")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
