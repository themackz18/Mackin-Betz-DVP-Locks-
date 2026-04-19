import os
import json
import logging
import requests
from datetime import datetime
from flask import Flask, render_template, redirect, url_for

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

APIFY_TOKEN = os.getenv("APIFY_TOKEN", "")
ACTOR_ID = "zen-studio~prizepicks-player-props"
DATA_DIR = "data"

app = Flask(__name__)

def fetch_from_apify():
    if not APIFY_TOKEN:
        return []
    url = f"https://api.apify.com/v2/acts/{ACTOR_ID}/run-sync-get-dataset-items?token={APIFY_TOKEN}&timeout=300"
    payload = {"leagues": ["NBA"]}
    try:
        resp = requests.post(url, json=payload, timeout=300)
        if resp.status_code not in (200, 201):
            logger.error(f"Apify status {resp.status_code}")
            return []
        return resp.json()
    except Exception as e:
        logger.error(f"Apify error: {e}")
        return []

def get_report():
    os.makedirs(DATA_DIR, exist_ok=True)
    raw_props = fetch_from_apify()
    total = len(raw_props)

    report = {
        "total_props": total,
        "generated_at": datetime.now().isoformat(),
        "leagues": {
            "NBA": {
                "display_name": "NBA",
                "prop_count": total,
                "top_props": raw_props[:50] if raw_props else []  # First 50 for display
            }
        }
    }
    return report

@app.route("/")
def index():
    report = get_report()
    return render_template("index.html", report=report)

@app.route("/refresh")
def refresh():
    get_report()
    return redirect("/")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8081))
    app.run(host="0.0.0.0", port=port)
