import os
import json
import logging
from datetime import datetime
from flask import Flask, render_template, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from scraper import run_daily_scrape

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Ensure data directory exists at import time (works with gunicorn)
os.makedirs("data", exist_ok=True)

app = Flask(__name__)

DATA_FILE = "data/latest_report.json"

def scheduled_job():
    logger.info("⏰ Noon scrape triggered...")
    try:
        run_daily_scrape(DATA_FILE)
        logger.info("✅ Scrape + analysis complete.")
    except Exception as e:
        logger.error(f"❌ Scrape failed: {e}")

# Schedule daily at noon ET
scheduler = BackgroundScheduler(timezone="America/New_York")
scheduler.add_job(scheduled_job, "cron", hour=12, minute=0)
scheduler.start()

@app.route("/")
def index():
    report = load_report()
    return render_template("index.html", report=report)

@app.route("/api/report")
def api_report():
    return jsonify(load_report())

@app.route("/refresh")
def refresh():
    """Manual trigger for testing"""
    try:
        run_daily_scrape(DATA_FILE)
        return jsonify({"status": "ok", "message": "Report refreshed successfully!"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

def load_report():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE) as f:
            return json.load(f)
    return None

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
