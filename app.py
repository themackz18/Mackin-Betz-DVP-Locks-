import os
import json
import logging
from datetime import datetime
from flask import Flask, render_template, jsonify, redirect, url_for, send_from_directory

# Import the scraper
from scraper import run_daily_scrape

# ============================
# LOGGING SETUP
# ============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ============================
# CONFIG
# ============================
REPORT_PATH = "data/mackin_report.json"
DATA_DIR = "data"

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)


def load_report():
    """Load existing report from JSON file."""
    if os.path.exists(REPORT_PATH):
        try:
            with open(REPORT_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading report: {e}")
    return None


def get_or_build_report(force_refresh=False):
    """Load report or build a new one if missing or forced."""
    if force_refresh or not os.path.exists(REPORT_PATH):
        logger.info("Building fresh report...")
        try:
            report = run_daily_scrape(REPORT_PATH)
            logger.info("✅ Report built successfully")
            return report
        except Exception as e:
            logger.error(f"❌ Failed to build report: {e}", exc_info=True)
            return {
                "date": datetime.now().strftime("%Y-%m-%d"),
                "generated_at": datetime.now().isoformat(),
                "error": str(e),
                "game_count": 0,
                "same_game_p4": [],
                "slips": {"2": [], "3": [], "4": [], "5": []},
                "category_leaders": [],
                "top_locks": [],
                "value_plays": [],
                "top_overs": [],
                "top_unders": [],
                "power4": [], "power6": [], "power8": [],
                "ev_unders": []
            }

    report = load_report()
    if report:
        logger.info(f"Loaded existing report")
        return report

    return get_or_build_report(force_refresh=True)


@app.route("/")
def index():
    report = get_or_build_report()
    return render_template("index.html", report=report)


@app.route("/refresh")
def refresh():
    try:
        run_daily_scrape(REPORT_PATH)
        logger.info("✅ Report refreshed")
    except Exception as e:
        logger.error(f"Refresh failed: {e}")
    return redirect(url_for("index"))


@app.route("/api/report")
def api_report():
    report = get_or_build_report()
    return jsonify(report)


@app.route('/data/<path:filename>')
def serve_data(filename):
    """Serve the cheatsheet PNG and any other files from data folder"""
    try:
        return send_from_directory(DATA_DIR, filename)
    except FileNotFoundError:
        logger.warning(f"File not found: data/{filename}")
        return "File not found", 404


@app.route("/health")
def health():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "report_exists": os.path.exists(REPORT_PATH)
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"Starting Mackin Betz app on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
