import os
import json
import logging
from flask import Flask, render_template, jsonify, redirect, url_for

# Import your scraper (make sure scraper.py is in the same directory)
from scraper import run_daily_scrape

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Fixed: proper straight quotes
REPORT_PATH = "data/report.json"


def load_report():
    """Load existing report from JSON file."""
    if os.path.exists(REPORT_PATH):
        try:
            with open(REPORT_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading report: {e}")
    return None


def get_or_build_report():
    """Load report or build a new one if it doesn't exist."""
    report = load_report()
    if report is None:
        logger.info("No report found - building now")
        try:
            report = run_daily_scrape(REPORT_PATH)
        except Exception as e:
            logger.error(f"Failed to build report: {e}")
            report = {
                "generated_at": "Not available",
                "slate_date": "",
                "game_count": 0,
                "same_game_p4": [],
                "slips": {"2": [], "3": [], "4": [], "5": []},
                "category_leaders": [],
                "top_locks": [],
                "value_plays": [],
            }
    return report


@app.route("/")
def index():
    report = get_or_build_report()
    return render_template("index.html", report=report)


@app.route("/refresh")
def refresh():
    try:
        run_daily_scrape(REPORT_PATH)
        logger.info("Report refreshed successfully")
    except Exception as e:
        logger.error(f"Refresh failed: {e}")
    return redirect(url_for("index"))


@app.route("/api/report")
def api_report():
    report = get_or_build_report()
    return jsonify(report)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
