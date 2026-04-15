import os
import json
import logging
from datetime import datetime
from flask import Flask, render_template, jsonify, redirect, url_for

# Import the corrected scraper function
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
REPORT_PATH = "data/mackin_report.json"   # Consistent with scraper.py
DATA_DIR = "data"

# Ensure data directory exists at startup
os.makedirs(DATA_DIR, exist_ok=True)


def load_report():
    """Load existing report from JSON file."""
    if os.path.exists(REPORT_PATH):
        try:
            with open(REPORT_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            logger.error("Report JSON is corrupted")
        except Exception as e:
            logger.error(f"Error loading report: {e}")
    return None


def get_or_build_report(force_refresh=False):
    """
    Load report or build a new one if missing or forced.
    """
    if force_refresh or not os.path.exists(REPORT_PATH):
        logger.info("Building fresh report...")
        try:
            report = run_daily_scrape(REPORT_PATH)
            logger.info("✅ Report built and saved successfully")
            return report
        except Exception as e:
            logger.error(f"❌ Failed to build report: {e}", exc_info=True)
            # Return graceful fallback
            return {
                "date": datetime.now().strftime("%Y-%m-%d"),
                "generated_at": datetime.now().isoformat(),
                "error": f"Failed to generate report: {str(e)}",
                "top_overs": [],
                "top_unders": [],
                "power4": [], 
                "power6": [], 
                "power8": []
            }

    # Load existing report
    report = load_report()
    if report:
        logger.info(f"Loaded existing report from {REPORT_PATH}")
        return report

    # Fallback if loading fails
    logger.warning("Failed to load report, building new one")
    return get_or_build_report(force_refresh=True)


@app.route("/")
def index():
    """Main page - shows the betting cheatsheet"""
    report = get_or_build_report()
    return render_template("index.html", report=report)


@app.route("/refresh")
def refresh():
    """Force rebuild the report and redirect to home"""
    try:
        report = run_daily_scrape(REPORT_PATH)
        logger.info("✅ Manual refresh completed successfully")
    except Exception as e:
        logger.error(f"❌ Refresh failed: {e}", exc_info=True)
    
    return redirect(url_for("index"))


@app.route("/api/report")
def api_report():
    """JSON API endpoint for the report"""
    report = get_or_build_report()
    return jsonify(report)


@app.route("/api/refresh")
def api_refresh():
    """API endpoint to refresh report and return new data"""
    try:
        report = run_daily_scrape(REPORT_PATH)
        logger.info("✅ API refresh completed")
        return jsonify({
            "status": "success",
            "message": "Report refreshed",
            "report": report
        })
    except Exception as e:
        logger.error(f"❌ API refresh failed: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route("/health")
def health():
    """Simple health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "report_exists": os.path.exists(REPORT_PATH)
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"Starting Mackin Bets Flask app on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
