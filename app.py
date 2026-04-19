import os
import json
import logging
import requests
from datetime import datetime
from flask import Flask, render_template, jsonify, redirect, send_from_directory

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

REPORT_URL = "https://raw.githubusercontent.com/themackz18/Mackin-Betz-DVP-Locks/main/data/mackin_report.json"
DATA_DIR = "data"

os.makedirs(DATA_DIR, exist_ok=True)

def load_report():
    """Load the latest report from the main repo (pushed by test site)."""
    try:
        r = requests.get(REPORT_URL, timeout=15)
        if r.status_code == 200:
            report = r.json()
            logger.info(f"Loaded fresh report with {len(report.get('top_overs', []))} props")
            return report
        else:
            logger.warning(f"Failed to load report: {r.status_code}")
    except Exception as e:
        logger.error(f"Error loading report from GitHub: {e}")

    # Fallback empty report
    return {
        "generated_at": datetime.now().isoformat(),
        "game_count": 0,
        "same_game_p4": [],
        "slips": {"2": [], "3": [], "4": [], "5": []},
        "category_leaders": [],
        "top_locks": [],
        "value_plays": [],
        "top_overs": [],
        "top_unders": [],
        "ev_unders": []
    }

@app.route("/")
def index():
    report = load_report()
    return render_template("index.html", report=report)

@app.route("/refresh")
def refresh():
    # Just reload from GitHub (test site does the actual refresh)
    logger.info("Refresh requested - pulling latest from main repo")
    return redirect("/")

@app.route("/api/report")
def api_report():
    report = load_report()
    return jsonify(report)

@app.route('/data/<path:filename>')
def serve_data(filename):
    """Serve cheatsheet and other static files if they exist locally"""
    try:
        return send_from_directory(DATA_DIR, filename)
    except FileNotFoundError:
        # Fallback: try to serve cheatsheet from main repo if needed
        if filename == "mackin_cheatsheet.png":
            try:
                r = requests.get(f"https://raw.githubusercontent.com/themackz18/Mackin-Betz-DVP-Locks/main/data/mackin_cheatsheet.png", timeout=10)
                if r.status_code == 200:
                    return r.content, 200, {'Content-Type': 'image/png'}
            except:
                pass
        logger.warning(f"File not found: data/{filename}")
        return "File not found", 404

@app.route("/health")
def health():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "using_live_report": True
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"Starting Mackin Betz DVP Locks reader on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
