"""
app.py - Mackin Betz DVP Locks (Flask)
"""

from flask import Flask, render_template, jsonify
import os
from scraper import run_daily_scrape

app = Flask(__name__)

REPORT_PATH = "data/latest_report.json"


@app.route("/")
def index():
    if os.path.exists(REPORT_PATH):
        with open(REPORT_PATH, "r") as f:
            report = f.read()
        return render_template("index.html", report=report)
    else:
        return render_template("index.html", report=None)


@app.route("/refresh")
def refresh():
    try:
        report = run_daily_scrape(REPORT_PATH)
        return jsonify({"message": "Report refreshed successfully!", "status": "ok"})
    except Exception as e:
        return jsonify({"message": str(e), "status": "error"}), 500


@app.route("/api/report")
def api_report():
    if os.path.exists(REPORT_PATH):
        with open(REPORT_PATH, "r") as f:
            report = f.read()
        return jsonify(report)
    else:
        return jsonify({"message": "No report data available yet.", "status": "error"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
