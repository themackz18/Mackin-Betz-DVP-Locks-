from flask import Flask, render_template, jsonify
import os
import json
from scraper import run_daily_scrape

app = Flask(__name__)

REPORT_PATH = "data/latest_report.json"


@app.route("/")
def index():
    report = None
    if os.path.exists(REPORT_PATH):
        try:
            with open(REPORT_PATH, "r") as f:
                report = json.load(f)
        except:
            report = None
    return render_template("index.html", report=report)


@app.route("/refresh")
def refresh():
    try:
        run_daily_scrape(REPORT_PATH)
        return jsonify({"message": "Report refreshed successfully!", "status": "ok"})
    except Exception as e:
        return jsonify({"message": str(e), "status": "error"}), 500


@app.route("/api/report")
def api_report():
    if os.path.exists(REPORT_PATH):
        with open(REPORT_PATH, "r") as f:
            report = json.load(f)
        return jsonify(report)
    return jsonify({"message": "No report data available yet.", "status": "error"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
