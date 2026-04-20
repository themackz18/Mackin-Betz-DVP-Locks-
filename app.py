import os
import logging
from datetime import datetime
from flask import Flask, render_template, redirect, url_for, send_from_directory

# Import the processor
from processor import run_daily_scrape, OUTPUT_JSON, OUTPUT_IMG, DATA_DIR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

def get_report():
    os.makedirs(DATA_DIR, exist_ok=True)
    
    try:
        report = run_daily_scrape()
        logger.info("✅ Report generated from uploaded CSV")
    except Exception as e:
        logger.error(f"CSV processing failed: {e}")
        report = {
            "error": str(e),
            "generated_at": datetime.now().isoformat()
        }

    total = len(report.get("top_overs", [])) if isinstance(report, dict) else 0

    web_report = {
        "total_props": total,
        "generated_at": datetime.now().isoformat(),
        "leagues": {
            "NBA": {
                "display_name": "NBA",
                "prop_count": total,
                "top_props": report.get("top_overs", [])[:50] if isinstance(report, dict) else []
            }
        },
        "full_report": report
    }
    return web_report

@app.route("/")
def index():
    report = get_report()
    return render_template("index.html", report=report)

@app.route("/refresh")
def refresh():
    get_report()   # Re-process the CSV
    return redirect("/")

@app.route("/cheatsheet")
def cheatsheet():
    return send_from_directory(DATA_DIR, os.path.basename(OUTPUT_IMG))

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
