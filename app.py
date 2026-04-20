import os
import logging
from datetime import datetime
from flask import Flask, render_template, redirect, url_for, send_from_directory

from processor import run_daily_scrape, OUTPUT_JSON, OUTPUT_IMG, DATA_DIR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

def get_report():
    os.makedirs(DATA_DIR, exist_ok=True)
    
    try:
        report = run_daily_scrape()
        logger.info("✅ Report generated successfully")
    except Exception as e:
        logger.error(f"Processing failed: {e}", exc_info=True)
        report = {
            "error": str(e),
            "generated_at": datetime.now().isoformat(),
            "game_count": 0,
            "top_overs": [],
            "top_unders": [],
            "top_locks": [],
            "value_plays": [],
            "ev_unders": [],
            "same_game_p4": [],
            "slips": {"2": [], "3": [], "4": [], "5": []},
            "category_leaders": []
        }
    
    return {
        "full_report": report,
        "generated_at": datetime.now().isoformat()
    }

@app.route("/")
def index():
    report = get_report()
    return render_template("index.html", report=report)

@app.route("/refresh")
def refresh():
    get_report()
    return redirect("/")

@app.route("/cheatsheet")
def cheatsheet():
    return send_from_directory(DATA_DIR, os.path.basename(OUTPUT_IMG), mimetype='image/png')

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
