import os
import json
import logging
from flask import Flask, render_template, jsonify, redirect, url_for
from scraper import run_daily_scrape

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(**name**)

app = Flask(**name**)

REPORT_PATH = ‘data/report.json’

def load_report():
if os.path.exists(REPORT_PATH):
with open(REPORT_PATH) as f:
return json.load(f)
return None

def get_or_build_report():
report = load_report()
if report is None:
logger.info(‘No report found - building now’)
try:
report = run_daily_scrape(REPORT_PATH)
except Exception as e:
logger.error(‘Failed to build report: %s’, e)
report = {
‘generated_at’: ‘Not available’,
‘slate_date’: ‘’,
‘game_count’: 0,
‘same_game_p4’: [],
‘slips’: {‘2’: [], ‘3’: [], ‘4’: [], ‘5’: []},
‘category_leaders’: [],
‘top_locks’: [],
‘value_plays’: [],
}
return report

@app.route(’/’)
def index():
report = get_or_build_report()
return render_template(‘index.html’, report=report)

@app.route(’/refresh’)
def refresh():
try:
run_daily_scrape(REPORT_PATH)
logger.info(‘Report refreshed successfully’)
except Exception as e:
logger.error(‘Refresh failed: %s’, e)
return redirect(url_for(‘index’))

@app.route(’/api/report’)
def api_report():
report = get_or_build_report()
return jsonify(report)

if **name** == ‘**main**’:
port = int(os.environ.get(‘PORT’, 8080))
app.run(host=‘0.0.0.0’, port=port, debug=False)
