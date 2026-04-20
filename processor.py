import os
import json
import logging
import pandas as pd
import requests
import numpy as np
from itertools import combinations
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger(__name__)

DATA_DIR = "data"
FALLBACK_CSV = os.getenv("FALLBACK_CSV", "data/fallback.csv")          # lineups.com
APIFY_CSV   = os.getenv("APIFY_CSV", "data/apify_prizepicks.csv")      # new Apify file

OUTPUT_JSON = "data/mackin_report.json"
OUTPUT_IMG  = "data/mackin_cheatsheet.png"

SIM_RUNS = 5000
PAYOUTS = {4: 10, 6: 25, 8: 100}

def load_prizepicks_lines():
    """Load fresh PrizePicks lines from Apify CSV if available, else fallback to old method"""
    lines = {}
    if os.path.exists(APIFY_CSV):
        try:
            df_apify = pd.read_csv(APIFY_CSV)
            logger.info(f"Loaded {len(df_apify)} rows from Apify PrizePicks CSV")
            
            # Apify PrizePicks scraper typically has columns like: name, stat_type, line_score, etc.
            # Adjust column names based on your actual Apify CSV (open it and check headers)
            for _, row in df_apify.iterrows():
                name = str(row.get("name", row.get("player", ""))).strip()
                if not name:
                    continue
                stat = str(row.get("stat_type", "")).upper().replace(" ", "_")
                line = float(row.get("line_score", row.get("line", 0)))
                if name and stat and line > 0:
                    lines.setdefault(name, {})[stat] = line
            logger.info(f"Extracted PrizePicks lines for {len(lines)} players from Apify")
        except Exception as e:
            logger.error(f"Failed to load Apify CSV: {e}")
    return lines

def run_daily_scrape():
    try:
        if not os.path.exists(FALLBACK_CSV):
            raise FileNotFoundError(f"Missing main CSV at {FALLBACK_CSV}")

        df = pd.read_csv(FALLBACK_CSV)                    # Your lineups.com data
        lines = load_prizepicks_lines()                   # Fresh lines from Apify (preferred)

        players = build_players(df, lines)                # Uses Apify lines when available
        # ... rest of your original function stays exactly the same ...

        # (copy the rest of run_daily_scrape from the previous full processor.py I gave you)
        # including rank_props, build_slips, create_cheatsheet, report dict, json save, image, etc.

    except Exception as e:
        logger.error(f"Processing failed: {e}", exc_info=True)
        raise
