"""
scraper.py
Fetches NBA fantasy projections using Playwright (real browser) 
and runs the full Power 4 / multi-game analysis engine.
"""

import os
import json
import time
import logging
import random
import pandas as pd
from datetime import datetime
from itertools import combinations
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

logger = logging.getLogger(__name__)

LINEUPS_URL = "https://www.lineups.com/nba/nba-fantasy-basketball-projections"

# Fallback CSV (upload manually to data/fallback.csv when needed)
FALLBACK_CSV = "data/fallback.csv"


def fetch_projections_csv() -> pd.DataFrame:
    """Use Playwright (real browser) to scrape lineups.com and bypass 403 blocks."""
    logger.info("🚀 Launching Playwright to scrape lineups.com...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        try:
            # Step 1: Visit homepage first to set cookies/referer
            logger.info("Visiting homepage for cookies...")
            page.goto("https://www.lineups.com/nba", timeout=30000)
            page.wait_for_timeout(random.randint(2000, 4000))

            # Step 2: Go to projections page
            logger.info(f"Navigating to projections page: {LINEUPS_URL}")
            page.goto(LINEUPS_URL, timeout=45000)
            
            # Wait for JavaScript to load the table
            page.wait_for_timeout(random.randint(5000, 7000))

            # Get rendered HTML and parse tables
            content = page.content()
            soup = BeautifulSoup(content, "html.parser")
            tables = soup.find_all("table")

            logger.info(f"Found {len(tables)} tables on the page")

            for i, table in enumerate(tables):
                try:
                    df = pd.read_html(str(table))[0]
                    # Check if this looks like a projections table
                    if len(df) > 20 and any(col in str(df.columns).upper() 
                                           for col in ["PLAYER", "NAME", "PROJECTION", "SALARY", "FPTS"]):
                        logger.info(f"✅ SUCCESS: Playwright scraped table {i+1} with {len(df)} rows")
                        logger.info(f"Sample columns: {list(df.columns[:10])}")
                        browser.close()
                        return df
                except Exception as table_err:
                    logger.debug(f"Table {i} parse failed: {table_err}")
                    continue

        except Exception as e:
            logger.error(f"Playwright scrape failed: {e}")
        finally:
            browser.close()

    # Fallback to manually uploaded CSV
    if os.path.exists(FALLBACK_CSV):
        logger.info(f"✅ Using fallback.csv from repo ({FALLBACK_CSV})")
        return pd.read_csv(FALLBACK_CSV)

    raise RuntimeError("All data sources failed. Upload fallback.csv to data/ folder in GitHub.")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to match our expected schema."""
    col_map = {
        "Player": "Name", "PLAYER": "Name",
        "Salary": "Salary", "SAL": "Salary",
        "Projection": "Projection", "FPTS": "Projection", "Proj": "Projection",
        "Pts/$1k": "Pts/$1k", "VALUE": "Pts/$1k", "Value": "Pts/$1k",
        "FPPM": "FPPM",
        "USG%": "USG%", "USG": "USG%",
        "Team": "Team", "TEAM": "Team",
        "Opp": "Opp", "OPP": "Opp", "Opponent": "Opp",
        "DVP": "DVP",
        "Spread": "Spread",
        "Total": "Total",
        "O/U": "O/U",
        "Minutes": "Minutes", "MIN": "Minutes", "Mins": "Minutes",
        "PTS": "PTS", "Pts": "PTS",
        "AST": "AST", "Ast": "AST",
