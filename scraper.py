import os
import json
import logging
import time
import pandas as pd
import requests
from datetime import datetime, timedelta
from itertools import combinations

logger = logging.getLogger(**name**)

FALLBACK_CSV  = “data/fallback.csv”
REPORT_PATH   = “data/report.json”

EDGE_LOCK = 6.0
EDGE_LEAN = 3.0
EDGE_DART = 1.5

PAYOUTS = {2: 3, 3: 5, 4: 10, 5: 20}

PROP_CATS = [“PTS”, “REB”, “AST”, “STL”, “BLK”, “PR”, “PA”, “RA”, “PRA”, “3PM”, “FG_ATT”]

# EV threshold: projection must be this % below the line to flag as +EV under

EV_THRESHOLD = 0.04   # 4%

# ─────────────────────────────────────────────────────────────────

# NBA TEAM ABBREVIATION MAP  (used to match CSV opp → API team id)

# ─────────────────────────────────────────────────────────────────

NBA_TEAM_IDS = {
“ATL”:1610612737,“BOS”:1610612738,“BKN”:1610612751,“CHA”:1610612766,
“CHI”:1610612741,“CLE”:1610612739,“DAL”:1610612742,“DEN”:1610612743,
“DET”:1610612765,“GS”:1610612744,“GSW”:1610612744,“HOU”:1610612745,
“IND”:1610612754,“LAC”:1610612746,“LAL”:1610612747,“MEM”:1610612763,
“MIA”:1610612748,“MIL”:1610612749,“MIN”:1610612750,“NO”:1610612740,
“NOP”:1610612740,“NYK”:1610612752,“OKC”:1610612760,“ORL”:1610612753,
“PHI”:1610612755,“PHX”:1610612756,“POR”:1610612757,“SAC”:1610612758,
“SAS”:1610612759,“TOR”:1610612761,“UTA”:1610612762,“WAS”:1610612764,
}

NBA_HEADERS = {
“Host”: “stats.nba.com”,
“User-Agent”: “Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36”,
“Accept”: “application/json, text/plain, */*”,
“Accept-Language”: “en-US,en;q=0.9”,
“Accept-Encoding”: “gzip, deflate, br”,
“x-nba-stats-origin”: “stats”,
“x-nba-stats-token”: “true”,
“Connection”: “keep-alive”,
“Referer”: “https://www.nba.com/”,
“Origin”: “https://www.nba.com”,
}

# ─────────────────────────────────────────────────────────────────

# CSV LOADER

# ─────────────────────────────────────────────────────────────────

def fetch_projections_csv():
if os.path.exists(FALLBACK_CSV):
df = pd.read_csv(FALLBACK_CSV)
logger.info(f”Loaded {len(df)} rows from fallback.csv”)
return df
raise RuntimeError(“fallback.csv not found - upload to data/ folder”)

# ─────────────────────────────────────────────────────────────────

# SHARP ODDS  (The Odds API)

# ─────────────────────────────────────────────────────────────────

def fetch_sharp_odds():
api_key = os.environ.get(“ODDS_API_KEY”)
if not api_key:
logger.warning(“No ODDS_API_KEY - skipping sharp odds”)
return pd.DataFrame()
try:
events_url = (
f”https://api.the-odds-api.com/v4/sports/basketball_nba/events”
f”?apiKey={api_key}”
)
events = requests.get(events_url, timeout=15).json()
odds_list = []
for event in events[:10]:
eid = event.get(“id”)
if not eid:
continue
url = (
f”https://api.the-odds-api.com/v4/sports/basketball_nba/events/{eid}/odds”
f”?apiKey={api_key}&regions=us”
f”&markets=player_points,player_rebounds,player_assists,player_threes”
f”&oddsFormat=american”
)
resp = requests.get(url, timeout=15)
if resp.status_code != 200:
continue
for bm in resp.json().get(“bookmakers”, []):
for mkt in bm.get(“markets”, []):
for out in mkt.get(“outcomes”, []):
if out.get(“point”) is not None:
odds_list.append({
“Player”: out.get(“description”, “”).strip(),
“Market”: mkt.get(“key”, “”).replace(“player_”, “”),
“Line”:   float(out.get(“point”, 0)),
“Odds”:   int(out.get(“price”, 0)),
“Book”:   bm.get(“key”, “sharp”),
})
df = pd.DataFrame(odds_list)
logger.info(f”Fetched {len(df)} sharp prop lines”)
return df
except Exception as e:
logger.error(f”Sharp odds fetch failed: {e}”)
return pd.DataFrame()

# ─────────────────────────────────────────────────────────────────

# PRIZEPICKS LINES

# ─────────────────────────────────────────────────────────────────

def fetch_prizepicks_lines():
“””
Returns dict: { “Player Name”: {“PTS”: line, “REB”: line, …} }
“””
out = {}
try:
url = “https://api.prizepicks.com/projections?league_id=7&per_page=250&single_stat=true”
headers = {
“User-Agent”: “Mozilla/5.0”,
“Accept”: “application/json”,
“Referer”: “https://app.prizepicks.com/”,
}
resp = requests.get(url, headers=headers, timeout=15)
if resp.status_code != 200:
logger.warning(f”PrizePicks API returned {resp.status_code}”)
return out

```
    data = resp.json()
    # Build player id → name map from "included"
    player_map = {}
    for item in data.get("included", []):
        if item.get("type") == "new_player":
            pid = item["id"]
            player_map[pid] = item["attributes"].get("name", "")

    for proj in data.get("data", []):
        attrs = proj.get("attributes", {})
        stat  = attrs.get("stat_type", "")
        line  = attrs.get("line_score")
        pid   = (proj.get("relationships", {})
                     .get("new_player", {})
                     .get("data", {})
                     .get("id"))
        name  = player_map.get(pid, "")
        if name and stat and line is not None:
            out.setdefault(name, {})[stat.upper().replace(" ", "_")] = float(line)

    logger.info(f"PrizePicks: {len(out)} players loaded")
except Exception as e:
    logger.error(f"PrizePicks fetch failed: {e}")
return out
```

# ─────────────────────────────────────────────────────────────────

# UNDERDOG LINES

# ─────────────────────────────────────────────────────────────────

def fetch_underdog_lines():
“””
Returns dict: { “Player Name”: {“PTS”: line, …} }
“””
out = {}
try:
url = “https://api.underdogfantasy.com/beta/v5/over_under_lines”
headers = {
“User-Agent”: “Mozilla/5.0”,
“Accept”: “application/json”,
“Referer”: “https://underdogfantasy.com/”,
}
resp = requests.get(url, headers=headers, timeout=15)
if resp.status_code != 200:
logger.warning(f”Underdog API returned {resp.status_code}”)
return out

```
    data = resp.json()
    # build player map
    player_map = {p["id"]: p.get("name", "") for p in data.get("players", [])}
    # appearances map: appearance_id → player_id
    app_map = {a["id"]: a.get("player_id", "") for a in data.get("appearances", [])}

    for line in data.get("over_under_lines", []):
        app_id  = line.get("over_under", {}).get("appearance_stat", {}).get("appearance_id", "")
        stat    = line.get("over_under", {}).get("appearance_stat", {}).get("stat", "")
        stat_line = line.get("stat_value")
        pid     = app_map.get(app_id, "")
        name    = player_map.get(pid, "")
        if name and stat and stat_line is not None:
            out.setdefault(name, {})[stat.upper().replace(" ", "_")] = float(stat_line)

    logger.info(f"Underdog: {len(out)} players loaded")
except Exception as e:
    logger.error(f"Underdog fetch failed: {e}")
return out
```

# ─────────────────────────────────────────────────────────────────

# DRAFTKINGS + FANDUEL ODDS

# ─────────────────────────────────────────────────────────────────

def fetch_dk_fd_odds():
“””
Returns dict: { “Player Name|STAT”: {“dk_line”: x, “dk_over”: odds, “dk_under”: odds,
“fd_line”: x, “fd_over”: odds, “fd_under”: odds} }
Uses The Odds API with dk/fanduel bookmakers filter.
“””
out = {}
api_key = os.environ.get(“ODDS_API_KEY”)
if not api_key:
logger.warning(“No ODDS_API_KEY - skipping DK/FD odds”)
return out
try:
events_url = (
f”https://api.the-odds-api.com/v4/sports/basketball_nba/events”
f”?apiKey={api_key}”
)
events = requests.get(events_url, timeout=15).json()

```
    stat_map = {
        "player_points":   "PTS",
        "player_rebounds":  "REB",
        "player_assists":   "AST",
        "player_threes":    "3PM",
        "player_steals":    "STL",
        "player_blocks":    "BLK",
    }

    for event in events[:10]:
        eid = event.get("id")
        if not eid:
            continue
        markets = ",".join(stat_map.keys())
        url = (
            f"https://api.the-odds-api.com/v4/sports/basketball_nba/events/{eid}/odds"
            f"?apiKey={api_key}&regions=us"
            f"&markets={markets}"
            f"&bookmakers=draftkings,fanduel"
            f"&oddsFormat=american"
        )
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            continue
        for bm in resp.json().get("bookmakers", []):
            book = bm.get("key", "")  # "draftkings" or "fanduel"
            prefix = "dk" if book == "draftkings" else "fd"
            for mkt in bm.get("markets", []):
                stat = stat_map.get(mkt.get("key", ""), "")
                if not stat:
                    continue
                # group outcomes by player
                by_player = {}
                for out_item in mkt.get("outcomes", []):
                    name = out_item.get("description", "").strip()
                    side = out_item.get("name", "").lower()  # "over" or "under"
                    pt   = out_item.get("point")
                    odds = out_item.get("price")
                    if name and pt is not None:
                        by_player.setdefault(name, {"line": float(pt)})
                        by_player[name][f"{prefix}_{side}"] = int(odds)
                for name, vals in by_player.items():
                    key = f"{name}|{stat}"
                    out.setdefault(key, {})
                    out[key][f"{prefix}_line"] = vals.get("line")
                    if f"{prefix}_over" in vals:
                        out[key][f"{prefix}_over"] = vals[f"{prefix}_over"]
                    if f"{prefix}_under" in vals:
                        out[key][f"{prefix}_under"] = vals[f"{prefix}_under"]

    logger.info(f"DK/FD odds: {len(out)} player|stat entries")
except Exception as e:
    logger.error(f"DK/FD odds fetch failed: {e}")
return out
```

# ─────────────────────────────────────────────────────────────────

# NBA STATS API — PLAYER ID LOOKUP

# ─────────────────────────────────────────────────────────────────

_player_id_cache = {}

def get_nba_player_id(name):
if name in *player_id_cache:
return *player_id_cache[name]
try:
url = “https://stats.nba.com/stats/commonallplayers?LeagueID=00&Season=2024-25&IsOnlyCurrentSeason=1”
resp = requests.get(url, headers=NBA_HEADERS, timeout=15)
if resp.status_code != 200:
return None
rows = resp.json()[“resultSets”][0][“rowSet”]
headers* = resp.json()[“resultSets”][0][“headers”]
id_idx   = headers*.index(“PERSON_ID”)
name_idx = headers_.index(“DISPLAY_FIRST_LAST”)
for row in rows:
_player_id_cache[row[name_idx]] = row[id_idx]
return _player_id_cache.get(name)
except Exception as e:
logger.warning(f”Player ID lookup failed for {name}: {e}”)
return None

# ─────────────────────────────────────────────────────────────────

# RECENT FORM  (last 5 games)

# ─────────────────────────────────────────────────────────────────

def fetch_recent_form(player_name, n=5):
“””
Returns dict with last-N averages: {pts, reb, ast, pra, last5_games}
“””
empty = {“l5_pts”: None, “l5_reb”: None, “l5_ast”: None, “l5_pra”: None, “last5_games”: []}
try:
pid = get_nba_player_id(player_name)
if not pid:
return empty

```
    url = (
        f"https://stats.nba.com/stats/playergamelogs"
        f"?PlayerID={pid}&Season=2024-25&SeasonType=Regular+Season&LastNGames={n}"
    )
    resp = requests.get(url, headers=NBA_HEADERS, timeout=15)
    if resp.status_code != 200:
        return empty

    result = resp.json()["resultSets"][0]
    hdrs   = result["headers"]
    rows   = result["rowSet"]
    if not rows:
        return empty

    pts_i = hdrs.index("PTS")
    reb_i = hdrs.index("REB")
    ast_i = hdrs.index("AST")
    date_i = hdrs.index("GAME_DATE")
    opp_i  = hdrs.index("MATCHUP")

    games = []
    for row in rows[:n]:
        pts = float(row[pts_i])
        reb = float(row[reb_i])
        ast = float(row[ast_i])
        games.append({
            "date": row[date_i],
            "opp":  row[opp_i],
            "pts":  pts,
            "reb":  reb,
            "ast":  ast,
            "pra":  round(pts + reb + ast, 1),
        })

    if not games:
        return empty

    return {
        "l5_pts": round(sum(g["pts"] for g in games) / len(games), 1),
        "l5_reb": round(sum(g["reb"] for g in games) / len(games), 1),
        "l5_ast": round(sum(g["ast"] for g in games) / len(games), 1),
        "l5_pra": round(sum(g["pra"] for g in games) / len(games), 1),
        "last5_games": games,
    }
except Exception as e:
    logger.warning(f"Recent form fetch failed for {player_name}: {e}")
    return empty
```

# ─────────────────────────────────────────────────────────────────

# H2H DATA  (vs today’s opponent)

# ─────────────────────────────────────────────────────────────────

def fetch_h2h(player_name, opp_abbr):
“””
Returns dict with career splits vs opponent: {h2h_pts, h2h_reb, h2h_ast, h2h_pra, h2h_games}
“””
empty = {“h2h_pts”: None, “h2h_reb”: None, “h2h_ast”: None, “h2h_pra”: None, “h2h_games”: 0}
try:
pid    = get_nba_player_id(player_name)
opp_id = NBA_TEAM_IDS.get(opp_abbr.upper().strip())
if not pid or not opp_id:
return empty

```
    url = (
        f"https://stats.nba.com/stats/playergamelogs"
        f"?PlayerID={pid}&Season=2024-25&SeasonType=Regular+Season&OpponentTeamID={opp_id}"
    )
    resp = requests.get(url, headers=NBA_HEADERS, timeout=15)
    if resp.status_code != 200:
        return empty

    result = resp.json()["resultSets"][0]
    hdrs   = result["headers"]
    rows   = result["rowSet"]
    if not rows:
        return empty

    pts_i = hdrs.index("PTS")
    reb_i = hdrs.index("REB")
    ast_i = hdrs.index("AST")

    pts_vals = [float(r[pts_i]) for r in rows]
    reb_vals = [float(r[reb_i]) for r in rows]
    ast_vals = [float(r[ast_i]) for r in rows]
    n = len(rows)

    return {
        "h2h_pts":   round(sum(pts_vals) / n, 1),
        "h2h_reb":   round(sum(reb_vals) / n, 1),
        "h2h_ast":   round(sum(ast_vals) / n, 1),
        "h2h_pra":   round((sum(pts_vals) + sum(reb_vals) + sum(ast_vals)) / n, 1),
        "h2h_games": n,
    }
except Exception as e:
    logger.warning(f"H2H fetch failed for {player_name} vs {opp_abbr}: {e}")
    return empty
```

# ─────────────────────────────────────────────────────────────────

# COLUMN NORMALIZER

# ─────────────────────────────────────────────────────────────────

def normalize_columns(df):
col_map = {
“Player”: “Name”, “PLAYER”: “Name”,
“DvP”: “DVP”,     “DVP”: “DVP”,
“Projection”: “Projection”, “FPTS”: “Projection”,
“Value”: “Value”, “Pts/$1k”: “Value”,
“Team”: “Team”,   “Opp”: “Opp”,
“MINS”: “MINS”,
“PTS”: “PTS”,     “AST”: “AST”,  “REB”: “REB”,
“STL”: “STL”,     “BLK”: “BLK”,  “3PM”: “3PM”,
“FGA”: “FG_ATT”,
}
df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
for col in [“DVP”, “Projection”, “Value”, “MINS”, “PTS”, “AST”, “REB”,
“STL”, “BLK”, “3PM”, “FG_ATT”]:
if col not in df.columns:
df[col] = 0.0
numeric = [“DVP”, “Projection”, “Value”, “MINS”, “PTS”, “AST”, “REB”,
“STL”, “BLK”, “3PM”, “FG_ATT”]
for col in numeric:
df[col] = pd.to_numeric(df[col], errors=“coerce”).fillna(0.0)
return df

# ─────────────────────────────────────────────────────────────────

# COMBO STATS + CONFIDENCE

# ─────────────────────────────────────────────────────────────────

def build_combo_stats(df):
df[“PR”]  = df[“PTS”] + df[“REB”]
df[“PA”]  = df[“PTS”] + df[“AST”]
df[“RA”]  = df[“REB”] + df[“AST”]
df[“PRA”] = df[“PTS”] + df[“REB”] + df[“AST”]

```
dvp_max = df["DVP"].max() if df["DVP"].max() > 0 else 30
df["DVP_norm"]   = df["DVP"] / dvp_max
val              = df["Value"].where(df["Value"] > 0, 5.0)
df["conf_score"] = df["Projection"] * df["DVP_norm"] * (val / 5.0)
return df
```

def best_cat(row):
cats = {c: float(row.get(c, 0)) for c in PROP_CATS if float(row.get(c, 0)) > 0}
return max(cats, key=cats.get) if cats else “PRA”

# ─────────────────────────────────────────────────────────────────

# MATCHUP GRADE  (A–F)

# ─────────────────────────────────────────────────────────────────

def compute_matchup_grade(dvp_rank_pct):
“””
dvp_rank_pct: 0–1 where 1 = best matchup (highest DVP allowed)
Returns letter grade + color
“””
if dvp_rank_pct >= 0.80:
return {“grade”: “A+”, “color”: “#10b981”}
elif dvp_rank_pct >= 0.65:
return {“grade”: “A”,  “color”: “#10b981”}
elif dvp_rank_pct >= 0.50:
return {“grade”: “B+”, “color”: “#84cc16”}
elif dvp_rank_pct >= 0.35:
return {“grade”: “B”,  “color”: “#84cc16”}
elif dvp_rank_pct >= 0.20:
return {“grade”: “C”,  “color”: “#f59e0b”}
else:
return {“grade”: “D”,  “color”: “#ef4444”}

# ─────────────────────────────────────────────────────────────────

# CONFIDENCE SCORE  (1–10)

# ─────────────────────────────────────────────────────────────────

def compute_confidence(dvp_norm, proj, sharp_edge=None):
“””
DVP weight 40%, projection magnitude 30%, sharp edge 30%
Returns int 1–10
“””
dvp_score  = min(dvp_norm * 10, 10) * 0.40
proj_score = min(proj / 5, 10) * 0.30
if sharp_edge is not None:
edge_score = min(max(sharp_edge / 10, 0), 10) * 0.30
else:
edge_score = 5 * 0.30   # neutral if no sharp data
raw = dvp_score + proj_score + edge_score
return max(1, min(10, round(raw)))

# ─────────────────────────────────────────────────────────────────

# EV UNDERS  —  find +EV under plays

# ─────────────────────────────────────────────────────────────────

def find_ev_unders(players_data, pp_lines, ud_lines, dk_fd_odds):
“””
Compare our projection vs PrizePicks/Underdog/DK/FD lines.
Flag as +EV under when projection < line by EV_THRESHOLD.
Returns list of dicts.
“””
ev_plays = []

```
for p in players_data:
    name = p["name"]
    proj = p["proj"]
    if proj <= 0:
        continue

    # Check each stat category
    for stat in ["PTS", "REB", "AST", "3PM", "PRA"]:
        stat_proj = p.get(stat.lower()) or p.get("best_val") if stat == "PRA" else p.get(stat.lower(), 0)
        if stat == "PRA":
            stat_proj = p.get("pts", 0) + p.get("reb", 0) + p.get("ast", 0)
        elif stat == "PTS":
            stat_proj = p.get("pts", 0)
        elif stat == "REB":
            stat_proj = p.get("reb", 0)
        elif stat == "AST":
            stat_proj = p.get("ast", 0)
        elif stat == "3PM":
            stat_proj = p.get("threepm", 0)

        if stat_proj <= 0:
            continue

        pp_line = (pp_lines.get(name) or {}).get(stat) or (pp_lines.get(name) or {}).get(stat.replace("_", " "))
        ud_line = (ud_lines.get(name) or {}).get(stat)
        dk_key  = f"{name}|{stat}"
        dk_line = dk_fd_odds.get(dk_key, {}).get("dk_line")
        fd_line = dk_fd_odds.get(dk_key, {}).get("fd_line")
        dk_under_odds = dk_fd_odds.get(dk_key, {}).get("dk_under")
        fd_under_odds = dk_fd_odds.get(dk_key, {}).get("fd_under")

        lines_found = {k: v for k, v in {
            "PrizePicks": pp_line,
            "Underdog":   ud_line,
            "DraftKings": dk_line,
            "FanDuel":    fd_line,
        }.items() if v is not None}

        for book, line in lines_found.items():
            if line <= 0:
                continue
            edge_pct = (line - stat_proj) / line   # positive = we're below the line
            if edge_pct >= EV_THRESHOLD:
                # implied prob from american odds if available
                raw_odds = None
                if book == "DraftKings":
                    raw_odds = dk_under_odds
                elif book == "FanDuel":
                    raw_odds = fd_under_odds

                implied_prob = None
                if raw_odds is not None:
                    if raw_odds < 0:
                        implied_prob = round(abs(raw_odds) / (abs(raw_odds) + 100) * 100, 1)
                    else:
                        implied_prob = round(100 / (raw_odds + 100) * 100, 1)

                ev_plays.append({
                    "name":         name,
                    "team":         p.get("team", ""),
                    "opp":          p.get("opp", ""),
                    "stat":         stat,
                    "our_proj":     round(stat_proj, 1),
                    "line":         line,
                    "edge_pct":     round(edge_pct * 100, 1),
                    "book":         book,
                    "under_odds":   raw_odds,
                    "implied_prob": implied_prob,
                    "dvp":          p.get("dvp", 0),
                    "grade":        p.get("matchup_grade", {}).get("grade", "?"),
                    "confidence":   p.get("confidence", 5),
                    "target_prop":  stat,
                })

# Sort by edge_pct descending
ev_plays.sort(key=lambda x: x["edge_pct"], reverse=True)
return ev_plays[:30]   # top 30
```

# ─────────────────────────────────────────────────────────────────

# FORMAT PLAYER  (builds the dict stored in JSON)

# ─────────────────────────────────────────────────────────────────

def fmt_player(row, target_cat=None, dvp_max=30, form=None, h2h=None,
pp_lines=None, ud_lines=None, dk_fd_odds=None):
dvp      = float(row.get(“DVP”, 0))
dvp_norm = dvp / dvp_max if dvp_max > 0 else 0
proj     = float(row.get(“Projection”, 0))
target   = target_cat or best_cat(row)

```
# Sharp edge: compare proj to sharp line if available
sharp_edge = None
if not (pp_lines is None) and row.get("Name") in (pp_lines or {}):
    pp_line_val = pp_lines[row["Name"]].get(target)
    if pp_line_val and pp_line_val > 0:
        sharp_edge = ((proj - pp_line_val) / pp_line_val) * 10

conf      = compute_confidence(dvp_norm, proj, sharp_edge)
mg        = compute_matchup_grade(dvp_norm)

name = str(row.get("Name", ""))
opp  = str(row.get("Opp", ""))

# Lines
pp_stat  = (pp_lines or {}).get(name, {}).get(target)
ud_stat  = (ud_lines or {}).get(name, {}).get(target)
dk_entry = (dk_fd_odds or {}).get(f"{name}|{target}", {})

return {
    "name":           name,
    "team":           str(row.get("Team", "")),
    "opp":            opp,
    "dvp":            round(dvp, 1),
    "proj":           round(proj, 1),
    "val":            round(float(row.get("Value", 0)), 1),
    "pts":            round(float(row.get("PTS", 0)), 1),
    "reb":            round(float(row.get("REB", 0)), 1),
    "ast":            round(float(row.get("AST", 0)), 1),
    "stl":            round(float(row.get("STL", 0)), 1),
    "blk":            round(float(row.get("BLK", 0)), 1),
    "threepm":        round(float(row.get("3PM", 0)), 1),
    "fg_att":         round(float(row.get("FG_ATT", 0)), 1),
    "target_prop":    target,
    "best_val":       round(float(row.get(target, 0)), 1),
    "confidence":     conf,
    "matchup_grade":  mg,
    # Lines from books
    "pp_line":        pp_stat,
    "ud_line":        ud_stat,
    "dk_line":        dk_entry.get("dk_line"),
    "fd_line":        dk_entry.get("fd_line"),
    "dk_over_odds":   dk_entry.get("dk_over"),
    "dk_under_odds":  dk_entry.get("dk_under"),
    "fd_over_odds":   dk_entry.get("fd_over"),
    "fd_under_odds":  dk_entry.get("fd_under"),
    # Recent form
    "l5_pts":         (form or {}).get("l5_pts"),
    "l5_reb":         (form or {}).get("l5_reb"),
    "l5_ast":         (form or {}).get("l5_ast"),
    "l5_pra":         (form or {}).get("l5_pra"),
    "last5_games":    (form or {}).get("last5_games", []),
    # H2H
    "h2h_pts":        (h2h or {}).get("h2h_pts"),
    "h2h_reb":        (h2h or {}).get("h2h_reb"),
    "h2h_ast":        (h2h or {}).get("h2h_ast"),
    "h2h_pra":        (h2h or {}).get("h2h_pra"),
    "h2h_games":      (h2h or {}).get("h2h_games", 0),
    "sharp_edge":     round(sharp_edge, 2) if sharp_edge is not None else None,
}
```

# ─────────────────────────────────────────────────────────────────

# GAME DETECTION

# ─────────────────────────────────────────────────────────────────

def detect_games(df):
games  = set()
opp_map = dict(zip(df[“Team”], df[“Opp”]))
for team in df[“Team”].dropna().unique():
opp = opp_map.get(team)
if opp and (opp, team) not in games:
games.add((team, opp))
return list(games)

# ─────────────────────────────────────────────────────────────────

# BUILDERS  (same-game P4, slips, leaders)

# ─────────────────────────────────────────────────────────────────

def build_same_game_p4s(df, games, dvp_max, pp_lines, ud_lines, dk_fd_odds, form_cache, h2h_cache):
results = []
for t1, t2 in games:
gdf = df[df[“Team”].isin([t1, t2])].copy().sort_values(“conf_score”, ascending=False)
alpha = gdf.head(4)
results.append({
“game”: f”{t1} vs {t2}”,
“alpha”: [
fmt_player(row, dvp_max=dvp_max,
pp_lines=pp_lines, ud_lines=ud_lines, dk_fd_odds=dk_fd_odds,
form=form_cache.get(row[“Name”]),
h2h=h2h_cache.get(row[“Name”]))
for _, row in alpha.iterrows()
]
})
return results

def build_diverse_slips(df, dvp_max, pp_lines, ud_lines, dk_fd_odds, form_cache, h2h_cache):
slips    = {“2”: [], “3”: [], “4”: [], “5”: []}
high_conf = df[df[“conf_score”] >= EDGE_DART].nlargest(25, “conf_score”)

```
for size in [2, 3, 4, 5]:
    for cat in PROP_CATS:
        cat_df = high_conf[high_conf.apply(lambda r: best_cat(r) == cat, axis=1)]
        if len(cat_df) >= size:
            for combo in combinations(cat_df.iterrows(), size):
                players = [
                    fmt_player(row, cat, dvp_max=dvp_max,
                               pp_lines=pp_lines, ud_lines=ud_lines, dk_fd_odds=dk_fd_odds,
                               form=form_cache.get(row["Name"]),
                               h2h=h2h_cache.get(row["Name"]))
                    for _, row in combo
                ]
                total_proj = sum(p["proj"] for p in players)
                slips[str(size)].append({
                    "players":     players,
                    "total_proj":  round(total_proj, 1),
                    "payout":      PAYOUTS.get(size, 0),
                    "target_prop": cat,
                })
                if len(slips[str(size)]) >= 6:
                    break
        if len(slips[str(size)]) >= 8:
            break
return slips
```

def build_category_leaders(df, dvp_max, pp_lines, ud_lines, dk_fd_odds, form_cache, h2h_cache):
leaders = []
for cat in [“PTS”, “REB”, “AST”, “PRA”, “3PM”, “STL”, “BLK”]:
top = df.nlargest(5, cat)
leaders.append({
“category”: cat,
“players”: [
fmt_player(row, cat, dvp_max=dvp_max,
pp_lines=pp_lines, ud_lines=ud_lines, dk_fd_odds=dk_fd_odds,
form=form_cache.get(row[“Name”]),
h2h=h2h_cache.get(row[“Name”]))
for _, row in top.iterrows()
]
})
return leaders

# ─────────────────────────────────────────────────────────────────

# MAIN SCRAPE

# ─────────────────────────────────────────────────────────────────

def run_daily_scrape(output_path=REPORT_PATH):
logger.info(“Starting daily scrape with full data integration…”)

```
# 1. Load CSV
df = fetch_projections_csv()
df = normalize_columns(df)
df = build_combo_stats(df)

dvp_max = df["DVP"].max() if df["DVP"].max() > 0 else 30

# 2. Fetch sportsbook lines
sharp_df  = fetch_sharp_odds()
pp_lines  = fetch_prizepicks_lines()
ud_lines  = fetch_underdog_lines()
dk_fd_odds = fetch_dk_fd_odds()

# 3. Fetch recent form + H2H for top players (cap at 20 to save time/quota)
games    = detect_games(df)
top_names = df.nlargest(20, "conf_score")["Name"].tolist()

form_cache = {}
h2h_cache  = {}

opp_map = dict(zip(df["Name"], df["Opp"]))

for name in top_names:
    logger.info(f"Fetching form/H2H for {name}...")
    form_cache[name] = fetch_recent_form(name, n=5)
    opp = opp_map.get(name, "")
    if opp:
        h2h_cache[name] = fetch_h2h(name, opp)
    time.sleep(0.4)   # respect NBA stats rate limit

# 4. Build all player lists
kwargs = dict(
    dvp_max=dvp_max,
    pp_lines=pp_lines,
    ud_lines=ud_lines,
    dk_fd_odds=dk_fd_odds,
    form_cache=form_cache,
    h2h_cache=h2h_cache,
)

top_locks   = [fmt_player(row, dvp_max=dvp_max, pp_lines=pp_lines, ud_lines=ud_lines,
                           dk_fd_odds=dk_fd_odds, form=form_cache.get(row["Name"]),
                           h2h=h2h_cache.get(row["Name"]))
               for _, row in df[df["conf_score"] >= EDGE_LOCK].nlargest(12, "conf_score").iterrows()]

value_plays = [fmt_player(row, dvp_max=dvp_max, pp_lines=pp_lines, ud_lines=ud_lines,
                           dk_fd_odds=dk_fd_odds, form=form_cache.get(row["Name"]),
                           h2h=h2h_cache.get(row["Name"]))
               for _, row in df[df["conf_score"] >= EDGE_LEAN].nlargest(18, "conf_score").iterrows()]

all_players = top_locks + value_plays

# 5. Find EV unders
ev_unders = find_ev_unders(all_players, pp_lines, ud_lines, dk_fd_odds)
logger.info(f"Found {len(ev_unders)} +EV under plays")

# 6. Assemble report
report = {
    "generated_at":      datetime.now().isoformat(),
    "slate_date":        datetime.now().strftime("%Y-%m-%d"),
    "game_count":        len(games),
    "same_game_p4":      build_same_game_p4s(df, games, **kwargs),
    "slips":             build_diverse_slips(df, **kwargs),
    "category_leaders":  build_category_leaders(df, **kwargs),
    "top_locks":         top_locks,
    "value_plays":       value_plays,
    "ev_unders":         ev_unders,
}

os.makedirs(os.path.dirname(output_path), exist_ok=True)
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2, ensure_ascii=False)

logger.info(f"Report saved: {len(top_locks)} locks, {len(value_plays)} value, {len(ev_unders)} EV unders")
return report
```

if **name** == “**main**”:
logging.basicConfig(level=logging.INFO)
run_daily_scrape()
