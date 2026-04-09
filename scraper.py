def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Improved column normalization for lineups.com CSV."""
    col_map = {
        "Player": "Name", "PLAYER": "Name",
        "Salary": "Salary", "SAL": "Salary",
        "Projection": "Projection", "FPTS": "Projection", "Proj": "Projection",
        "Pts/$1k": "Pts/$1k", "VALUE": "Pts/$1k", "Value": "Pts/$1k",
        "Team": "Team", "TEAM": "Team",
        "Opp": "Opp", "OPP": "Opp", "Opponent": "Opp",
        "DVP": "DVP",
        "Spread": "Spread",
        "Total": "Total",
        "Minutes": "Minutes", "MIN": "Minutes",
        "PTS": "PTS", "Pts": "PTS",
        "AST": "AST", "Ast": "AST",
        "REB": "REB", "Reb": "REB",
        "STL": "STL", "Stl": "STL",
        "BLK": "BLK", "Blk": "BLK",
        "Pos": "Pos", "Position": "Pos",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # Ensure required columns exist with defaults
    required = ["Name", "Team", "Opp", "DVP", "Projection", "Salary", "Pts/$1k", "PTS", "AST", "REB"]
    for col in required:
        if col not in df.columns:
            df[col] = 0.0

    # Coerce numeric columns
    numeric_cols = ["DVP", "Projection", "Salary", "Pts/$1k", "PTS", "AST", "REB", "STL", "BLK", "Minutes"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    return df
