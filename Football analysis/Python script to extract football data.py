# -*- coding: utf-8 -*-
"""
Extract average Possession Rate and Progressive Passes per team per season
from the premier_league library's SQLite database.

Source: https://pypi.org/project/premier-league/

Coverage:  Premier League, La Liga, Serie A, Bundesliga, Ligue 1
           2017-2018 through 2023-2024  (7 complete seasons)

Columns:
    - league:                   League name
    - team:                     Team name
    - season:                   Season (e.g. '2023-2024')
    - avg_possession_rate:      Mean possession % across all league matches
    - avg_progressive_passes:   Mean progressive passes per match
    - position:                 Final league position (computed from results)

NOTE: The premier_league library (MatchStatistics) contains detailed match
stats from the 2017-2018 season onwards only. FBRef does not carry
progressive pass data before ~2017-18. Ligue 1 2019-2020 was curtailed
early due to COVID-19, so some teams have fewer than 38 matches that season.
Ligue 1 reduced to 18 teams from 2023-2024.

Positions are derived from match results in the database (points > goal
difference > goals scored). In seasons with points deductions (e.g. Everton
2023-24), computed positions may differ slightly from the official table.
"""

# pip install premier_league <- run this if this is the first try

import os
import sqlite3

import appdirs
import pandas as pd
from premier_league import MatchStatistics

# --------------------------------------------------------------------------
# 1. Initialise the database (triggers download on first use)
# --------------------------------------------------------------------------
_ = MatchStatistics()

# --------------------------------------------------------------------------
# 2. Locate the SQLite file created by the library
# --------------------------------------------------------------------------
db_path = os.path.join(
    appdirs.user_data_dir("premier_league"), "premier_league.db"
)
if not os.path.exists(db_path):
    db_path = os.path.join("data", "premier_league.db")

# --------------------------------------------------------------------------
# 3. Query all Top-5 leagues directly from SQLite
#    League IDs:  1 = Premier League
#                 2 = La Liga
#                 3 = Serie A
#                 4 = Bundesliga (stored as Fußball-Bundesliga)
#                 5 = Ligue 1
# --------------------------------------------------------------------------
conn = sqlite3.connect(db_path)

query = """
SELECT
    l.name                                AS league,
    t.name                                AS team,
    g.season                              AS season,
    ROUND(AVG(gs.possession_rate), 2)     AS avg_possession_rate,
    ROUND(AVG(gs.progressive_passes), 2)  AS avg_progressive_passes
FROM game_stats gs
JOIN game g   ON gs.game_id = g.id
JOIN team t   ON gs.team_id = t.id
JOIN league l ON g.league_id = l.id
WHERE g.league_id IN (1, 2, 3, 4, 5)
  AND g.season BETWEEN '2017-2018' AND '2023-2024'
GROUP BY l.name, t.name, g.season
ORDER BY l.name, g.season, t.name
"""

df = pd.read_sql_query(query, conn)

# --------------------------------------------------------------------------
# 3b. Compute end-of-season positions from match results
#     Points derived from goals (3 for win, 1 for draw, 0 for loss),
#     ranked by points > goal difference > goals scored.
# --------------------------------------------------------------------------
standings_query = """
WITH team_results AS (
    SELECT g.league_id, g.season,
           g.home_team_id AS team_id,
           CASE WHEN g.home_goals > g.away_goals THEN 3
                WHEN g.home_goals = g.away_goals THEN 1
                ELSE 0 END AS pts,
           g.home_goals AS gf, g.away_goals AS ga
    FROM game g
    UNION ALL
    SELECT g.league_id, g.season,
           g.away_team_id AS team_id,
           CASE WHEN g.away_goals > g.home_goals THEN 3
                WHEN g.away_goals = g.home_goals THEN 1
                ELSE 0 END AS pts,
           g.away_goals AS gf, g.home_goals AS ga
    FROM game g
)
SELECT
    l.name   AS league,
    t.name   AS team,
    tr.season,
    ROW_NUMBER() OVER (
        PARTITION BY tr.league_id, tr.season
        ORDER BY SUM(tr.pts) DESC,
                 SUM(tr.gf) - SUM(tr.ga) DESC,
                 SUM(tr.gf) DESC
    ) AS position
FROM team_results tr
JOIN team t   ON tr.team_id = t.id
JOIN league l ON tr.league_id = l.id
WHERE tr.league_id IN (1, 2, 3, 4, 5)
  AND tr.season BETWEEN '2017-2018' AND '2023-2024'
GROUP BY tr.league_id, tr.season, t.name
"""

standings_df = pd.read_sql_query(standings_query, conn)
conn.close()

# Clean up the Bundesliga name for readability
df["league"] = df["league"].replace({
    "Fußball-Bundesliga": "Bundesliga",
    "FuÃŸball-Bundesliga": "Bundesliga"
})
standings_df["league"] = standings_df["league"].replace({
    "Fußball-Bundesliga": "Bundesliga",
    "FuÃŸball-Bundesliga": "Bundesliga"
})

# Fix Character Encoding Issues for German/French/Spanish Teams
# --------------------------------------------------------------------------
def fix_mojibake(text):
    """Reverses UTF-8 text that was incorrectly read as Latin-1."""
    try:
        return text.encode('latin-1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        # If it's already correct or can't be decoded, return as-is
        return text

# Apply the fix to the team column (and league column while we are at it)
df["team"] = df["team"].apply(fix_mojibake)
standings_df["team"] = standings_df["team"].apply(fix_mojibake)

# Manual overrides for specific teams that still look incorrect
team_fixes = {
    "Saint-Ã‰tienne": "Saint-Étienne",
    "Saint-Etienne": "Saint-Étienne",
    "NÃ®mes": "Nîmes"
}

df["team"] = df["team"].replace(team_fixes)
standings_df["team"] = standings_df["team"].replace(team_fixes)

# --------------------------------------------------------------------------
# 3c. Merge position into the main dataframe
# --------------------------------------------------------------------------
df = df.merge(standings_df, on=["league", "team", "season"], how="left")

# --------------------------------------------------------------------------
# 4. Display summary and export
# --------------------------------------------------------------------------
summary = df.groupby("league").agg(
    seasons=("season", "nunique"),
    teams=("team", "nunique"),
    rows=("team", "count"),
)
print(summary)
print(f"\nTotal: {len(df)} rows")

output_path = r"data\data.csv"
df.to_csv(output_path, index=False)
print(f"Saved to {output_path}")
