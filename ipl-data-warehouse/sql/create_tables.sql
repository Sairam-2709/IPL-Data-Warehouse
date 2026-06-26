-- sql/create_tables.sql
-- -----------------------------------------------
-- BigQuery Star Schema — IPL Data Warehouse
-- -----------------------------------------------

-- 1. Dimension: Seasons
CREATE TABLE IF NOT EXISTS `ipl_data_warehouse.dim_seasons` (
    season_id   INT64,
    season_year STRING
);

-- 2. Dimension: Teams
CREATE TABLE IF NOT EXISTS `ipl_data_warehouse.dim_teams` (
    team_id   INT64,
    team_name STRING
);

-- 3. Dimension: Players
CREATE TABLE IF NOT EXISTS `ipl_data_warehouse.dim_players` (
    player_id   INT64,
    player_name STRING
);

-- 4. Dimension: Matches (partitioned by match_date, clustered by season_year)
CREATE TABLE IF NOT EXISTS `ipl_data_warehouse.dim_matches`
PARTITION BY DATE(match_date)
CLUSTER BY season_year, match_winner
AS SELECT
    CAST(id AS INT64)              AS match_id,
    season                         AS season_year,
    city,
    venue,
    PARSE_DATE('%Y-%m-%d', date)   AS match_date,
    team1,
    team2,
    toss_winner,
    toss_decision,
    winner                         AS match_winner,
    CAST(win_by_runs AS INT64)     AS win_by_runs,
    CAST(win_by_wickets AS INT64)  AS win_by_wickets,
    player_of_match
FROM `ipl_data_warehouse.raw_matches`;

-- 5. Fact: Deliveries (clustered by batting_team, bowling_team)
CREATE TABLE IF NOT EXISTS `ipl_data_warehouse.fact_deliveries`
CLUSTER BY batting_team, bowling_team
AS SELECT
    CAST(match_id AS INT64)        AS match_id,
    season,
    CAST(inning AS INT64)          AS inning,
    batting_team,
    bowling_team,
    CAST(over AS INT64)            AS over,
    CAST(ball AS INT64)            AS ball,
    batsman,
    non_striker,
    bowler,
    CAST(batsman_runs AS INT64)    AS batsman_runs,
    CAST(extra_runs AS INT64)      AS extra_runs,
    CAST(total_runs AS INT64)      AS total_runs,
    CASE WHEN CAST(batsman_runs AS INT64) IN (4,6) THEN 1 ELSE 0 END AS is_boundary,
    CASE WHEN CAST(batsman_runs AS INT64) = 4      THEN 1 ELSE 0 END AS is_four,
    CASE WHEN CAST(batsman_runs AS INT64) = 6      THEN 1 ELSE 0 END AS is_six,
    CASE WHEN CAST(total_runs AS INT64)   = 0      THEN 1 ELSE 0 END AS is_dot_ball,
    CASE WHEN player_dismissed IS NOT NULL         THEN 1 ELSE 0 END AS is_wicket,
    player_dismissed,
    dismissal_kind
FROM `ipl_data_warehouse.raw_deliveries`;
