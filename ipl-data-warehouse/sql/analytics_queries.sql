-- sql/analytics_queries.sql
-- -----------------------------------------------
-- Sample Analytical Queries — IPL Data Warehouse
-- -----------------------------------------------

-- 1. Top 10 Run Scorers (All Time)
SELECT
    batsman,
    SUM(batsman_runs)   AS total_runs,
    COUNT(DISTINCT match_id) AS matches,
    ROUND(SUM(batsman_runs) / COUNT(ball) * 100, 2) AS strike_rate,
    SUM(is_six)         AS sixes,
    SUM(is_four)        AS fours
FROM `ipl_data_warehouse.fact_deliveries`
GROUP BY batsman
ORDER BY total_runs DESC
LIMIT 10;

-- 2. Top 10 Wicket Takers (All Time)
SELECT
    bowler,
    SUM(is_wicket)      AS wickets,
    SUM(total_runs)     AS runs_conceded,
    ROUND(SUM(total_runs) / (COUNT(ball) / 6), 2) AS economy_rate,
    ROUND(SUM(total_runs) / NULLIF(SUM(is_wicket), 0), 2) AS bowling_average,
    ROUND(SUM(is_dot_ball) / COUNT(ball) * 100, 2) AS dot_ball_pct
FROM `ipl_data_warehouse.fact_deliveries`
GROUP BY bowler
ORDER BY wickets DESC
LIMIT 10;

-- 3. Season-wise Run Totals
SELECT
    season,
    SUM(total_runs)      AS total_runs,
    COUNT(DISTINCT match_id) AS total_matches,
    ROUND(SUM(total_runs) / COUNT(DISTINCT match_id), 2) AS avg_runs_per_match
FROM `ipl_data_warehouse.fact_deliveries`
GROUP BY season
ORDER BY season;

-- 4. Team Win/Loss Record
SELECT
    match_winner AS team,
    COUNT(*)     AS wins
FROM `ipl_data_warehouse.dim_matches`
WHERE match_winner IS NOT NULL
GROUP BY match_winner
ORDER BY wins DESC;

-- 5. Toss Impact Analysis
SELECT
    toss_decision,
    COUNT(*)  AS total_matches,
    SUM(CASE WHEN toss_winner = match_winner THEN 1 ELSE 0 END) AS toss_wins_match,
    ROUND(
        SUM(CASE WHEN toss_winner = match_winner THEN 1 ELSE 0 END) / COUNT(*) * 100, 2
    ) AS win_pct_after_toss
FROM `ipl_data_warehouse.dim_matches`
WHERE toss_winner IS NOT NULL AND match_winner IS NOT NULL
GROUP BY toss_decision;

-- 6. Top Venues by Number of Matches
SELECT
    venue,
    city,
    COUNT(*) AS matches_hosted
FROM `ipl_data_warehouse.dim_matches`
GROUP BY venue, city
ORDER BY matches_hosted DESC
LIMIT 10;

-- 7. Best Strike Rates (min 500 balls faced)
SELECT
    batsman,
    SUM(batsman_runs) AS runs,
    COUNT(ball)       AS balls_faced,
    ROUND(SUM(batsman_runs) / COUNT(ball) * 100, 2) AS strike_rate
FROM `ipl_data_warehouse.fact_deliveries`
GROUP BY batsman
HAVING COUNT(ball) >= 500
ORDER BY strike_rate DESC
LIMIT 10;

-- 8. Most Sixes in a Single Season
SELECT
    batsman,
    season,
    SUM(is_six) AS sixes
FROM `ipl_data_warehouse.fact_deliveries`
GROUP BY batsman, season
ORDER BY sixes DESC
LIMIT 10;

-- 9. Economy Rate by Bowler in Powerplay (Overs 1–6)
SELECT
    bowler,
    COUNT(DISTINCT match_id) AS matches,
    ROUND(SUM(total_runs) / (COUNT(ball) / 6), 2) AS powerplay_economy
FROM `ipl_data_warehouse.fact_deliveries`
WHERE over BETWEEN 1 AND 6
GROUP BY bowler
HAVING COUNT(ball) >= 120
ORDER BY powerplay_economy ASC
LIMIT 10;

-- 10. Player of the Match Leaderboard
SELECT
    player_of_match,
    COUNT(*) AS awards
FROM `ipl_data_warehouse.dim_matches`
WHERE player_of_match IS NOT NULL
GROUP BY player_of_match
ORDER BY awards DESC
LIMIT 10;
