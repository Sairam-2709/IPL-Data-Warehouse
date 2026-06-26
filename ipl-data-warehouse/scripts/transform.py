# scripts/transform.py
# -----------------------------------------------
# PySpark Transformation Script
# Reads raw CSVs from GCS, transforms data,
# generates derived metrics, and writes Parquet to GCS
# -----------------------------------------------

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from config.config import (
    GCS_BUCKET_NAME,
    GCS_RAW_PREFIX,
    GCS_PROCESSED_PREFIX,
    MATCHES_FILE,
    DELIVERIES_FILE,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

RAW_BASE    = f"gs://{GCS_BUCKET_NAME}/{GCS_RAW_PREFIX}"
OUTPUT_BASE = f"gs://{GCS_BUCKET_NAME}/{GCS_PROCESSED_PREFIX}"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("IPL_DataWarehouse_Transform")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# ── Dimension Tables ────────────────────────────────────────────

def build_dim_matches(matches_df):
    """Dimension table for match metadata."""
    logger.info("Building dim_matches...")
    return matches_df.select(
        F.col("id").alias("match_id"),
        F.col("season").alias("season_year"),
        F.col("city"),
        F.col("venue"),
        F.col("date").cast("date").alias("match_date"),
        F.col("team1"),
        F.col("team2"),
        F.col("toss_winner"),
        F.col("toss_decision"),
        F.col("winner").alias("match_winner"),
        F.col("win_by_runs"),
        F.col("win_by_wickets"),
        F.col("player_of_match"),
    ).dropDuplicates(["match_id"])


def build_dim_teams(matches_df):
    """Dimension table for teams."""
    logger.info("Building dim_teams...")
    team1 = matches_df.select(F.col("team1").alias("team_name"))
    team2 = matches_df.select(F.col("team2").alias("team_name"))
    return (
        team1.union(team2)
        .dropDuplicates(["team_name"])
        .withColumn("team_id", F.monotonically_increasing_id())
    )


def build_dim_players(deliveries_df):
    """Dimension table for players (batsmen + bowlers)."""
    logger.info("Building dim_players...")
    batsmen = deliveries_df.select(F.col("batsman").alias("player_name"))
    bowlers = deliveries_df.select(F.col("bowler").alias("player_name"))
    return (
        batsmen.union(bowlers)
        .dropDuplicates(["player_name"])
        .withColumn("player_id", F.monotonically_increasing_id())
    )


def build_dim_seasons(matches_df):
    """Dimension table for seasons."""
    logger.info("Building dim_seasons...")
    return (
        matches_df.select(F.col("season").alias("season_year"))
        .dropDuplicates(["season_year"])
        .orderBy("season_year")
        .withColumn("season_id", F.monotonically_increasing_id())
    )


# ── Fact Table ──────────────────────────────────────────────────

def build_fact_deliveries(deliveries_df, matches_df):
    """
    Fact table — ball-by-ball records with derived metrics.
    Joined with matches to bring in season and team context.
    """
    logger.info("Building fact_deliveries...")
    df = deliveries_df.join(
        matches_df.select("id", "season", "team1", "team2"),
        deliveries_df["match_id"] == matches_df["id"],
        how="left",
    ).drop("id")

    df = df.withColumn(
        "is_boundary", F.when(F.col("batsman_runs").isin(4, 6), 1).otherwise(0)
    ).withColumn(
        "is_six", F.when(F.col("batsman_runs") == 6, 1).otherwise(0)
    ).withColumn(
        "is_four", F.when(F.col("batsman_runs") == 4, 1).otherwise(0)
    ).withColumn(
        "is_dot_ball",
        F.when((F.col("total_runs") == 0), 1).otherwise(0)
    ).withColumn(
        "is_wicket", F.when(F.col("player_dismissed").isNotNull(), 1).otherwise(0)
    )

    return df.select(
        "match_id", "season", "inning", "batting_team", "bowling_team",
        "over", "ball", "batsman", "non_striker", "bowler",
        "batsman_runs", "extra_runs", "total_runs",
        "is_boundary", "is_four", "is_six", "is_dot_ball",
        "is_wicket", "player_dismissed", "dismissal_kind",
    )


# ── Derived Batting & Bowling Aggregates ────────────────────────

def build_batting_stats(fact_df):
    """Aggregated batting statistics per player per season."""
    logger.info("Building batting stats...")
    return fact_df.groupBy("batsman", "season").agg(
        F.sum("batsman_runs").alias("total_runs"),
        F.count("ball").alias("balls_faced"),
        F.sum("is_boundary").alias("boundaries"),
        F.sum("is_four").alias("fours"),
        F.sum("is_six").alias("sixes"),
        F.countDistinct("match_id").alias("matches_played"),
    ).withColumn(
        "strike_rate",
        F.round((F.col("total_runs") / F.col("balls_faced")) * 100, 2)
    ).withColumn(
        "batting_average",
        F.round(F.col("total_runs") / F.countDistinct("matches_played"), 2)
    )


def build_bowling_stats(fact_df):
    """Aggregated bowling statistics per player per season."""
    logger.info("Building bowling stats...")
    return fact_df.groupBy("bowler", "season").agg(
        F.sum("is_wicket").alias("wickets"),
        F.sum("total_runs").alias("runs_conceded"),
        F.count("ball").alias("balls_bowled"),
        F.sum("is_dot_ball").alias("dot_balls"),
        F.countDistinct("match_id").alias("matches_played"),
    ).withColumn(
        "overs_bowled", F.round(F.col("balls_bowled") / 6, 1)
    ).withColumn(
        "economy_rate",
        F.round(F.col("runs_conceded") / (F.col("balls_bowled") / 6), 2)
    ).withColumn(
        "bowling_average",
        F.when(
            F.col("wickets") > 0,
            F.round(F.col("runs_conceded") / F.col("wickets"), 2)
        ).otherwise(None)
    ).withColumn(
        "dot_ball_pct",
        F.round((F.col("dot_balls") / F.col("balls_bowled")) * 100, 2)
    )


# ── Write to GCS ────────────────────────────────────────────────

def write_parquet(df, name: str) -> None:
    path = f"{OUTPUT_BASE}{name}/"
    df.write.mode("overwrite").parquet(path)
    logger.info(f"Written: {path}")


# ── Main ────────────────────────────────────────────────────────

def run_transformations() -> None:
    spark = create_spark_session()

    logger.info("Reading raw data from GCS...")
    matches_df    = spark.read.option("header", True).csv(f"{RAW_BASE}{MATCHES_FILE}")
    deliveries_df = spark.read.option("header", True).csv(f"{RAW_BASE}{DELIVERIES_FILE}")

    # Cast numeric columns
    deliveries_df = deliveries_df.withColumn("batsman_runs", F.col("batsman_runs").cast("int")) \
                                 .withColumn("extra_runs",   F.col("extra_runs").cast("int")) \
                                 .withColumn("total_runs",   F.col("total_runs").cast("int")) \
                                 .withColumn("over",         F.col("over").cast("int")) \
                                 .withColumn("ball",         F.col("ball").cast("int"))

    matches_df = matches_df.withColumn("win_by_runs",    F.col("win_by_runs").cast("int")) \
                           .withColumn("win_by_wickets", F.col("win_by_wickets").cast("int"))

    # Remove duplicates and nulls
    deliveries_df = deliveries_df.dropDuplicates().dropna(subset=["match_id", "batsman", "bowler"])
    matches_df    = matches_df.dropDuplicates().dropna(subset=["id"])

    # Build and write all tables
    write_parquet(build_dim_matches(matches_df),          "dim_matches")
    write_parquet(build_dim_teams(matches_df),            "dim_teams")
    write_parquet(build_dim_players(deliveries_df),       "dim_players")
    write_parquet(build_dim_seasons(matches_df),          "dim_seasons")

    fact_df = build_fact_deliveries(deliveries_df, matches_df)
    write_parquet(fact_df,                                "fact_deliveries")
    write_parquet(build_batting_stats(fact_df),           "batting_stats")
    write_parquet(build_bowling_stats(fact_df),           "bowling_stats")

    logger.info("All transformations complete.")
    spark.stop()


if __name__ == "__main__":
    run_transformations()
