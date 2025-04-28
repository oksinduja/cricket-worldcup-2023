import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# Initialize Glue context
glueContext = GlueContext()
spark = glueContext.spark_session
job = Job(glueContext)

# Define S3 paths
s3_source_path = "s3://worldcup-data-2023/processed"
s3_target_path = "s3://worldcup-data-2023/processed"

# Read data from S3 using Spark - now reading CSV files
deliveries_df = spark.read.option("header", "true").csv(f"{s3_source_path}/deliveries")
matches_df = spark.read.option("header", "true").csv(f"{s3_source_path}/matches")
players_df = spark.read.option("header", "true").csv(f"{s3_source_path}/players")

# Calculate batting statistics
batting_stats = deliveries_df.groupBy("match_id", "striker") \
    .agg(
        F.sum(F.col("runs_off_bat").cast("int")).alias("runs_scored"),
        F.count("match_id").alias("balls_faced"),
        F.sum(F.when(F.col("runs_off_bat") == 4, 1).otherwise(0)).alias("fours"),
        F.sum(F.when(F.col("runs_off_bat") == 6, 1).otherwise(0)).alias("sixes")
    )

# Add role column
batting_stats = batting_stats.withColumn("role_batting", F.lit("Batsman"))

# Calculate bowling statistics
bowling_wicket_types = ["bowled", "caught", "lbw", "stumped", "caught and bowled", "hit wicket"]
bowling_stats = deliveries_df.groupBy("match_id", "bowler") \
    .agg(
        F.count("match_id").alias("balls_bowled"),
        F.sum(F.col("extras").cast("int")).alias("extras"),
        F.sum(F.col("runs_off_bat").cast("int")).alias("runs_given"),
        F.sum(F.when(F.col("wicket_type").isin(bowling_wicket_types), 1).otherwise(0)).alias("wickets")
    )

# Add role column
bowling_stats = bowling_stats.withColumn("role_bowling", F.lit("Bowler"))

# Rename columns to prepare for merge
batting_stats = batting_stats.withColumnRenamed("striker", "player_name")
bowling_stats = bowling_stats.withColumnRenamed("bowler", "player_name")

# Merge batting and bowling statistics using a full outer join
player_stats = batting_stats.join(
    bowling_stats,
    on=["match_id", "player_name"],
    how="full_outer"
)

# Determine player role 
player_stats = player_stats \
    .withColumn("role", 
        F.when(
            (F.col("role_batting").isNotNull()) & (F.col("role_bowling").isNotNull()), 
            F.lit("All-rounder")
        )
        .when(F.col("role_batting").isNotNull(), F.lit("Batsman"))
        .when(F.col("role_bowling").isNotNull(), F.lit("Bowler"))
        .otherwise(F.lit(None))
    )

# Fill missing values with zeros
columns_to_fill = ["runs_scored", "balls_faced", "fours", "sixes", "runs_given",
                   "balls_bowled", "extras", "wickets"]

for col in columns_to_fill:
    player_stats = player_stats.withColumn(col, F.coalesce(F.col(col), F.lit(0)))

# Join with player information to get player_id
player_stats_with_id = player_stats.alias("a").join(
    players_df.alias("b"),
    F.col("a.player_name") == F.col("b.player_name"),
    "left"
)

# Select columns explicitly using aliases to avoid ambiguity
player_stats_with_id = player_stats_with_id.select(
    F.col("a.match_id"),
    F.col("b.player_id"),
    F.col("a.role"),
    F.col("a.runs_scored"),
    F.col("a.balls_faced"),
    F.col("a.fours"),
    F.col("a.sixes"),
    F.col("a.balls_bowled"),
    F.col("a.extras"),
    F.col("a.runs_given"),
    F.col("a.wickets")
)

# Calculate strike rate and economy rate
player_stats_final = player_stats_with_id \
    .withColumn("strike_rate", 
        F.when(
            F.col("balls_faced") > 0, 
            F.round(F.col("runs_scored") / F.col("balls_faced") * 100, 2)
        )
        .otherwise(0.0)
    ) \
    .withColumn("economy_rate", 
        F.when(
            F.col("balls_bowled") > 0, 
            F.round(F.col("runs_given") / (F.col("balls_bowled") / 6), 2)
        )
        .otherwise(0.0)
    )

# Write the final dataframe back to S3 as a single CSV file
player_stats_final.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{s3_target_path}/player_stats")

print(f"Processing complete. Output written to {s3_target_path}/player_stats")

# End the job
job.commit()