import sys
import json
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import when, col, row_number, lit, concat, regexp_replace, substring
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Set up Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default Data Quality Rule
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Load data from S3 via Glue Catalog
matches_df = glueContext.create_dynamic_frame.from_catalog(
    database="etlworldcup", table_name="matches_csv", transformation_ctx="matches_df"
)

points_table_df = glueContext.create_dynamic_frame.from_catalog(
    database="etlworldcup", table_name="points_table_csv", transformation_ctx="points_table_df"
)

deliveries_df = glueContext.create_dynamic_frame.from_catalog(
    database="etlworldcup", table_name="deliveries_csv", transformation_ctx="deliveries_df"
)

# Load stadiums JSON data
# Load stadiums JSON data
try:
    # Read the JSON file directly from S3
    stadiums_json_path = "s3://worldcup-data-2023/raw/stadiums.json"
    
    # Use AWS Glue's native S3 file reading capabilities
    from awsglue.dynamicframe import DynamicFrame
    s3_stadiums = glueContext.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={
            "paths": [stadiums_json_path],
            "recurse": False
        },
        format="json"
    )
    
    if s3_stadiums.count() > 0:
        stadiums_data = [row.asDict() for row in s3_stadiums.toDF().collect()]
        print(f"Successfully loaded {len(stadiums_data)} stadium records")
    else:
        # If JSON loading fails, extract venue info from matches
        print("JSON file loaded but no data found. Extracting venue info from matches data.")
        stadiums_data = []
        venue_info = df_matches.select("venue", "city").distinct().collect()
        
        for row in venue_info:
            stadiums_data.append({
                "venue": row["venue"],
                "city": row["city"],
                "state": "Unknown",
                "country": "India"
            })
        print(f"Created {len(stadiums_data)} venue entries from matches data")
        
except Exception as e:
    print(f"Error loading stadiums JSON: {e}")
    print("Extracting venue info from matches data.")
    
    # Extract venue info from matches if JSON loading fails
    stadiums_data = []
    venue_info = df_matches.select("venue", "city").distinct().collect()
    
    for row in venue_info:
        stadiums_data.append({
            "venue": row["venue"],
            "city": row["city"],
            "state": "Unknown",
            "country": "India"
        })
    print(f"Created {len(stadiums_data)} venue entries from matches data")

# Change Schema (Apply Mapping)
mapped_matches = ApplyMapping.apply(frame=matches_df, mappings=[
    ("season", "string", "season", "string"),
    ("team1", "string", "team1", "string"),
    ("team2", "string", "team2", "string"),
    ("date", "string", "date", "string"),
    ("match_number", "long", "match_number", "int"),
    ("venue", "string", "venue", "string"),
    ("city", "string", "city", "string"),
    ("toss_winner", "string", "toss_winner", "string"),
    ("toss_decision", "string", "toss_decision", "string"),
    ("player_of_match", "string", "player_of_match", "string"),
    ("umpire1", "string", "umpire1", "string"),
    ("umpire2", "string", "umpire2", "string"),
    ("reserve_umpire", "string", "reserve_umpire", "string"),
    ("match_referee", "string", "match_referee", "string"),
    ("winner", "string", "winner", "string"),
    ("winner_wickets", "long", "winner_wickets", "int"),
    ("match_type", "string", "match_type", "string"),
    ("winner_runs", "long", "winner_runs", "int")
], transformation_ctx="mapped_matches")

mapped_points = ApplyMapping.apply(frame=points_table_df, mappings=[
    ("ranking", "long", "ranking", "int"),
    ("team", "string", "team", "string"),
    ("matches", "long", "matches", "int"),
    ("won", "long", "won", "int"),
    ("lost", "long", "lost", "int"),
    ("tie", "long", "tie", "int"),
    ("no results", "long", "no_results", "int"),
    ("points", "long", "points", "int"),
    ("net run rate", "double", "net_run_rate", "float"),
    ("for", "string", "for", "string"),
    ("against", "string", "against", "string")
], transformation_ctx="mapped_points")

mapped_deliveries = ApplyMapping.apply(frame=deliveries_df, mappings=[
    ("match_id", "long", "match_id", "int"),
    ("season", "string", "season", "string"),
    ("start_date", "string", "start_date", "date"),
    ("venue", "string", "venue", "string"),
    ("innings", "long", "innings", "int"),
    ("ball", "double", "ball", "float"),
    ("batting_team", "string", "batting_team", "string"),
    ("bowling_team", "string", "bowling_team", "string"),
    ("striker", "string", "striker", "string"),
    ("non_striker", "string", "non_striker", "string"),
    ("bowler", "string", "bowler", "string"),
    ("runs_off_bat", "long", "runs_off_bat", "int"),
    ("extras", "long", "extras", "int"),
    ("wides", "double", "wides", "double"),
    ("legbyes", "double", "legbyes", "double"),
    ("byes", "double", "byes", "double"),
    ("noballs", "double", "noballs", "double"),
    ("wicket_type", "string", "wicket_type", "string"),
    ("player_dismissed", "string", "player_dismissed", "string")
], transformation_ctx="mapped_deliveries")

# Convert to DataFrame for transformations
df_matches = mapped_matches.toDF()
df_points = mapped_points.toDF()
df_deliveries = mapped_deliveries.toDF()

# Handle nulls in deliveries
df_deliveries = df_deliveries.fillna({
    "wides": 0.0,
    "noballs": 0.0,
    "byes": 0.0,
    "legbyes": 0.0,
})
df_deliveries = df_deliveries.withColumn(
    "wicket_type",
    when(col("wicket_type") == "", "N/A").otherwise(col("wicket_type"))
).withColumn(
    "player_dismissed",
    when(col("player_dismissed") == "", "N/A").otherwise(col("player_dismissed"))
)

# Handle nulls in matches
df_matches = df_matches.withColumn(
    "match_referee",
    when(col("match_referee") == "", "N/A").otherwise(col("match_referee"))
)
df_matches = df_matches.fillna({
    "winner_wickets": 0,
    "winner_runs": 0
})

# Handle nulls in points table
df_points = df_points.fillna({
    "tie": 0,
    "no_results": 0,
    "net_run_rate": 0.0
})


venue_data = []
state_counters = {}

for stadium in stadiums_data:
    venue = stadium.get("venue", "")
    city = stadium.get("city", "")
    state = stadium.get("state", "")
    country = stadium.get("country", "India")
    
    # Create state code (first 2 letters of state name)
    state_code = ''.join(word[0:1] for word in state.upper().split())
    if len(state_code) < 2:
        state_code = state.upper()[0:2]
    
    # Increment counter for this state
    if state_code not in state_counters:
        state_counters[state_code] = 1
    else:
        state_counters[state_code] += 1
    
    # Create venue_id
    venue_id = f"{state_code}{state_counters[state_code]:02d}"
    
    venue_data.append((venue_id, venue, city, state, country))

# Create venues DataFrame
venues_schema = ["venue_id", "venue", "city", "state", "country"]
df_venues = spark.createDataFrame(venue_data, venues_schema)

# Create a mapping of venue to venue_id for later use
venue_id_mapping = {row["venue"]: row["venue_id"] for row in df_venues.collect()}


df_points = df_points.withColumn("season", lit("2023/24"))
df_points = df_points.withColumn("season_id", concat(lit("S"), col("season").substr(0, 4), lit("-"), col("ranking")))


# Add fixed season to matches
df_matches = df_matches.withColumn("season", lit("2023/24"))

# Create mapping DataFrame for teams to ranking
teams_ranking_df = df_points.select("team", "ranking")

# Join matches with teams_ranking_df to get ranking for team1 (as a fallback)
df_matches = df_matches.join(
    teams_ranking_df,
    df_matches["team1"] == teams_ranking_df["team"],
    "left"
).withColumnRenamed("ranking", "team1_ranking")

# Create season_id using the fixed season and team1's ranking as a fallback
df_matches = df_matches.withColumn(
    "season_id", 
    concat(
        lit("S"), 
        substring(col("season"), 1, 4), 
        lit("-"), 
        when(col("team1_ranking").isNotNull(), col("team1_ranking")).otherwise(lit(0))
    )
)

# Create match_id as composite of season_id and match_number
df_matches = df_matches.withColumn(
    "match_id", 
    concat(col("season_id"), lit("-M"), col("match_number"))
)

# Create a mapping DataFrame for venue to venue_id
venue_mapping_df = spark.createDataFrame(
    [(venue, venue_id) for venue, venue_id in venue_id_mapping.items()],
    ["venue", "venue_id"]
)

# Join matches with venue mapping instead of using UDF
df_matches = df_matches.join(
    venue_mapping_df,
    df_matches["venue"] == venue_mapping_df["venue"],
    "left"
)

# Handle cases where venue has no mapping
df_matches = df_matches.withColumn(
    "venue_id",
    when(col("venue_id").isNull(), lit("UNK01")).otherwise(col("venue_id"))
)

# Select final columns for matches (removing city)
df_matches_final = df_matches.select(
    "season_id", "match_number", "match_id", "team1", "team2", "date",
    "venue_id", "toss_winner", "toss_decision", "player_of_match",
    "umpire1", "umpire2", "reserve_umpire", "match_referee", "winner",
    "winner_wickets", "match_type", "winner_runs"
)

match_id_mapping = {}
for row in df_matches_final.select("match_number", "match_id").collect():
    match_id_mapping[row["match_number"]] = row["match_id"]

# Create a mapping DataFrame for old match_id to new match_id
match_id_mapping_df = spark.createDataFrame(
    [(int(match_num), match_id) for match_num, match_id in match_id_mapping.items()],
    ["old_match_id", "new_match_id"]
)

# Join deliveries with match_id mapping instead of using UDF
df_deliveries = df_deliveries.join(
    match_id_mapping_df,
    df_deliveries["match_id"] == match_id_mapping_df["old_match_id"],
    "left"
)

# Handle cases where match_id has no mapping
df_deliveries = df_deliveries.withColumn(
    "new_match_id",
    when(col("new_match_id").isNull(), 
         concat(lit("Unknown-"), col("match_id").cast("string"))
        ).otherwise(col("new_match_id"))
)

# Select final columns for deliveries
df_deliveries_final = df_deliveries.select(
    "new_match_id", "innings", "ball", "batting_team", "bowling_team",
    "striker", "non_striker", "bowler", "runs_off_bat", "extras",
    "wides", "legbyes", "byes", "noballs", "wicket_type", "player_dismissed"
).withColumnRenamed("new_match_id", "match_id")

# ======== TRANSFORMATION 5: Players Extraction with Team ========

# Extract all player names from deliveries DataFrame
striker_df = df_deliveries.select("striker", "batting_team").withColumnRenamed("striker", "player_name").withColumnRenamed("batting_team", "team")
non_striker_df = df_deliveries.select("non_striker", "batting_team").withColumnRenamed("non_striker", "player_name").withColumnRenamed("batting_team", "team")
bowler_df = df_deliveries.select("bowler", "bowling_team").withColumnRenamed("bowler", "player_name").withColumnRenamed("bowling_team", "team")

# Combine player dataframes
all_players_df = striker_df.union(non_striker_df).union(bowler_df)
all_players_df = all_players_df.filter(col("player_name").isNotNull()).dropDuplicates(["player_name"])

# Create player_id
players_df = all_players_df.withColumn(
    "player_id", 
    row_number().over(Window.orderBy("player_name"))
)

# Convert back to DynamicFrames for Glue operations
final_matches = DynamicFrame.fromDF(df_matches_final, glueContext, "final_matches")
final_points = DynamicFrame.fromDF(df_points, glueContext, "final_points")
final_deliveries = DynamicFrame.fromDF(df_deliveries_final, glueContext, "final_deliveries")
final_venues = DynamicFrame.fromDF(df_venues, glueContext, "final_venues")
final_players = DynamicFrame.fromDF(players_df, glueContext, "final_players")

# Data Quality Evaluation and S3 Write

# Matches
EvaluateDataQuality().process_rows(
    frame=final_matches,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={"dataQualityEvaluationContext": "dq_matches", "enableDataQualityResultsPublishing": True},
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

# Convert to single CSV file
df_matches_final.coalesce(1).write.mode("overwrite").option("header", "true").csv("s3://worldcup-data-2023/processed/matches/")

# Points Table
EvaluateDataQuality().process_rows(
    frame=final_points,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={"dataQualityEvaluationContext": "dq_points", "enableDataQualityResultsPublishing": True},
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

# Convert to single CSV file
df_points.coalesce(1).write.mode("overwrite").option("header", "true").csv("s3://worldcup-data-2023/processed/points_table/")

# Deliveries
EvaluateDataQuality().process_rows(
    frame=final_deliveries,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={"dataQualityEvaluationContext": "dq_deliveries", "enableDataQualityResultsPublishing": True},
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

# Convert to single CSV file
df_deliveries_final.coalesce(1).write.mode("overwrite").option("header", "true").csv("s3://worldcup-data-2023/processed/deliveries/")

# Venues
EvaluateDataQuality().process_rows(
    frame=final_venues,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={"dataQualityEvaluationContext": "dq_venues", "enableDataQualityResultsPublishing": True},
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

# Convert to single CSV file
df_venues.coalesce(1).write.mode("overwrite").option("header", "true").csv("s3://worldcup-data-2023/processed/venues/")

# Players
EvaluateDataQuality().process_rows(
    frame=final_players,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={"dataQualityEvaluationContext": "dq_players", "enableDataQualityResultsPublishing": True},
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

# Convert to single CSV file
players_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("s3://worldcup-data-2023/processed/players/")

# Print stats for verification (optional)
print(f"Total venues: {df_venues.count()}")
print(f"Total matches: {df_matches_final.count()}")
print(f"Total points table entries: {df_points.count()}")
print(f"Total deliveries: {df_deliveries_final.count()}")
print(f"Total players: {players_df.count()}")

# Job Commit
job.commit()