# Databricks notebook source
### COMP 4334
### Lab 8
### Michael Ghattas
### May/26/2025


from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, window
from pyspark.sql.types import *
import time

spark = SparkSession.builder.appName("FIFATrendingHashtags").getOrCreate()

# Define Schema
fifaSchema = StructType([
    StructField('ID', LongType(), True),
    StructField('lang', StringType(), True),
    StructField('Date', TimestampType(), True),
    StructField('Source', StringType(), True),
    StructField('len', LongType(), True),
    StructField('Orig_Tweet', StringType(), True),
    StructField('Tweet', StringType(), True),
    StructField('Likes', LongType(), True),
    StructField('RTs', LongType(), True),
    StructField('Hashtags', StringType(), True),
    StructField('UserMentionNames', StringType(), True),
    StructField('UserMentionID', StringType(), True),
    StructField('Name', StringType(), True),
    StructField('Place', StringType(), True),
    StructField('Followers', LongType(), True),
    StructField('Friends', LongType(), True)
])

# Load Full CSV (Static)
full_df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(fifaSchema) \
    .load("dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/FIFA.csv") \
    .dropna(subset=["ID", "Date", "Hashtags"]) \

# Explode Hashtags
hashtags_df = full_df.select(
    col("ID"), col("Date"), explode(split(col("Hashtags"), ",")).alias("Hashtag")
)

# Sliding Window Count of Hashtags (Static)
static_windowed = hashtags_df.groupBy(
    window(col("Date"), "60 minutes", "30 minutes"), col("Hashtag")
).count().filter("count > 100").orderBy("window")

# Show Results (Static)
static_windowed.show(25, truncate=False)

# COMMAND ----------

# Input source path
source_path = "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/FIFA.csv"
# Target streaming input folder
stream_folder = "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/stream_input"

# Split FIFA static dataframe into equal parts for streaming simulation
df_parts = full_df.randomSplit([1.0] * 50, seed=42)  # creates partitions

# Ensure the stream folder is clean
dbutils.fs.rm(stream_folder, recurse=True)
dbutils.fs.mkdirs(stream_folder)

# Prepare Streaming
fifa_stream = spark.readStream \
    .schema(fifaSchema) \
    .option("header", "true") \
    .option("mode", "DROPMALFORMED") \
    .option("maxFilesPerTrigger", 1) \
    .csv(stream_folder)

# Transform Streaming Hashtags
stream_hashtags = fifa_stream.select("Date", explode(split("Hashtags", ",")).alias("Hashtag"))

# Apply watermarking and windowing
stream_windowed_counts = stream_hashtags \
    .withWatermark("Date", "24 hours") \
    .groupBy(
        window("Date", "60 minutes", "30 minutes"),
        "Hashtag"
    ).count() \
    .filter("count > 100") \
    .orderBy("window")

# Output Streaming to Memory Sink
query = stream_windowed_counts.writeStream \
    .format("memory") \
    .outputMode("complete") \
    .queryName("HashtagCounts") \
    .trigger(processingTime="5 seconds") \
    .start()

# Wait to ensure Spark is watching the folder
time.sleep(5)

# COMMAND ----------

# Simulate incoming streaming files into flat directory (stream_folder)
for i, part in enumerate(df_parts):
    # Temporary path to write the coalesced CSV file
    temp_path = f"{stream_folder}/temp_file_{i}"
    
    # Write single CSV file to temp location
    part.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(temp_path)

    # Identify the actual .csv file Spark created
    csv_files = [f for f in dbutils.fs.ls(temp_path) if f.name.endswith(".csv")]
    
    if not csv_files:
        raise Exception(f"No CSV file found in {temp_path}")
    
    source_file = csv_files[0].path
    destination_file = f"{stream_folder}/file_{i}.csv"

    # Move the CSV file from the temp folder directly into the stream input directory
    dbutils.fs.mv(source_file, destination_file)
    time.sleep(5)


# COMMAND ----------

time.sleep(30)
# Querying the Memory Sink
spark.sql("SELECT * FROM HashtagCounts ORDER BY window").show(100, truncate=False)