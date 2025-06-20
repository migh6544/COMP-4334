# Databricks notebook source
### COMP 4334
### Lab 5
### Michael Ghattas
### May/4/2025


## Ensure SparkContext is available
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import avg, round as round_
import csv
from io import StringIO


# Initialize Spark
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# COMMAND ----------

# Load CSV into RDD
path = "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/Master.csv"
raw_rdd = sc.textFile(path)

# Remove header
header = raw_rdd.first()

# Safe CSV parser using csv.reader
def safe_split(line):
    return next(csv.reader(StringIO(line)))

# Apply safe CSV parsing
parsed_rdd = raw_rdd.filter(lambda line: line != header).map(safe_split)

# Print and verify header structure
print("Parsed header fields:")
print(safe_split(header))

parsed_sample = parsed_rdd.take(5)
for i, row in enumerate(parsed_sample):
    print(f"Row {i} (len={len(row)}): {row}")

# COMMAND ----------

# Parse and filter
split_rdd = data_rdd.map(lambda line: line.split(",")) # Split each line

# Filter out rows with non-numeric height values
filtered_rdd = split_rdd.filter(lambda x: len(x) > 18 and x[17].isdigit())

# Map to Row RDD with cleaned fields
row_rdd = filtered_rdd.map(lambda x: Row(
    playerID=x[0],
    birthState=x[5],
    birthCountry=x[4],
    height=int(x[17])
))

# COMMAND ----------

# Define Schema and create DataFrame
schema = StructType([
    StructField("playerID", StringType(), True),
    StructField("birthState", StringType(), True),
    StructField("birthCountry", StringType(), True),
    StructField("height", IntegerType(), True)
])

df = spark.createDataFrame(row_rdd, schema)
df.createOrReplaceTempView("players")

# Check distinct birthState values
df.select("birthState").distinct().show(100)

# Ensure CO data exists
co_sample = df.filter(df.birthState == "CO").take(5)
print("Sample players from Colorado:")
for row in co_sample:
    print(row)

df.select("birthState").distinct().show(100)

# COMMAND ----------

# Query 1: Number of players born in Colorado

# SQL
print("SQL Query: Players born in Colorado")
colorado_sql = spark.sql("""
    SELECT COUNT(*) AS colorado_count
    FROM players
    WHERE birthState = 'CO'
""")
colorado_sql.show()

# DataFrame
colorado_count_df = df.filter(df.birthState == "CO").count()
print(f"DataFrame Query: Players born in Colorado = {colorado_count_df}")

# COMMAND ----------

# Query 2: Average height by birthCountry

# SQL
print("SQL Query: Average height by country")
avg_height_sql = spark.sql("""
    SELECT birthCountry, ROUND(AVG(height), 2) AS avg_height
    FROM players
    GROUP BY birthCountry
    ORDER BY avg_height DESC
""")
avg_height_sql.show(avg_height_sql.count())

# DataFrame
print("DataFrame Query: Average height by country")
avg_height_df = df.groupBy("birthCountry") \
    .agg(round_(avg("height"), 2).alias("avg_height")) \
    .orderBy("avg_height", ascending=False)
avg_height_df.show(avg_height_df.count())