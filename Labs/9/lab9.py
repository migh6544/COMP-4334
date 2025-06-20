# Databricks notebook source
### COMP 4334
### Lab 9
### Michael Ghattas
### Jun/01/2025


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from graphframes import GraphFrame

# Initialize Spark session
spark = SparkSession.builder.appName("Lab9_GraphFrames").getOrCreate()

# Load and prepare airports.csv
airport_columns = ["id", "name", "city", "country", "iata", "icao", 
                   "latitude", "longitude", "altitude", "timezone", 
                   "dst", "tz_database", "type", "source"]

df1_raw = spark.read.format("csv").option("header", "false") \
    .load("dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/airports.csv")
df1 = df1_raw.toDF(*airport_columns)

# Load and prepare routes.csv
df2_raw = spark.read.format("csv").option("header", "true") \
    .load("dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/routes.csv")

df2 = df2_raw.select(
    col(" source airport").alias("sourceAirport"),
    col(" destination apirport").alias("destAirport")
).distinct()

# Filter for US airports only
us_airports = df1.filter(
    (col("country") == "United States") & (col("iata").isNotNull())
).select("iata").distinct()

us_iata_list = [row["iata"] for row in us_airports.collect()]

# Keep only US-to-US routes
us_routes = df2.filter(col("sourceAirport").isin(us_iata_list)) \
               .filter(col("destAirport").isin(us_iata_list)) \
               .withColumnRenamed("sourceAirport", "src") \
               .withColumnRenamed("destAirport", "dst")

# Create Vertices and GraphFrame
vertices = us_routes.select("src").union(us_routes.select("dst")) \
    .distinct().withColumnRenamed("src", "id")

g = GraphFrame(vertices, us_routes)

# COMMAND ----------


# Count of US airports and routes
print("QUERY-1:")
print(f"Number of US airports: {vertices.count()}")
print(f"Number of US to US routes: {us_routes.count()}")

# COMMAND ----------


# One-way flights to/from DEN
print("QUERY-2:")
one_way = us_routes.alias("a").join(us_routes.alias("b"),
    (col("a.src") == col("b.dst")) & (col("a.dst") == col("b.src")), "left_anti"
).filter((col("a.src") == "DEN") | (col("a.dst") == "DEN")) \
 .select(col("a.src").alias("src"), col("a.dst").alias("dst"))

one_way_filtered = one_way.withColumn("IATA", 
    when(col("src") == "DEN", col("dst")).otherwise(col("src"))
).filter(col("IATA") != "DEN").select("IATA").distinct()

print("Airports with one-way flight to/from DEN but not roundtrip:")
one_way_filtered.show()

# COMMAND ----------

# Airports ≥ 4 hops away from DEN
print("QUERY-3:")
shortest_paths = g.shortestPaths(landmarks=["DEN"])

reachable_from_DEN = shortest_paths \
    .filter(col("distances").isNotNull()) \
    .withColumn("Hops", col("distances")["DEN"]) \
    .filter(col("Hops") >= 4) \
    .select("id", "Hops")

print("Airports that require 4+ flights to reach from DEN:")
reachable_from_DEN.orderBy("Hops").show()