# Databricks notebook source
### COMP 4334
### Lab 6
### Michael Ghattas
### May/11/2025


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark Session
spark = SparkSession.builder.appName("Lab6").getOrCreate()

# COMMAND ----------

# Load DataFrames
master_path = "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/Master-1.csv"
masterDF = spark.read.option("header", "true").csv(master_path).select("playerID", "nameFirst", "nameLast")

teams_path = "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/Teams.csv"
teamsDF = spark.read.option("header", "true").csv(teams_path).select("teamID", "name").withColumnRenamed("name", "teamName")

allstar_path = "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/AllstarFull.csv"
allstarDF = spark.read.option("header", "true").csv(allstar_path).select("playerID", "teamID")

# COMMAND ----------

# Join AllstarFull with Master on playerID
allstar_master_df = allstarDF.join(masterDF, "playerID")

# Join the result with Teams on teamID
allstars_df = allstar_master_df.join(teamsDF, "teamID").select("playerID", "teamID", "nameFirst", "nameLast", "teamName").distinct()

# COMMAND ----------

# Save as Partitioned Parquet and partition by teamName
output_path = "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/allstars_parquet"
allstars_df.write.mode("overwrite").partitionBy("teamName").parquet(output_path)

# Validate Parquet and reload and filter for Colorado Rockies
loaded_df = spark.read.parquet(output_path)

# COMMAND ----------

# Filter for Colorado Rockies All-Stars
rockies_allstars_df = loaded_df.filter(loaded_df.teamName == "Colorado Rockies")

# Display result count and full list
rockies_count = rockies_allstars_df.count()
print(f"Colorado Rockies All-Stars count: {rockies_count}")
rockies_allstars_df.show(rockies_count, truncate=False)