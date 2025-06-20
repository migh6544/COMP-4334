# Databricks notebook source
### COMP 4334
### Assignment 2
### Michael Ghattas
### Jun/07/2025


from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, Bucketizer
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder.appName("Assignment2_Streaming_ML").getOrCreate()

# Establish DBFS paths
input_csv = "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/heart.csv"
training_path = "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/heart_train"
stream_path = "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/heart_stream"

# Clean old files
for path in [training_path, stream_path]:
    try:
        dbutils.fs.rm(path, True)
    except:
        pass

# Load data
heartDF = spark.read.option("header", True).option("inferSchema", True).csv(input_csv)

# Age buckets
splits = [0, 40, 50, 60, 70, float("inf")]
bucketizer = Bucketizer(splits=splits, inputCol="Age", outputCol="AgeGroup")

# Categorical to numeric
sexIndexer = StringIndexer(inputCol="Sex", outputCol="SexIndex")
cpIndexer = StringIndexer(inputCol="ChestPainType", outputCol="CPIndex")
ecgIndexer = StringIndexer(inputCol="RestingECG", outputCol="ECGIndex")
anginaIndexer = StringIndexer(inputCol="ExerciseAngina", outputCol="AnginaIndex")
slopeIndexer = StringIndexer(inputCol="ST_Slope", outputCol="SlopeIndex")

# Feature assembler
assembler = VectorAssembler(
    inputCols=["AgeGroup", "SexIndex", "CPIndex", "RestingBP", "Cholesterol", "FastingBS",
               "ECGIndex", "MaxHR", "AnginaIndex", "Oldpeak", "SlopeIndex"],
    outputCol="features"
)

# Model
lr = LogisticRegression(featuresCol="features", labelCol="HeartDisease")

# Pipeline
pipeline = Pipeline(stages=[
    bucketizer, sexIndexer, cpIndexer, ecgIndexer,
    anginaIndexer, slopeIndexer, assembler, lr
])

# Split into training and testing
trainDF, testDF = heartDF.randomSplit([0.7, 0.3], seed=42)

# Save for training and streaming
trainDF.write.mode("overwrite").option("header", True).csv(training_path)
testDF.repartition(10).write.mode("overwrite").option("header", True).csv(stream_path)

# Train model
model = pipeline.fit(trainDF)

# Static transformation output
transformed = model.transform(testDF)
transformed.select("Age", "Sex", "HeartDisease", "probability", "prediction").show(20, False)

# Streaming prediction section 
schema = heartDF.schema

streamingDF = (
    spark.readStream.schema(schema).option("header", True)
    .csv(stream_path)
)

streamed = model.transform(streamingDF)

query = (
    streamed.select("Age", "Sex", "HeartDisease", "probability", "prediction")
    .writeStream
    .format("memory")
    .queryName("heart_preds")
    .outputMode("append")
    .start()
)

# Allow time for stream to start
import time
time.sleep(10)

# Query in-memory results
spark.sql("SELECT * FROM heart_preds LIMIT 20").show(truncate=False)


# COMMAND ----------

# Model Analysis
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import col, when


# Accuracy Evaluation on Static Test Data (One-Hot Encoding)
results = transformed.withColumn("correct", when(col("HeartDisease") == col("prediction"), 1).otherwise(0))

# Compute overall accuracy by averaging
accuracy = results.selectExpr("avg(correct)").first()[0]
print(f"Static Test Accuracy: {accuracy:.4f}")

# BinaryClassificationEvaluator to measure the quality of probabilistic predictions
evaluator = BinaryClassificationEvaluator(
    labelCol="HeartDisease",
    rawPredictionCol="probability",
    metricName="areaUnderROC"
)

# Area Under ROC Curve (AUC) Evaluation
auc = evaluator.evaluate(transformed)
print(f"AUC (ROC): {auc:.4f}")

# Confusion Matrix (Group by true label and predicted label to count occurrences)
confusion = results.groupBy("HeartDisease", "prediction").count().orderBy("HeartDisease", "prediction")
print("Confusion Matrix:")
confusion.show()

# Count total number of records that were processed by the stream
stream_count = spark.sql("SELECT COUNT(*) AS total FROM heart_preds").first()["total"]
print(f"Total Streamed Records Processed: {stream_count}")

# Show a sample of streaming predictions
print("Sample Streaming Predictions:")
spark.sql("""
    SELECT Age, Sex, HeartDisease, prediction, probability
    FROM heart_preds
    LIMIT 10
""").show(truncate=False)


# COMMAND ----------

# Data Analysis
from pyspark.sql.functions import count, mean, stddev, min, max, col


# Summary Statistics (Numerical Features)
print("Summary Stats for Numerical Variables:")
numerical_cols = ["Age", "RestingBP", "Cholesterol", "MaxHR", "Oldpeak"]
heartDF.select(numerical_cols).describe().show()

# Heart Disease Prevalence (Target Distribution)
print("Heart Disease Class Distribution:")
heartDF.groupBy("HeartDisease").count().orderBy("HeartDisease").show()

# Age Distribution
print("Heart Disease Rate by Age Group:")
from pyspark.ml.feature import Bucketizer

# Apply same age binning logic used in model
splits = [0, 40, 50, 60, 70, float("inf")]
bucketizer = Bucketizer(splits=splits, inputCol="Age", outputCol="AgeGroup")
binnedDF = bucketizer.transform(heartDF)

# Show disease prevalence by age bin
binnedDF.groupBy("AgeGroup").agg(
    count("*").alias("Total"),
    mean("HeartDisease").alias("DiseaseRate")
).orderBy("AgeGroup").show()

# Cholesterol vs Heart Disease
print("Average Cholesterol by Heart Disease Status:")
heartDF.groupBy("HeartDisease").agg(
    mean("Cholesterol").alias("AvgCholesterol")
).show()

# Gender Differences
print("Heart Disease Rate by Sex:")
heartDF.groupBy("Sex").agg(
    count("*").alias("Total"),
    mean("HeartDisease").alias("DiseaseRate")
).show()