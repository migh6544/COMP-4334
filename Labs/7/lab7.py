# Databricks notebook source
### COMP 4334
### Lab 7
### Michael Ghattas
### May/18/2025


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, trim
from pyspark.sql.types import StringType, DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression


# Initialize Spark Session
spark = SparkSession.builder.appName("HeartDiseasePrediction").getOrCreate()

# File paths
train_file = "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/heartTraining.csv"
test_file = "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/heartTesting.csv"

# Load the data
train_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(train_file)
test_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(test_file)

# Strip whitespace from column names
train_df = train_df.select([trim(col(c)).alias(c.strip()) for c in train_df.columns])
test_df = test_df.select([trim(col(c)).alias(c.strip()) for c in test_df.columns])

# Convert "chol" to double
train_df = train_df.withColumn("chol", col("chol").cast(DoubleType()))
test_df = test_df.withColumn("chol", col("chol").cast(DoubleType()))

# Transform Age into Categorical Bins
def age_category(age):
    age = float(age)
    if age < 40:
        return "Below 40"
    elif 40 <= age < 50:
        return "40-49"
    elif 50 <= age < 60:
        return "50-59"
    elif 60 <= age < 70:
        return "60-69"
    else:
        return "70 and above"

age_udf = udf(age_category, StringType())
train_df = train_df.withColumn("AgeCategory", age_udf(col("age")))
test_df = test_df.withColumn("AgeCategory", age_udf(col("age")))

# Convert Sex and pred to numeric
sex_indexer = StringIndexer(inputCol="sex", outputCol="SexIndexed")
pred_indexer = StringIndexer(inputCol="pred", outputCol="label")

# Convert AgeCategory to numeric
age_indexer = StringIndexer(inputCol="AgeCategory", outputCol="AgeCategoryIndexed")

# Assemble feature vector
assembler = VectorAssembler(inputCols=["chol", "SexIndexed", "AgeCategoryIndexed"], outputCol="features")

# Define the Logistic Regression model
lr = LogisticRegression(featuresCol="features", labelCol="label")

# Build the Pipeline
pipeline = Pipeline(stages=[sex_indexer, pred_indexer, age_indexer, assembler, lr])

# Train the model
model = pipeline.fit(train_df)

# Make predictions on the test set
predictions = model.transform(test_df)

# Print results by selecting id, probability, and prediction
results = predictions.select("id", "probability", "prediction").limit(100)
results.show(100, truncate=False)
