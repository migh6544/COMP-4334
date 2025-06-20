# Databricks notebook source
### COMP 4334
### Lab 3
### Michael Ghattas
### Apr/20/2025


## Ensure SparkContext is available and import random library
from pyspark import SparkContext
import random


# Ensure a working Spark context
sc = SparkContext.getOrCreate()

# COMMAND ----------

# Define helper function for (target, source) pair mapping
def extract_target_source_pairs(line):
    tokens = line.strip().split()
    if len(tokens) < 2:
        return []
    source = tokens[0]
    targets = tokens[1:]
    return [(target, source) for target in targets]

# COMMAND ----------

# Define function to build reverse web index
def build_reverse_index(input_path):
    return (
        sc.textFile(input_path)
          .flatMap(extract_target_source_pairs)          # (target, source)
          .groupByKey()                                  # (target, [sources])
          .mapValues(lambda srcs: sorted(set(srcs)))     # sort and deduplicate
          .sortByKey()                                   # sort by target URL
    )

# COMMAND ----------

# Run on short dataset 
short_path = (
    "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/shortLab3data0.txt,"
    "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/shortLab3data1.txt"
)
short_index = build_reverse_index(short_path)
short_results = short_index.collect()

print("SHORT DATASET RESULTS:")
for target, sources in short_results:
    print(f"{target} <- {sources}")


# COMMAND ----------

# Run on full dataset
full_path = (
    "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/fullLab3data0.txt,"
    "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/fullLab3data1.txt,"
    "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/fullLab3data2.txt,"
    "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/fullLab3data3.txt"
)
full_index = build_reverse_index(full_path)
full_count = full_index.count()
full_sample = full_index.take(10)

print("FULL DATASET SUMMARY:")
print(f"Total unique target URLs: {full_count}")
print("Sample of 10 entries:")
for target, sources in full_sample:
    print(f"{target} <- {sources}")