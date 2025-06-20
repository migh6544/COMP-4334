# Databricks notebook source
### COMP 4334
### Lab 4
### Michael Ghattas
### Apr/28/2025


## Ensure SparkContext is available
from pyspark import SparkContext


# Ensure a working Spark context
sc = SparkContext.getOrCreate()

# COMMAND ----------

# Setup hardcoded simple test dataset
lines = sc.parallelize([
    "a b c",
    "b a a",
    "c b",
    "d a"
])

# COMMAND ----------

# Build the links RDD
links = lines.map(lambda line: line.split()) \
             .map(lambda parts: (parts[0], parts[1:])) \
             .mapValues(lambda targets: list(set(targets)))  # Remove duplicate links

# Print initial links
print("Initial links:", links.collect())

# COMMAND ----------

# Build the initial rankings RDD
pages = links.keys().collect()
initial_rank = 1.0 / len(pages)
ranks = sc.parallelize([(page, initial_rank) for page in pages])

# Print initial rankings
print("Initial rankings:", ranks.collect())


# COMMAND ----------

# PageRank Iterations
num_iterations = 10

for i in range(num_iterations):
    print(f"\nIteration: {i}")
    
    joined = links.join(ranks)
    print("Joined RDD:", joined.collect())
    
    contributions = joined.flatMap(lambda x: [
        (neighbor, x[1][1] / len(x[1][0])) for neighbor in x[1][0]
    ])
    print("Neighbor contributions:", contributions.collect())
    
    ranks = contributions.reduceByKey(lambda x, y: x + y)
    print("New rankings:", ranks.collect())

# Final sorted rankings
final_ranks = ranks.sortBy(lambda x: -x[1]).collect()

print("\nFinal sorted rankings:")
for page, rank in final_ranks:
    print(f"{page} has rank: {rank}")

# COMMAND ----------

# Load Lab3Short files
lab3short_path = (
    "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/Lab3Short/shortLab3data0.txt,"
    "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/Lab3Short/shortLab3data1.txt"
)

lab3_lines = sc.textFile(lab3short_path)

# COMMAND ----------

# Build links RDD for lab3short
lab3_links = lab3_lines.map(lambda line: line.split()) \
                       .map(lambda parts: (parts[0], parts[1:])) \
                       .mapValues(lambda targets: list(set(targets)))

# COMMAND ----------

# Build initial ranks
lab3_pages = lab3_links.keys().collect()
initial_lab3_rank = 1.0 / len(lab3_pages)
lab3_ranks = sc.parallelize([(page, initial_lab3_rank) for page in lab3_pages])

# COMMAND ----------

# PageRank Iterations
num_iterations = 10

for _ in range(num_iterations):
    lab3_joined = lab3_links.join(lab3_ranks)
    
    lab3_contributions = lab3_joined.flatMap(lambda x: [
        (neighbor, x[1][1] / len(x[1][0])) for neighbor in x[1][0]
    ])
    
    lab3_ranks = lab3_contributions.reduceByKey(lambda x, y: x + y)

# COMMAND ----------

# Final sorted rankings for lab3short
final_lab3_ranks = lab3_ranks.sortBy(lambda x: -x[1]).collect()

print("\nFinal sorted rankings for Lab3Short:")
for page, rank in final_lab3_ranks:
    print(f"{page} has rank: {rank}")