# Databricks notebook source
### COMP 4334
### Lab 2
### Michael Ghattas
### Apr/13/2025


## Ensure SparkContext is available and import random library
from pyspark import SparkContext
import random


# Ensure a working Spark context
sc = SparkContext.getOrCreate()

# COMMAND ----------

## Prime Exercise: Count primes from 100 to 10,000

# Helper function to test primality
def is_prime(n):
    if n < 2:
        return False
    for i in range(2, int(n**0.5) + 1):
        if n % i == 0:
            return False
    return True

# Generate list of numbers from 100 to 10,000
nums = list(range(100, 10001))

# Parallelize and count primes
prime_count = sc.parallelize(nums).filter(lambda x: is_prime(x)).count()

# Print results
print(f"Number of primes between 100 and 10,000: {prime_count}")

# COMMAND ----------

## Celsius Exercise: Average Celsius > freezing

# Generate 1000 random Fahrenheit temperatures between 0 and 100
fahrenheit_list = [random.uniform(0, 100) for _ in range(1000)]

# Create RDD
fahrenheit_rdd = sc.parallelize(fahrenheit_list)

# Convert to Celsius
celsius_rdd = fahrenheit_rdd.map(lambda f: (f - 32) * 5.0 / 9)

# Filter above freezing
above_freezing_rdd = celsius_rdd.filter(lambda c: c > 0)

# Persist to avoid recomputation
above_freezing_rdd.persist()

# Count and sum
count = above_freezing_rdd.count()
total = above_freezing_rdd.reduce(lambda x, y: x + y)

# Compute and print average
if count > 0:
    avg_celsius = total / count
    print(f"Average Celsius temperature above freezing: {avg_celsius:.2f}°C")
else:
    print("No temperatures above freezing.")