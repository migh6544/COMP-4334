# Databricks notebook source
### COMP 4334
### Assignment 1
### Michael Ghattas
### May/17/2025

from pyspark import SparkContext
import math
import logging

# Initialize Spark Context
sc = SparkContext.getOrCreate()

# Configuration
THRESHOLD_DISTANCE = 0.75  # Distance threshold for close pairs
GRID_CELL_SIZE = 0.75      # Size of each grid cell
PARTITION_COUNT = 128      # Optimized partition count for balanced processing

# Data Loading and Cleaning
def parse_point(line):
    try:
        line = line.strip()
        if not line or line.lower().startswith("pointid"):
            return None
        parts = line.split(",")
        if len(parts) != 3:
            return None
        point_id, x, y = parts
        try:
            x = float(x.strip())
            y = float(y.strip())
        except ValueError:
            return None
        point_id = point_id.strip()
        if not point_id:
            return None
        return (point_id, x, y)
    except Exception as e:
        return None

# Load and Balance Data
geoPoints_files = [
    "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/geoPoints0.csv",
    "dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/geoPoints1.csv"
]

points_rdd = sc.union([
    sc.textFile(file)
    .filter(lambda line: line.strip() and not line.lower().startswith("pointid"))
    .map(parse_point)
    .filter(lambda point: point is not None)
    .repartition(PARTITION_COUNT)  # Early partitioning to balance load
    for file in geoPoints_files
]).cache()

# Grid Cell Mapping
def get_grid_cell(x, y, cell_size):
    return (math.floor(x / cell_size), math.floor(y / cell_size))

def generate_relevant_neighbors(i, j, distance=THRESHOLD_DISTANCE, cell_size=GRID_CELL_SIZE):
    max_offset = math.floor(distance / cell_size)
    for dx in range(-max_offset, max_offset + 1):
        for dy in range(-max_offset, max_offset + 1):
            if dx == 0 and dy == 0:
                continue  # Skip the center cell itself
            # Only include cells that are realistically within the distance threshold
            if math.sqrt((dx * cell_size) ** 2 + (dy * cell_size) ** 2) <= distance:
                yield (i + dx, j + dy)

# Generate Cell-Point Pairs
cells_rdd = points_rdd.flatMap(
    lambda p: [
        (cell, p) for cell in generate_relevant_neighbors(*get_grid_cell(p[1], p[2], GRID_CELL_SIZE))
    ]
).partitionBy(PARTITION_COUNT).cache()

# True Balanced Pair Extraction
def distance(p1, p2):
    x1, y1 = p1[1], p1[2]
    x2, y2 = p2[1], p2[2]
    return math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2)

def find_close_pairs(points_list):
    n = len(points_list)
    pairs = set()
    for i in range(n):
        p1 = points_list[i]
        for j in range(i + 1, n):
            p2 = points_list[j]
            if len(p1) == 3 and len(p2) == 3 and distance(p1, p2) <= THRESHOLD_DISTANCE:
                # Consistent ordering for pairs
                pair = (p1[0], p2[0]) if p1[0] < p2[0] else (p2[0], p1[0])
                pairs.add(pair)
    return pairs

# Efficient Grouping to Minimize Hot Partitions
close_pairs_rdd = cells_rdd.partitionBy(PARTITION_COUNT).groupByKey().flatMap(
    lambda cell_points: find_close_pairs(list(cell_points[1]))
).distinct().cache()

# Collect and Print Results
close_pairs = close_pairs_rdd.collect()
print(f"Dist:{THRESHOLD_DISTANCE}")
print(f"{len(close_pairs)} {close_pairs}")
