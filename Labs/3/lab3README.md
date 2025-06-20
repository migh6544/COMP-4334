# COMP 4334 - Lab 3: MapReduce-Based Web Index Using PySpark

**Author:** Michael Ghattas  
**Course:** COMP 4334 – Parallel and Distributed Computing  
**Lab Title:** Lab 3 – Building a Reverse Web Index using Spark RDDs  
**Environment:** Databricks with PySpark  

---

## Objective

The goal of this lab is to use **Apache Spark** and **MapReduce-style transformations** to construct a **reverse web index**. Given a list of source URLs and their outbound links, the program builds an index that maps each **target URL** to a **sorted list of source URLs** that reference it.

---

## Datasets Used

### Short Dataset:
- `dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/shortLab3data0.txt`
- `dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/shortLab3data1.txt`

### Full Dataset:
- `dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/fullLab3data0.txt`
- `dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/fullLab3data1.txt`
- `dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/fullLab3data2.txt`
- `dbfs:/FileStore/shared_uploads/michael.ghattas@du.edu/fullLab3data3.txt`

---

## How It Works

1. **Each line of input** contains a source URL followed by one or more target URLs.
2. The `extract_target_source_pairs()` function emits `(target, source)` pairs for each line.
3. These pairs are processed using Spark RDD transformations:
   - `flatMap()` to emit multiple (target, source) pairs.
   - `groupByKey()` to group all source URLs by target.
   - `mapValues()` to **sort and deduplicate** source URLs for each target.
   - `sortByKey()` to return a lexicographically ordered result.

---

## Key Code Features

- Written in **pure PySpark** using **RDDs**.
- Handles both short and full datasets.
- Includes `.collect()` output for short file verification.
- Includes `.count()` and `.take(10)` for efficient previewing of large datasets.

---