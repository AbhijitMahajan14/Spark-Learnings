# 🧠 Lesson 1: Introduction to Data Processing & Big Data Concepts

## 1. What Is Data Processing?
Data processing means **collecting raw data → cleaning → transforming → analyzing** to get useful insights.

### Example
- Raw data: CSV of customer purchases.  
- Processing steps: Remove nulls → calculate total per customer → store results in database.

```python
import pandas as pd

# sample dataset
data = {'Customer': ['A', 'B', 'A', 'C'],
        'Amount': [100, 200, 150, 300]}

df = pd.DataFrame(data)

# process: total purchase per customer
result = df.groupby('Customer')['Amount'].sum().reset_index()
print(result)
```
This is *small-scale data processing*. When your data becomes **massive (TBs or PBs)**, you need distributed systems like Hadoop or Spark.

---

## 2. Understanding Big Data
**Big Data** refers to datasets that are too large or complex to be handled by traditional tools (like Excel or a single machine).

We often define Big Data using the **3 Vs**:
- **Volume** – Huge amount of data (terabytes or more)
- **Velocity** – Data is generated fast (e.g., IoT sensors)
- **Variety** – Different formats (CSV, JSON, images, logs)

---

## 3. Types of Data
| Type | Description | Example |
|------|--------------|----------|
| **Structured** | Follows a fixed schema (rows, columns) | SQL tables, CSV files |
| **Semi-Structured** | Has tags or key-value pairs but not fixed schema | JSON, XML |
| **Unstructured** | No predefined format | Images, videos, PDFs, emails |

**Mini exercise**  
Identify these:  
- CSV file → structured  
- JSON logs → semi-structured  
- YouTube videos → unstructured  

---

## 4. Big Data Problems and Their Solutions

Traditional (monolithic) systems process data on a **single machine**. But when data exceeds hardware limits, the system slows or crashes.

### ✅ Solution: Distributed Computing
Split big data across many machines (nodes), process chunks in parallel, then combine results.

```
Task: Count words in 1TB text file
Traditional: One machine → too slow
Distributed: 10 machines → each counts part → merge results
```

That’s the core idea behind **Hadoop** and **Spark**.

---

## 🏗️ Hadoop Architecture Overview

Hadoop is a **distributed framework** for storing and processing big data.  
It has two main parts:

1. **HDFS (Hadoop Distributed File System)** – stores large files across multiple machines.
2. **MapReduce** – splits tasks into smaller jobs and processes them in parallel.

### 🧩 HDFS Example (Conceptual)
- File `data.txt` = 300MB  
- HDFS splits it into 3 chunks (100MB each)  
- Stores each on different nodes  
- If one node fails, data is still safe due to replication (default 3 copies)

### 🧩 MapReduce Example (Conceptual)
Word count job:
1. **Map** → Each node counts words in its chunk.  
2. **Reduce** → Combine results from all nodes.

---

## ⚡ Introduction to Apache Spark

After Hadoop, engineers wanted **faster** and **easier** tools — that’s where **Spark** came in.

| Feature | Hadoop MapReduce | Spark |
|----------|------------------|-------|
| Speed | Slower (disk I/O heavy) | Faster (in-memory processing) |
| Ease of Use | Java-heavy | APIs in Python, Scala, SQL |
| Flexibility | Batch only | Batch + Streaming + ML + SQL |

Spark can run **with or without Hadoop**:
- With Hadoop → uses HDFS for storage.  
- Without Hadoop → can use S3, local files, or other data sources.

---

## 🧠 Modern Concepts: Data Lake & Lakehouse

- **Data Lake** – Centralized storage of raw structured + unstructured data (e.g., on S3, GCS, or Azure Data Lake).
- **Lakehouse** – Combines Data Lake’s flexibility with Data Warehouse’s reliability (e.g., Delta Lake, BigLake).

🧩 Think of it like:
> Data Lake = warehouse for all raw materials  
> Lakehouse = refined version with better structure + analytics

---

## 🧪 Hands-On: Run Your First Spark Program

If you have Python + PySpark installed (or Databricks Community account):

```python
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("IntroToSpark").getOrCreate()

# Create DataFrame
data = [("A", 100), ("B", 200), ("A", 150)]
columns = ["Customer", "Amount"]
df = spark.createDataFrame(data, columns)

# Group by and sum
result = df.groupBy("Customer").sum("Amount")
result.show()
```

**Output**
```
+--------+-----------+
|Customer|sum(Amount)|
+--------+-----------+
|A       |250        |
|B       |200        |
+--------+-----------+
```

✅ You’ve just performed distributed data processing — similar to what Hadoop would do, but faster and simpler.

---

## 🎯 Summary
| Concept | Key Takeaway |
|----------|---------------|
| **Big Data** | Data too large for one machine |
| **Structured/Semi/Unstructured** | Classification by format |
| **Monolithic vs Distributed** | Single machine vs cluster processing |
| **Hadoop** | Distributed storage (HDFS) + processing (MapReduce) |
| **Spark** | In-memory fast computation engine |
| **Data Lake/Lakehouse** | Modern storage architecture for all data |
