# PySpark

> DataFrame API for distributed data processing in Python.
>
> **Validated against Spark 4.1**

## When to Use This Skill

- Writing Spark transformations in Python
- DataFrame operations (filter, select, join, aggregate)
- Reading/writing to various formats
- UDF creation and optimization

## Quick Start

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("quickstart").getOrCreate()

# Read
df = spark.read.table("iceberg.bronze.orders")

# Transform
df_result = (df
    .filter(f.col("status") == "completed")
    .groupBy("customer_id")
    .agg(
        f.count("*").alias("order_count"),
        f.sum("amount").alias("total_spent")
    ))

# Write
df_result.write.mode("overwrite").saveAsTable("iceberg.gold.customer_summary")
```

## Core Transformations

### Select & Filter
```python
df.select("col1", "col2", f.col("col3").alias("renamed"))
df.filter(f.col("amount") > 100)
df.filter((f.col("status") == "active") & (f.col("amount") > 0))
df.where("amount > 100")  # SQL string syntax
```

### Add & Modify Columns
```python
df.withColumn("new_col", f.lit("constant"))
df.withColumn("doubled", f.col("amount") * 2)
df.withColumn("event_date", f.to_date("event_timestamp"))
df.withColumnRenamed("old_name", "new_name")
df.drop("unwanted_column")
```

### Aggregations
```python
df.groupBy("category").agg(
    f.count("*").alias("count"),
    f.sum("amount").alias("total"),
    f.avg("amount").alias("average"),
    f.min("amount").alias("minimum"),
    f.max("amount").alias("maximum"),
    f.countDistinct("customer_id").alias("unique_customers")
)
```

### Joins
```python
# Inner join (default)
df1.join(df2, df1.id == df2.id)

# Left join
df1.join(df2, "common_column", "left")

# Multiple conditions
df1.join(df2, (df1.id == df2.id) & (df1.date == df2.date), "inner")

# Broadcast small tables
from pyspark.sql.functions import broadcast
df_large.join(broadcast(df_small), "key")
```

### Window Functions
```python
from pyspark.sql.window import Window

# Define window
window_spec = Window.partitionBy("customer_id").orderBy("order_date")

# Apply window functions
df.withColumn("row_num", f.row_number().over(window_spec))
df.withColumn("running_total", f.sum("amount").over(window_spec))
df.withColumn("prev_amount", f.lag("amount", 1).over(window_spec))
df.withColumn("next_amount", f.lead("amount", 1).over(window_spec))

# Ranking
df.withColumn("rank", f.rank().over(window_spec))
df.withColumn("dense_rank", f.dense_rank().over(window_spec))
```

## Common String Operations

```python
f.lower("column")
f.upper("column")
f.trim("column")
f.concat("col1", f.lit("-"), "col2")
f.concat_ws("-", "col1", "col2", "col3")
f.substring("column", 1, 5)
f.split("column", ",")
f.regexp_replace("column", r"\d+", "X")
f.regexp_extract("column", r"(\d+)", 1)
```

## Date/Time Operations

```python
f.current_date()
f.current_timestamp()
f.to_date("string_col", "yyyy-MM-dd")
f.to_timestamp("string_col", "yyyy-MM-dd HH:mm:ss")
f.date_format("date_col", "yyyy-MM")
f.datediff("end_date", "start_date")
f.date_add("date_col", 7)
f.date_sub("date_col", 7)
f.year("date_col")
f.month("date_col")
f.dayofweek("date_col")
```

## Null Handling

```python
df.filter(f.col("column").isNull())
df.filter(f.col("column").isNotNull())
df.na.drop()  # Drop rows with any null
df.na.drop(subset=["col1", "col2"])  # Drop if specific cols null
df.na.fill(0)  # Fill all numeric nulls
df.na.fill({"col1": 0, "col2": "unknown"})  # Fill specific columns
f.coalesce("col1", "col2", f.lit("default"))  # First non-null
```

## Reading & Writing

### Iceberg Tables
```python
# Read
df = spark.read.table("iceberg.bronze.events")

# Write (append)
df.write.mode("append").saveAsTable("iceberg.silver.events")

# Write (overwrite)
df.write.mode("overwrite").saveAsTable("iceberg.gold.summary")

# Merge/upsert
df.writeTo("iceberg.silver.events").using("iceberg").createOrReplace()
```

### Parquet
```python
df = spark.read.parquet("s3a://bucket/path/")
df.write.mode("overwrite").parquet("s3a://bucket/output/")
```

### CSV
```python
df = spark.read.option("header", True).option("inferSchema", True).csv("path/")
df.write.option("header", True).mode("overwrite").csv("output/")
```

### JSON
```python
df = spark.read.json("path/")
df.write.mode("overwrite").json("output/")
```

## Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `AnalysisException: cannot resolve column` | Column name typo or missing | Check `df.columns` or `df.printSchema()` |
| `Column is not iterable` | Used column in Python context | Use `f.col("name")` not just `"name"` in expressions |
| `NullPointerException` in UDF | Null values not handled | Add null checks in UDF or filter nulls first |
| `OutOfMemoryError` | Data skew or too much in driver | Use `repartition()`, avoid `collect()` |

## Performance Tips

```python
# Cache intermediate results used multiple times
df_cached = df.cache()

# Repartition for parallelism
df.repartition(200)  # By count
df.repartition("partition_col")  # By column

# Coalesce to reduce partitions (no shuffle)
df.coalesce(10)

# Avoid UDFs when built-in functions exist
# Bad: udf(lambda x: x.upper())
# Good: f.upper("column")

# Use broadcast for small dimension tables
from pyspark.sql.functions import broadcast
df_fact.join(broadcast(df_dim), "key")
```

## See Also

- [Structured-Streaming.md](Structured-Streaming.md) - Real-time processing
- [Spark-SQL.md](Spark-SQL.md) - SQL syntax patterns
- [SDP.md](SDP.md) - Declarative pipeline definitions
