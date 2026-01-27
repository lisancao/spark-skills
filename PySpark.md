# PySpark

> DataFrame API for distributed data processing in Python.
>
> **Validated against Spark 4.1**

## When to Use This Skill

- Writing Spark transformations in Python
- DataFrame operations (filter, select, join, aggregate)
- Reading/writing to various formats
- UDF creation and optimization

## Spark 4.1 Requirements

| Requirement | Version |
|-------------|---------|
| Python | 3.10+ (dropped 3.9) |
| JDK | 17+ (dropped 8/11) |
| Pandas | 2.2.0+ |
| NumPy | 1.22+ |
| PyArrow | 15.0.0+ |

## Quick Reference

| Task | Code |
|------|------|
| Filter rows | `df.filter(f.col("x") > 0)` |
| Select columns | `df.select("a", "b")` |
| Add column | `df.withColumn("new", expr)` |
| Rename column | `df.withColumnRenamed("old", "new")` |
| Conditional | `f.when(cond, val).otherwise(default)` |
| Group & aggregate | `df.groupBy("x").agg(f.sum("y"))` |
| Join | `df1.join(df2, "key", "left")` |
| Sort | `df.orderBy(f.col("x").desc())` |
| Deduplicate | `df.dropDuplicates(["id"])` |
| Cast type | `f.col("x").cast("int")` |

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

# Spark 4.1: Batch column creation with withColumns()
df.withColumns({
    "doubled": f.col("amount") * 2,
    "event_date": f.to_date("event_timestamp"),
    "status_upper": f.upper("status")
})
```

### Conditional Logic
```python
# when/otherwise - the most common pattern
df.withColumn(
    "tier",
    f.when(f.col("amount") >= 1000, "premium")
     .when(f.col("amount") >= 100, "standard")
     .otherwise("basic")
)

# Multiple conditions with AND/OR
df.withColumn(
    "flag",
    f.when((f.col("status") == "active") & (f.col("amount") > 0), True)
     .otherwise(False)
)

# Nested when (complex logic)
df.withColumn(
    "category",
    f.when(f.col("type") == "A",
        f.when(f.col("amount") > 100, "A-high").otherwise("A-low")
    ).otherwise("other")
)
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
df.withColumn("ntile", f.ntile(4).over(window_spec))  # Quartiles

# Window frames
window_rows = Window.partitionBy("customer_id").orderBy("order_date").rowsBetween(-2, 0)
df.withColumn("rolling_avg_3", f.avg("amount").over(window_rows))
```

### Sorting
```python
# Basic ordering
df.orderBy("amount")                              # Ascending (default)
df.orderBy(f.col("amount").desc())               # Descending
df.orderBy("category", f.col("amount").desc())   # Multiple columns

# Using sort() (alias for orderBy)
df.sort("amount")
df.sort(f.desc("amount"))

# Nulls handling
df.orderBy(f.col("amount").desc_nulls_last())    # Nulls at end
df.orderBy(f.col("amount").asc_nulls_first())    # Nulls at start
```

### Deduplication
```python
# Remove exact duplicate rows
df.distinct()

# Remove duplicates based on specific columns
df.dropDuplicates(["customer_id"])               # Keep first occurrence
df.dropDuplicates(["customer_id", "order_date"])

# Keep latest record per key (common pattern)
from pyspark.sql.window import Window
window = Window.partitionBy("customer_id").orderBy(f.col("updated_at").desc())
df.withColumn("rn", f.row_number().over(window)).filter(f.col("rn") == 1).drop("rn")
```

### Union Operations
```python
# Union (columns must match by position)
df1.union(df2)

# UnionByName (matches by column name, safer)
df1.unionByName(df2)

# UnionByName with missing columns allowed
df1.unionByName(df2, allowMissingColumns=True)

# Union multiple DataFrames
from functools import reduce
dfs = [df1, df2, df3]
result = reduce(lambda a, b: a.unionByName(b), dfs)
```

### Type Casting
```python
# Cast single column
df.withColumn("amount_int", f.col("amount").cast("int"))
df.withColumn("amount_decimal", f.col("amount").cast("decimal(10,2)"))

# Common type casts
f.col("x").cast("string")
f.col("x").cast("int")           # or "integer"
f.col("x").cast("long")          # or "bigint"
f.col("x").cast("double")
f.col("x").cast("boolean")
f.col("x").cast("date")
f.col("x").cast("timestamp")

# Using StructType
from pyspark.sql.types import IntegerType
df.withColumn("amount_int", f.col("amount").cast(IntegerType()))

# Spark 4.1 ANSI mode: try_cast for safe casting (returns null on failure)
df.selectExpr("try_cast(amount as int) as amount_int")
```

### DataFrame Creation (Testing)
```python
# From list of tuples
df = spark.createDataFrame([
    ("alice", 100),
    ("bob", 200),
], ["name", "amount"])

# From list of dicts
df = spark.createDataFrame([
    {"name": "alice", "amount": 100},
    {"name": "bob", "amount": 200},
])

# With explicit schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema = StructType([
    StructField("name", StringType()),
    StructField("amount", IntegerType()),
])
df = spark.createDataFrame([("alice", 100)], schema)

# Empty DataFrame with schema
df = spark.createDataFrame([], schema)
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

## ANSI Mode (Spark 4.x Default)

Spark 4.x enables ANSI mode by default. This changes error handling:

```python
# Division by zero: RAISES ERROR in ANSI mode
df.withColumn("ratio", f.col("a") / f.col("b"))  # Error if b=0

# FIX: Use try_divide() for safe division
df.withColumn("ratio", f.expr("try_divide(a, b)"))

# Invalid casts: RAISE ERROR in ANSI mode
df.withColumn("num", f.col("text").cast("int"))  # Error if text="abc"

# FIX: Use try_cast() for safe casting
df.selectExpr("try_cast(text as int) as num")

# Integer overflow: RAISES ERROR in ANSI mode
# FIX: Use appropriate types (long, decimal)
```

## Deprecated/Removed in Spark 4.x

```python
# REMOVED - Use alternatives:
# DataFrame.append() → Use ps.concat() with pandas API
# Series.iteritems() → Use Series.items()
# Int64Index, Float64Index → Use Index directly

# Arrow-optimized UDFs (Spark 4.1+)
@f.udf(returnType=StringType(), useArrow=True)
def my_udf(x):
    return x.upper()
```

## Parameterized SQL (Spark 4.1+)

```python
# Safe parameterized queries (prevents SQL injection)
result = spark.sql(
    "SELECT * FROM orders WHERE customer_id = :cid AND amount > :min",
    args={"cid": "C001", "min": 100}
)
```

## Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `AnalysisException: cannot resolve column` | Column name typo or missing | Check `df.columns` or `df.printSchema()` |
| `Column is not iterable` | Used column in Python context | Use `f.col("name")` not just `"name"` in expressions |
| `NullPointerException` in UDF | Null values not handled | Add null checks in UDF or filter nulls first |
| `OutOfMemoryError` | Data skew or too much in driver | Use `repartition()`, avoid `collect()` |
| `ArithmeticException: divide by zero` | ANSI mode division by zero | Use `try_divide()` or filter zeros first |
| `NumberFormatException` | ANSI mode invalid cast | Use `try_cast()` for safe type conversion |

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
