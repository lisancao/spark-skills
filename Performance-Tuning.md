# Performance Tuning

> Optimizing Spark job performance for speed and efficiency.
>
> **Validated against Spark 4.1**

## When to Use This Skill

- Jobs running slower than expected
- Out of memory errors
- Data skew causing stragglers
- Optimizing shuffle-heavy workloads
- Reducing cloud costs

## Quick Reference

| Issue | Solution |
|-------|----------|
| Small files | Increase `maxPartitionBytes`, use `COALESCE` |
| Too many partitions | Enable AQE coalescing |
| Skewed joins | Enable `skewJoin.enabled` |
| Slow joins | Use broadcast hints, check thresholds |
| OOM errors | Increase memory, reduce partition size |
| Shuffle bottleneck | Use SPJ with Iceberg, tune partitions |

## Adaptive Query Execution (AQE)

AQE optimizes queries at runtime using actual statistics. **Enabled by default** in Spark 3.2+.

### Core Settings
```python
# Master switch (on by default)
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Target partition size after coalescing
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")

# Coalesce small post-shuffle partitions
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

### Partition Coalescing

AQE automatically merges small partitions after shuffles:

```python
# Set initial partitions high, let AQE optimize
spark.conf.set("spark.sql.shuffle.partitions", "400")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1m")
```

**Before AQE:** Manually tune `shuffle.partitions` for each query
**With AQE:** Set high initial value, AQE reduces to optimal count

### Skew Join Optimization

AQE splits skewed partitions automatically:

```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5.0")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")
```

**How it works:** If a partition is 5x larger than median AND > 256MB, it gets split.

### Dynamic Join Strategy

AQE converts sort-merge joins to broadcast joins at runtime:

```python
# Use actual runtime sizes, not estimates
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "10m")

# Read shuffle files locally after conversion
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
```

## Join Optimization

### Broadcast Joins

Force small tables to be broadcast to all executors:

```python
from pyspark.sql.functions import broadcast

# DataFrame API
df_large.join(broadcast(df_small), "key")

# SQL hint
spark.sql("""
    SELECT /*+ BROADCAST(dim) */ *
    FROM fact
    JOIN dim ON fact.dim_id = dim.id
""")
```

### Broadcast Threshold
```python
# Default 10MB - increase for larger dimension tables
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100m")

# Disable auto-broadcast (force sort-merge)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

### Join Hints

| Hint | Use Case |
|------|----------|
| `BROADCAST` | Small table fits in memory |
| `MERGE` | Both tables large, sorted |
| `SHUFFLE_HASH` | One side fits in memory per partition |
| `SHUFFLE_REPLICATE_NL` | Very small table, cross join |

```sql
SELECT /*+ MERGE(t1) */ * FROM t1 JOIN t2 ON t1.id = t2.id;
SELECT /*+ SHUFFLE_HASH(t1) */ * FROM t1 JOIN t2 ON t1.id = t2.id;
```

### Join Order Hints (Spark 4.1+)
```sql
-- Force specific join order
SELECT /*+ LEADING(t1, t2, t3) */ *
FROM t1 JOIN t2 ON ... JOIN t3 ON ...
```

## Partition Tuning

### Shuffle Partitions
```python
# Default 200 - adjust based on data size
# Rule of thumb: data_size_gb * 2-4 partitions per GB
spark.conf.set("spark.sql.shuffle.partitions", "400")
```

### File-Based Partitions
```python
# Max bytes per partition when reading files
spark.conf.set("spark.sql.files.maxPartitionBytes", "128m")

# Minimum partitions (prevent too few)
spark.conf.set("spark.sql.files.minPartitionNum", "10")
```

### Repartitioning

```python
# Increase partitions (with shuffle)
df.repartition(200)
df.repartition("partition_column")
df.repartition(200, "partition_column")

# Decrease partitions (no shuffle, faster)
df.coalesce(10)
```

### Partition Hints in SQL
```sql
-- Reduce partitions (no shuffle)
SELECT /*+ COALESCE(3) */ * FROM large_table;

-- Repartition with shuffle
SELECT /*+ REPARTITION(100) */ * FROM large_table;

-- Repartition by column
SELECT /*+ REPARTITION(100, customer_id) */ * FROM orders;

-- Rebalance (AQE-aware repartition)
SELECT /*+ REBALANCE(100) */ * FROM skewed_table;
```

## Storage Partition Join (SPJ)

Eliminate shuffles using table partitioning (Spark 4.0+, works with Iceberg):

```python
# Enable SPJ
spark.conf.set("spark.sql.sources.v2.bucketing.enabled", "true")
spark.conf.set("spark.sql.sources.v2.bucketing.pushPartValues.enabled", "true")
```

### Iceberg Example
```sql
-- Create co-partitioned tables
CREATE TABLE orders (
    order_id BIGINT,
    customer_id BIGINT,
    amount DECIMAL(10,2)
) USING iceberg
PARTITIONED BY (bucket(16, customer_id));

CREATE TABLE customers (
    customer_id BIGINT,
    name STRING
) USING iceberg
PARTITIONED BY (bucket(16, customer_id));

-- Join avoids shuffle!
SELECT o.*, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;
```

**Check plan:** `EXPLAIN` should show no `Exchange` nodes before join.

## Caching

### When to Cache

Cache DataFrames that are:
- Used multiple times in the same job
- Expensive to compute
- Fit in cluster memory

```python
# Cache in memory
df_cached = df.cache()  # Alias for .persist(StorageLevel.MEMORY_AND_DISK)

# Force materialization
df_cached.count()

# Use cached DataFrame
result1 = df_cached.filter(...)
result2 = df_cached.groupBy(...)

# Release when done
df_cached.unpersist()
```

### Storage Levels
```python
from pyspark import StorageLevel

df.persist(StorageLevel.MEMORY_ONLY)        # Fast, may spill
df.persist(StorageLevel.MEMORY_AND_DISK)    # Default, spills to disk
df.persist(StorageLevel.DISK_ONLY)          # Large datasets
df.persist(StorageLevel.MEMORY_ONLY_SER)    # Serialized, less memory
```

### Cache Tables (SQL)
```sql
CACHE TABLE my_table;
UNCACHE TABLE my_table;
```

## Memory Configuration

### Driver Memory
```bash
--driver-memory 4g
--conf spark.driver.memoryOverhead=1g
```

### Executor Memory
```bash
--executor-memory 8g
--conf spark.executor.memoryOverhead=2g
--conf spark.memory.fraction=0.6       # Fraction for execution/storage
--conf spark.memory.storageFraction=0.5  # Fraction of above for storage
```

### Memory Overhead

For PySpark/Arrow UDFs, increase overhead:
```bash
--conf spark.executor.memoryOverheadFactor=0.2  # 20% of executor memory
```

## Handling Data Skew

### Identify Skew
```python
# Check partition sizes
df.groupBy(spark_partition_id()).count().show()

# Check key distribution
df.groupBy("join_key").count().orderBy(f.desc("count")).show(20)
```

### Salting Technique
```python
from pyspark.sql import functions as f

# Add salt to skewed key
num_salts = 10
df_salted = df.withColumn("salt", (f.rand() * num_salts).cast("int"))
df_salted = df_salted.withColumn(
    "salted_key",
    f.concat(f.col("skewed_key"), f.lit("_"), f.col("salt"))
)

# Explode small table with all salts
df_small_exploded = df_small.crossJoin(
    spark.range(num_salts).withColumnRenamed("id", "salt")
).withColumn(
    "salted_key",
    f.concat(f.col("key"), f.lit("_"), f.col("salt"))
)

# Join on salted key
result = df_salted.join(df_small_exploded, "salted_key")
```

### AQE Skew Handling (Preferred)
```python
# Let AQE handle automatically
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

## Shuffle Optimization

### Reduce Shuffle Data
```python
# Filter early
df.filter(f.col("date") >= "2024-01-01").groupBy("category").count()

# Select only needed columns
df.select("id", "amount").groupBy("id").sum("amount")
```

### Shuffle Compression
```python
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.shuffle.spill.compress", "true")
```

### Local Shuffle Read (AQE)
```python
# Read shuffle data locally when possible
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
```

## Predicate Pushdown

Ensure filters are pushed to data source:

```python
# Good - filter pushed to Iceberg/Parquet
df = spark.read.table("iceberg.events").filter(f.col("date") >= "2024-01-01")

# Check with explain
df.explain(True)  # Look for "PushedFilters"
```

### Column Pruning
```python
# Good - only reads needed columns
df.select("id", "name").show()

# Bad - reads all columns then selects
df.collect()  # Don't do this
```

## Explain Plans

### View Execution Plan
```python
# Simple plan
df.explain()

# Extended with statistics
df.explain("cost")

# Full detail
df.explain(mode="extended")
```

```sql
EXPLAIN COST SELECT * FROM orders WHERE amount > 100;
```

### Key Things to Look For
- `Exchange` nodes (shuffles) - minimize these
- `BroadcastHashJoin` vs `SortMergeJoin`
- `PushedFilters` in scans
- Partition counts at each stage

## Common Configurations

### Development
```python
spark.conf.set("spark.sql.shuffle.partitions", "10")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.driver.memory", "4g")
```

### Production (Medium Cluster)
```python
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100m")
spark.conf.set("spark.sql.sources.v2.bucketing.enabled", "true")
```

### Large Scale
```python
spark.conf.set("spark.sql.shuffle.partitions", "1000")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256m")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "512m")
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.executor.memoryOverhead", "4g")
```

## Pipeline.yml Configuration (SDP)

For Spark Declarative Pipelines, set in `pipeline.yml`:

```yaml
name: optimized_pipeline
libraries:
  - file: pipeline.py
catalog: iceberg
configuration:
  spark.sql.adaptive.enabled: "true"
  spark.sql.adaptive.coalescePartitions.enabled: "true"
  spark.sql.adaptive.skewJoin.enabled: "true"
  spark.sql.shuffle.partitions: "400"
  spark.sql.autoBroadcastJoinThreshold: "100m"
  spark.sql.sources.v2.bucketing.enabled: "true"
  spark.executor.memory: "8g"
  spark.executor.memoryOverhead: "2g"
```

## Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `OutOfMemoryError: Java heap` | Driver/executor OOM | Increase memory, reduce partition size |
| `Container killed by YARN` | Memory exceeded | Increase `memoryOverhead` |
| Job stuck on one task | Data skew | Enable AQE skew join, salt keys |
| Broadcast timeout | Table too large | Increase threshold or timeout |
| `FileNotFoundException` during shuffle | Executor lost | Add retries, check disk space |

## See Also

- [PySpark.md](PySpark.md) - DataFrame operations
- [Spark-SQL.md](Spark-SQL.md) - SQL optimization hints
- [SDP.md](SDP.md) - Pipeline configuration
- [Spark-Kubernetes.md](Spark-Kubernetes.md) - K8s resource tuning
