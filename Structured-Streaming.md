# Structured Streaming

> Real-time stream processing with Spark's DataFrame API.
>
> **Validated against Spark 4.1**

## When to Use This Skill

- Processing Kafka streams
- Real-time data ingestion to Iceberg
- Windowed aggregations on streaming data
- Handling late-arriving data

## Spark 4.1 Requirements

| Requirement | Version |
|-------------|---------|
| Python | 3.10+ (dropped 3.9) |
| JDK | 17+ (dropped 8/11) |
| Pandas | 2.2.0+ |
| PyArrow | 15.0.0+ |

## Quick Reference

| Task | Code |
|------|------|
| Read from Kafka | `spark.readStream.format("kafka").option("subscribe", "topic")` |
| Read from files | `spark.readStream.format("parquet").schema(schema).load(path)` |
| Write to Iceberg | `.writeStream.format("iceberg").toTable("table")` |
| Set watermark | `.withWatermark("event_time", "10 minutes")` |
| Tumbling window | `f.window("event_time", "5 minutes")` |
| Session window | `f.session_window("event_time", "10 minutes")` |
| Custom sink | `.writeStream.foreachBatch(process_batch)` |
| Test source | `spark.readStream.format("rate").load()` |

## Quick Start

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("streaming").getOrCreate()

# Read from Kafka
df_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "events")
    .option("startingOffsets", "latest")
    .load())

# Transform
df_parsed = (df_stream
    .selectExpr("CAST(value AS STRING) as json")
    .select(f.from_json("json", schema).alias("data"))
    .select("data.*"))

# Write to Iceberg
query = (df_parsed.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/events")
    .toTable("iceberg.bronze.events"))

query.awaitTermination()
```

## Reading Streams

### From Kafka
```python
df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "topic1,topic2")  # Multiple topics
    .option("startingOffsets", "earliest")  # or "latest"
    .option("maxOffsetsPerTrigger", 10000)  # Rate limiting
    .load())

# Kafka columns: key, value, topic, partition, offset, timestamp
df.selectExpr(
    "CAST(key AS STRING)",
    "CAST(value AS STRING)",
    "topic",
    "timestamp"
)
```

### From Files (Auto-ingest)
```python
df = (spark.readStream
    .format("parquet")  # or csv, json
    .schema(my_schema)  # Required for streaming
    .option("maxFilesPerTrigger", 100)
    .load("s3a://bucket/incoming/"))
```

## Writing Streams

### To Iceberg
```python
query = (df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/my-stream")
    .option("fanout-enabled", "true")  # For partitioned tables
    .toTable("iceberg.bronze.events"))
```

### To Kafka
```python
query = (df
    .selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "output-topic")
    .option("checkpointLocation", "/checkpoints/kafka-sink")
    .start())
```

### To Console (Debug)
```python
query = (df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start())
```

### foreachBatch (Custom Sinks)

Use `foreachBatch` for JDBC, REST APIs, or any custom destination:

```python
def write_to_postgres(batch_df, batch_id):
    """Write each micro-batch to PostgreSQL."""
    (batch_df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/mydb")
        .option("dbtable", "events")
        .option("user", "user")
        .option("password", "pass")
        .mode("append")
        .save())

query = (df.writeStream
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/checkpoints/postgres-sink")
    .start())
```

```python
# Multiple outputs per batch
def multi_sink(batch_df, batch_id):
    # Write to Iceberg
    batch_df.write.mode("append").saveAsTable("iceberg.silver.events")

    # Write aggregates to dashboard
    agg_df = batch_df.groupBy("event_type").count()
    agg_df.write.mode("overwrite").saveAsTable("iceberg.gold.event_counts")

query = (df.writeStream
    .foreachBatch(multi_sink)
    .option("checkpointLocation", "/checkpoints/multi-sink")
    .start())
```

```python
# REST API sink
import requests

def send_to_api(batch_df, batch_id):
    records = batch_df.toJSON().collect()
    for record in records:
        requests.post("https://api.example.com/events", data=record)

# Note: collect() brings data to driver - use for small batches only
```

## Testing Sources

### Rate Source (Generate Test Data)
```python
# Generates rows with timestamp and value columns
df = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 100)      # 100 rows/second
    .option("numPartitions", 4)
    .load())

# Schema: timestamp (Timestamp), value (Long)
```

### Memory Sink (Unit Testing)
```python
# Write to in-memory table for testing
query = (df.writeStream
    .format("memory")
    .queryName("test_output")          # Table name to query
    .outputMode("append")
    .start())

# Query results
spark.sql("SELECT * FROM test_output").show()
```

### Socket Source (Development)
```python
# Read from TCP socket (netcat: nc -lk 9999)
df = (spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load())
```

## Kafka Production Configuration

### SSL/TLS Configuration
```python
df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9093,broker2:9093")
    .option("subscribe", "events")
    .option("kafka.security.protocol", "SSL")
    .option("kafka.ssl.truststore.location", "/path/to/truststore.jks")
    .option("kafka.ssl.truststore.password", "password")
    .option("kafka.ssl.keystore.location", "/path/to/keystore.jks")
    .option("kafka.ssl.keystore.password", "password")
    .load())
```

### SASL Authentication
```python
df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "events")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        "username='user' password='pass';")
    .load())
```

### Consumer Group Management
```python
df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "events")
    .option("kafka.group.id", "my-spark-consumer-group")  # Set consumer group
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 10000)                # Rate limiting
    .option("minOffsetsPerTrigger", 1000)                 # Min batch size
    .load())
```

### Schema Registry Integration
```python
# Using Confluent Schema Registry with Avro
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql.avro.functions import from_avro

# Get schema from registry
schema_registry = SchemaRegistryClient({"url": "http://schema-registry:8081"})
schema = schema_registry.get_latest_version("events-value").schema.schema_str

# Decode Avro messages
df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "events")
    .load()
    .select(from_avro("value", schema).alias("data"))
    .select("data.*"))
```

## Triggers

```python
# Default: process as fast as possible
.trigger(availableNow=True)  # Process all available, then stop

# Fixed interval micro-batches
.trigger(processingTime="10 seconds")

# Once (batch-like, process all available)
.trigger(once=True)  # Deprecated, use availableNow

# Continuous (experimental, low latency)
.trigger(continuous="1 second")
```

## Windowed Aggregations

### Tumbling Window
```python
# Non-overlapping fixed windows
df.groupBy(
    f.window("event_time", "5 minutes"),
    "event_type"
).agg(f.count("*").alias("count"))
```

### Sliding Window
```python
# Overlapping windows
df.groupBy(
    f.window("event_time", "10 minutes", "5 minutes"),  # size, slide
    "event_type"
).agg(f.count("*").alias("count"))
```

### Session Window
```python
# Gap-based windows
df.groupBy(
    f.session_window("event_time", "10 minutes"),  # gap duration
    "user_id"
).agg(f.count("*").alias("events_in_session"))
```

## Watermarks (Late Data)

```python
# Allow data up to 10 minutes late
df_with_watermark = (df
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        f.window("event_time", "5 minutes"),
        "event_type"
    )
    .agg(f.count("*").alias("count")))
```

## Output Modes

| Mode | Use Case | Aggregations |
|------|----------|--------------|
| `append` | New rows only | Only with watermark |
| `update` | Changed rows | Yes |
| `complete` | Full result | Yes (unbounded) |

```python
.outputMode("append")   # Most common for streaming to storage
.outputMode("update")   # For updating dashboards
.outputMode("complete") # For small, bounded aggregations
```

## Stateful Operations

### Deduplication
```python
# Dedupe within watermark window
df.withWatermark("event_time", "10 minutes").dropDuplicates(["event_id"])
```

### Stream-Stream Joins
```python
# Join two streams with watermarks
df_orders = orders.withWatermark("order_time", "10 minutes")
df_payments = payments.withWatermark("payment_time", "10 minutes")

df_joined = df_orders.join(
    df_payments,
    expr("""
        order_id = payment_order_id AND
        payment_time >= order_time AND
        payment_time <= order_time + interval 1 hour
    """),
    "leftOuter"
)
```

### Stream-Static Joins
```python
# Join stream with static dimension table
df_stream.join(df_static_dimension, "key", "left")
```

## Monitoring

```python
# Get current status
query.status

# Get progress metrics
query.lastProgress

# Get all recent progress
query.recentProgress

# Check if active
query.isActive

# Stop gracefully
query.stop()
```

## Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `StreamingQueryException: Data missing` | Checkpoint corrupted | Delete checkpoint dir, restart from earliest |
| `Append output mode not supported` | Aggregation without watermark | Add `.withWatermark()` |
| `Multiple streaming aggregations` | Chained aggregations | Use single aggregation or restructure |
| `Query terminated with error` | Schema mismatch | Check source schema matches expected |
| `Queries with streaming sources must be executed with writeStream.start()` | Using `.write()` instead of `.writeStream` | Change to `.writeStream.start()` |
| `No output mode defined` | Missing outputMode | Add `.outputMode("append")` |
| `Cannot convert expression to SQL` | Dynamic column in agg | Use literal column names in aggregations |
| `Kafka topic not found` | Topic doesn't exist | Create topic or check `subscribe` option |
| `foreachBatch function not serializable` | Closure captures non-serializable object | Move imports inside function, avoid closures |

## Checkpoint Management

```python
# Checkpoints contain:
# - Offsets (Kafka positions)
# - State (aggregations, dedup)
# - Metadata

# Location must be fault-tolerant storage
.option("checkpointLocation", "s3a://bucket/checkpoints/stream-name")

# Never share checkpoints between queries
# Never delete checkpoints while query is running
```

## See Also

- [PySpark.md](PySpark.md) - DataFrame transformations
- [../streaming/Kafka.md](../streaming/Kafka.md) - Kafka configuration
- [../table-formats/Iceberg.md](../table-formats/Iceberg.md) - Iceberg sink options
