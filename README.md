# spark-skills

Claude Code skill files for Apache Spark.

> **Validated against Spark 4.1** | [Documentation](https://spark.apache.org/docs/latest/)

## Skills

| Skill | Focus |
|-------|-------|
| [SDP.md](SDP.md) | Declarative Pipelines - YAML-driven ETL |
| [PySpark.md](PySpark.md) | DataFrame API, transformations, actions |
| [Structured-Streaming.md](Structured-Streaming.md) | Real-time processing, watermarks, triggers |
| [Spark-SQL.md](Spark-SQL.md) | SQL patterns, window functions, CTEs |

## Usage

Copy to your project's `.claude/skills/` directory:

```bash
cp -r spark-skills/ your-project/.claude/skills/spark/
```

Or add as a submodule:

```bash
git submodule add https://github.com/YOUR_ORG/spark-skills .claude/skills/spark
```

## Version Support

| Version | Status |
|---------|--------|
| Spark 4.1 | Primary |
| Spark 4.0 | Supported |

## Conventions

```python
# Standard imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.window import Window

# SparkSession pattern
spark = (SparkSession.builder
    .appName("job-name")
    .getOrCreate())

# DataFrame naming
df_raw = ...       # Prefix with df_
df_cleaned = ...   # Descriptive suffix

# Column references
f.col("column_name")  # Explicit over implicit
```

## Common Patterns

### Read → Transform → Write
```python
(spark.read.table("iceberg.bronze.events")
    .filter(f.col("event_date") >= "2024-01-01")
    .groupBy("event_type")
    .agg(f.count("*").alias("event_count"))
    .write
    .mode("overwrite")
    .saveAsTable("iceberg.gold.event_summary"))
```
