# spark-skills

Claude Code documentation for Apache Spark.

> **Validated against Spark 4.1** | [Documentation](https://spark.apache.org/docs/latest/)

## Quick Start

Copy to your project root:

```bash
cp AGENTS.md your-project/AGENTS.md
```

The `AGENTS.md` file contains a compressed documentation index that's **always in context** - the agent doesn't need to decide to use it. Based on [Vercel Labs research](https://vercel.com/blog/agents-md-outperforms-skills-in-our-agent-evals), this approach achieves significantly better results than skills that require invocation.

## Architecture

| File | Purpose | When Used |
|------|---------|-----------|
| **AGENTS.md** | Compressed index (always loaded) | Every Spark-related task |
| **Skills (*.md)** | Detailed reference docs | Deep dives, complex patterns |

## Skills

| Skill | Focus |
|-------|-------|
| [SDP.md](SDP.md) | Declarative Pipelines - YAML-driven ETL |
| [PySpark.md](PySpark.md) | DataFrame API, transformations, actions |
| [Structured-Streaming.md](Structured-Streaming.md) | Real-time processing, watermarks, triggers |
| [Spark-SQL.md](Spark-SQL.md) | SQL patterns, window functions, CTEs |
| [Spark-Connect.md](Spark-Connect.md) | Client-server architecture, remote execution |
| [Spark-Kubernetes.md](Spark-Kubernetes.md) | K8s deployment, pod templates, RBAC |
| [Performance-Tuning.md](Performance-Tuning.md) | AQE, partitioning, joins, caching |

## Usage

### Recommended: AGENTS.md (Always-in-Context)

Copy the compressed index to your project root:

```bash
cp spark-skills/AGENTS.md your-project/AGENTS.md
```

### Optional: Full Skills (Deep Reference)

For detailed documentation, also copy to `.claude/skills/`:

```bash
cp -r spark-skills/ your-project/.claude/skills/spark/
```

Or add as a submodule:

```bash
git submodule add https://github.com/lisancao/spark-skills .claude/skills/spark
```

## Version Support

| Version | Status |
|---------|--------|
| Spark 4.1 | Primary |
| Spark 4.0 | Supported |

## Spark 4.1 Requirements

| Dependency | Minimum Version | Notes |
|------------|-----------------|-------|
| Python | 3.10+ | Dropped 3.9 support |
| JDK | 17+ | Dropped 8/11 support |
| Pandas | 2.2.0+ | Required for pandas API |
| NumPy | 1.22+ | |
| PyArrow | 15.0.0+ | Arrow-optimized UDFs |

### Breaking Changes in Spark 4.x

- **ANSI mode ON by default**: Division by zero, invalid casts raise errors
- **Use `try_divide()`, `try_cast()`** for safe operations
- **Pandas API changes**: `append()` removed, use `ps.concat()`

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
