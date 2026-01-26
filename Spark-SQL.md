# Spark SQL

> SQL interface for Spark data processing.
>
> **Validated against Spark 4.1**

## When to Use This Skill

- Writing SQL queries in Spark
- Creating views and tables
- Complex analytical queries
- Migrating SQL from other engines

## Quick Start

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("spark-sql").getOrCreate()

# Register DataFrame as temp view
df.createOrReplaceTempView("orders")

# Run SQL
result = spark.sql("""
    SELECT
        customer_id,
        COUNT(*) as order_count,
        SUM(amount) as total_spent
    FROM orders
    WHERE status = 'completed'
    GROUP BY customer_id
    HAVING COUNT(*) > 5
    ORDER BY total_spent DESC
""")

result.show()
```

## Table Operations

### Create Tables
```sql
-- Managed Iceberg table
CREATE TABLE iceberg.bronze.events (
    event_id STRING,
    event_type STRING,
    event_time TIMESTAMP,
    payload STRING
)
USING iceberg
PARTITIONED BY (days(event_time));

-- Create from query
CREATE TABLE iceberg.gold.summary AS
SELECT * FROM iceberg.silver.events WHERE event_date = current_date();

-- Create or replace
CREATE OR REPLACE TABLE iceberg.gold.summary AS
SELECT * FROM source_table;
```

### Insert Data
```sql
-- Append
INSERT INTO iceberg.bronze.events
SELECT * FROM staging_events;

-- Overwrite entire table
INSERT OVERWRITE iceberg.gold.summary
SELECT * FROM aggregated_data;

-- Overwrite specific partitions
INSERT OVERWRITE iceberg.bronze.events
PARTITION (event_date = '2024-01-15')
SELECT * FROM daily_events;
```

### Merge (Upsert)
```sql
MERGE INTO iceberg.silver.customers AS target
USING staging_customers AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
    UPDATE SET
        name = source.name,
        email = source.email,
        updated_at = current_timestamp()
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, created_at, updated_at)
    VALUES (source.customer_id, source.name, source.email,
            current_timestamp(), current_timestamp());
```

## Common Query Patterns

### Window Functions
```sql
SELECT
    customer_id,
    order_date,
    amount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) as order_num,
    SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date) as running_total,
    LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_amount,
    LEAD(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) as next_amount,
    AVG(amount) OVER (PARTITION BY customer_id) as avg_amount
FROM orders;
```

### Common Table Expressions (CTEs)
```sql
WITH daily_totals AS (
    SELECT
        DATE(order_time) as order_date,
        SUM(amount) as daily_total
    FROM orders
    GROUP BY DATE(order_time)
),
weekly_avg AS (
    SELECT AVG(daily_total) as avg_daily
    FROM daily_totals
)
SELECT
    d.order_date,
    d.daily_total,
    w.avg_daily,
    d.daily_total - w.avg_daily as variance
FROM daily_totals d
CROSS JOIN weekly_avg w
ORDER BY d.order_date;
```

### Pivoting
```sql
SELECT *
FROM (
    SELECT customer_id, category, amount
    FROM orders
)
PIVOT (
    SUM(amount)
    FOR category IN ('electronics', 'clothing', 'food')
);
```

### Unpivoting
```sql
SELECT customer_id, category, amount
FROM sales
UNPIVOT (
    amount FOR category IN (electronics, clothing, food)
);
```

## Date/Time Functions

```sql
-- Current date/time
SELECT current_date(), current_timestamp();

-- Formatting
SELECT date_format(event_time, 'yyyy-MM-dd HH:mm:ss');

-- Extraction
SELECT
    year(event_date),
    month(event_date),
    day(event_date),
    dayofweek(event_date),
    quarter(event_date);

-- Arithmetic
SELECT
    date_add(event_date, 7),
    date_sub(event_date, 7),
    datediff(end_date, start_date),
    months_between(end_date, start_date);

-- Truncation
SELECT
    date_trunc('month', event_time),
    date_trunc('week', event_time),
    date_trunc('hour', event_time);
```

## String Functions

```sql
SELECT
    lower(name),
    upper(name),
    trim(name),
    concat(first_name, ' ', last_name),
    concat_ws('-', col1, col2, col3),
    substring(code, 1, 3),
    split(tags, ','),
    regexp_replace(phone, '[^0-9]', ''),
    regexp_extract(email, '@(.+)', 1);
```

## Conditional Logic

```sql
-- CASE expression
SELECT
    order_id,
    CASE
        WHEN amount >= 1000 THEN 'high'
        WHEN amount >= 100 THEN 'medium'
        ELSE 'low'
    END as tier
FROM orders;

-- COALESCE (first non-null)
SELECT COALESCE(preferred_name, first_name, 'Unknown') as display_name;

-- NULLIF (return null if equal)
SELECT NULLIF(status, 'N/A') as status;

-- IF function
SELECT IF(amount > 100, 'large', 'small') as size;
```

## Joins

```sql
-- Inner join
SELECT o.*, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.id;

-- Left join
SELECT o.*, c.name
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id;

-- Multiple conditions
SELECT *
FROM orders o
JOIN inventory i
    ON o.product_id = i.product_id
    AND o.warehouse_id = i.warehouse_id;

-- Self join
SELECT
    e.name as employee,
    m.name as manager
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id;
```

## Aggregations

```sql
SELECT
    category,
    COUNT(*) as count,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(amount) as total,
    AVG(amount) as average,
    MIN(amount) as minimum,
    MAX(amount) as maximum,
    PERCENTILE_APPROX(amount, 0.5) as median,
    STDDEV(amount) as std_dev,
    COLLECT_LIST(product_id) as products,
    COLLECT_SET(product_id) as unique_products
FROM orders
GROUP BY category;
```

## Iceberg-Specific

### Time Travel
```sql
-- By timestamp
SELECT * FROM iceberg.bronze.events TIMESTAMP AS OF '2024-01-15 10:00:00';

-- By snapshot ID
SELECT * FROM iceberg.bronze.events VERSION AS OF 123456789;
```

### Metadata Queries
```sql
-- View snapshots
SELECT * FROM iceberg.bronze.events.snapshots;

-- View history
SELECT * FROM iceberg.bronze.events.history;

-- View files
SELECT * FROM iceberg.bronze.events.files;

-- View partitions
SELECT * FROM iceberg.bronze.events.partitions;
```

### Maintenance
```sql
-- Expire old snapshots
CALL iceberg.system.expire_snapshots('iceberg.bronze.events', TIMESTAMP '2024-01-01 00:00:00');

-- Remove orphan files
CALL iceberg.system.remove_orphan_files('iceberg.bronze.events');

-- Rewrite data files (compaction)
CALL iceberg.system.rewrite_data_files('iceberg.bronze.events');
```

## Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `Table or view not found` | Wrong catalog/namespace | Check `SHOW DATABASES`, `SHOW TABLES` |
| `Column cannot be resolved` | Ambiguous column in join | Use table alias: `t.column` |
| `Expression not in GROUP BY` | Select non-aggregated column | Add to GROUP BY or wrap in aggregate |
| `Data type mismatch` | Comparing incompatible types | Use CAST: `CAST(col AS STRING)` |

## See Also

- [PySpark.md](PySpark.md) - DataFrame equivalent operations
- [../table-formats/Iceberg.md](../table-formats/Iceberg.md) - Iceberg DDL and maintenance
