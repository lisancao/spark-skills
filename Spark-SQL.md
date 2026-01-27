# Spark SQL

> SQL interface for Spark data processing.
>
> **Validated against Spark 4.1**

## When to Use This Skill

- Writing SQL queries in Spark
- Creating views and tables
- Complex analytical queries
- Migrating SQL from other engines

## Spark 4.1 Requirements

| Requirement | Version |
|-------------|---------|
| Python | 3.10+ (dropped 3.9) |
| JDK | 17+ (dropped 8/11) |
| ANSI Mode | ON by default |

## Quick Reference

| Task | SQL |
|------|-----|
| Filter rows | `WHERE status = 'active'` |
| Group & count | `GROUP BY category HAVING COUNT(*) > 5` |
| Window ranking | `ROW_NUMBER() OVER (PARTITION BY x ORDER BY y)` |
| Filter on window | `QUALIFY ROW_NUMBER() OVER (...) = 1` |
| CTE | `WITH cte AS (SELECT ...) SELECT * FROM cte` |
| Pivot | `PIVOT (SUM(x) FOR col IN ('a', 'b'))` |
| Merge/upsert | `MERGE INTO target USING source ON ...` |
| Time travel | `SELECT * FROM table TIMESTAMP AS OF '...'` |

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

### Ranking Functions
```sql
SELECT
    customer_id,
    amount,
    -- ROW_NUMBER: 1, 2, 3, 4, 5 (no gaps, no ties)
    ROW_NUMBER() OVER (ORDER BY amount DESC) as row_num,
    -- RANK: 1, 2, 2, 4, 5 (gaps after ties)
    RANK() OVER (ORDER BY amount DESC) as rank,
    -- DENSE_RANK: 1, 2, 2, 3, 4 (no gaps)
    DENSE_RANK() OVER (ORDER BY amount DESC) as dense_rank,
    -- NTILE: Divide into N buckets (quartiles, deciles)
    NTILE(4) OVER (ORDER BY amount DESC) as quartile,
    -- PERCENT_RANK: (rank - 1) / (total - 1), range 0-1
    PERCENT_RANK() OVER (ORDER BY amount DESC) as pct_rank
FROM orders;
```

### Window Frames
```sql
SELECT
    order_date,
    amount,
    -- Rolling 3-row average
    AVG(amount) OVER (
        ORDER BY order_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as rolling_avg_3,
    -- Sum from start to current row
    SUM(amount) OVER (
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_sum,
    -- Range-based: all rows within 7 days
    AVG(amount) OVER (
        ORDER BY order_date
        RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW
    ) as weekly_avg
FROM orders;
```

### QUALIFY Clause (Filter on Window Functions)
```sql
-- Get top 3 orders per customer (without subquery!)
SELECT customer_id, order_date, amount
FROM orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) <= 3;

-- Get latest order per customer
SELECT *
FROM orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) = 1;

-- Filter by rank
SELECT customer_id, category, total_spent
FROM customer_spending
QUALIFY RANK() OVER (PARTITION BY category ORDER BY total_spent DESC) <= 10;
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

### Recursive CTEs (Spark 4.1+)
```sql
-- Traverse hierarchical data (org charts, graphs, trees)
WITH RECURSIVE org_hierarchy AS (
    -- Base case: top-level managers
    SELECT id, name, manager_id, 1 as level
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive case: employees under each manager
    SELECT e.id, e.name, e.manager_id, h.level + 1
    FROM employees e
    JOIN org_hierarchy h ON e.manager_id = h.id
)
SELECT * FROM org_hierarchy ORDER BY level, name;

-- Generate number sequence
WITH RECURSIVE numbers AS (
    SELECT 1 as n
    UNION ALL
    SELECT n + 1 FROM numbers WHERE n < 100
)
SELECT * FROM numbers;

-- Bill of materials / parts explosion
WITH RECURSIVE parts AS (
    SELECT part_id, parent_id, quantity, 1 as depth
    FROM bill_of_materials
    WHERE parent_id IS NULL

    UNION ALL

    SELECT b.part_id, b.parent_id, b.quantity * p.quantity, p.depth + 1
    FROM bill_of_materials b
    JOIN parts p ON b.parent_id = p.part_id
    WHERE p.depth < 10  -- Prevent infinite recursion
)
SELECT * FROM parts;
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

## Array Operations

```sql
-- Create arrays
SELECT ARRAY(1, 2, 3) as nums;
SELECT ARRAY('a', 'b', 'c') as letters;

-- Access elements (1-indexed in SQL, 0-indexed with element_at)
SELECT arr[1] as first_element;                    -- 1-indexed
SELECT element_at(arr, 1) as first_element;        -- 1-indexed

-- Array functions
SELECT
    array_size(arr) as size,
    array_contains(arr, 'x') as has_x,
    array_distinct(arr) as unique_vals,
    array_sort(arr) as sorted,
    array_join(arr, ',') as joined,
    array_max(nums) as max_val,
    array_min(nums) as min_val
FROM table;

-- Explode array to rows
SELECT id, exploded_value
FROM table
LATERAL VIEW explode(tags) t AS exploded_value;

-- Or using CROSS JOIN LATERAL
SELECT id, tag
FROM table
CROSS JOIN LATERAL explode(tags) AS t(tag);

-- Transform array elements
SELECT transform(nums, x -> x * 2) as doubled;

-- Filter array elements
SELECT filter(nums, x -> x > 0) as positive;

-- Aggregate array elements
SELECT aggregate(nums, 0, (acc, x) -> acc + x) as sum;
```

## Map Operations

```sql
-- Create maps
SELECT MAP('a', 1, 'b', 2) as my_map;
SELECT MAP_FROM_ENTRIES(ARRAY(('a', 1), ('b', 2))) as my_map;

-- Access values
SELECT my_map['key'] as value;
SELECT element_at(my_map, 'key') as value;

-- Map functions
SELECT
    map_keys(my_map) as keys,
    map_values(my_map) as values,
    map_entries(my_map) as entries,
    size(my_map) as count
FROM table;

-- Explode map to rows
SELECT id, key, value
FROM table
LATERAL VIEW explode(my_map) t AS key, value;
```

## Struct Operations

```sql
-- Create structs
SELECT STRUCT(1 as id, 'Alice' as name) as person;
SELECT NAMED_STRUCT('id', 1, 'name', 'Alice') as person;

-- Access fields
SELECT person.id, person.name FROM table;
SELECT person.* FROM table;  -- Expand all fields

-- JSON to struct
SELECT from_json(json_col, 'id INT, name STRING') as parsed;

-- Struct to JSON
SELECT to_json(person) as json_string;

-- Get JSON field (without parsing entire struct)
SELECT get_json_object(json_col, '$.name') as name;
SELECT json_col:name as name;  -- Spark 3.0+ shorthand
```

## Set Operations

```sql
-- UNION: Combine and deduplicate
SELECT id, name FROM table1
UNION
SELECT id, name FROM table2;

-- UNION ALL: Combine keeping duplicates (faster)
SELECT id, name FROM table1
UNION ALL
SELECT id, name FROM table2;

-- INTERSECT: Rows in both
SELECT id FROM table1
INTERSECT
SELECT id FROM table2;

-- EXCEPT: Rows in first but not second
SELECT id FROM table1
EXCEPT
SELECT id FROM table2;

-- EXCEPT ALL: Keep duplicates
SELECT id FROM table1
EXCEPT ALL
SELECT id FROM table2;
```

## VARIANT Type (Spark 4.1+)

Semi-structured data type for JSON-like data with automatic schema inference:

```sql
-- Create table with VARIANT column
CREATE TABLE events (
    id BIGINT,
    event_data VARIANT
) USING iceberg;

-- Insert JSON data
INSERT INTO events VALUES
    (1, PARSE_JSON('{"user": "alice", "action": "click", "meta": {"page": "/home"}}')),
    (2, PARSE_JSON('{"user": "bob", "action": "purchase", "amount": 99.99}'));

-- Query with colon syntax (field access)
SELECT
    id,
    event_data:user::STRING as user,           -- Extract as string
    event_data:action::STRING as action,
    event_data:meta:page::STRING as page,      -- Nested access
    event_data:amount::DOUBLE as amount
FROM events;

-- Filter on VARIANT fields
SELECT * FROM events
WHERE event_data:action::STRING = 'purchase';

-- Check if field exists
SELECT * FROM events
WHERE event_data:amount IS NOT NULL;
```

### Shredding (Performance Optimization)
```sql
-- Enable shredding for frequently accessed fields
-- Stores common fields as typed Parquet columns for faster reads
-- Trade-off: 20-50% slower writes, much faster reads

ALTER TABLE events SET TBLPROPERTIES (
    'write.parquet.variant.shredding.enabled' = 'true'
);
```

## SQL Scripting (Spark 4.1+ GA)

Procedural SQL with variables, control flow, and error handling:

```sql
-- Declare variables
DECLARE total_count INT DEFAULT 0;
DECLARE batch_size INT = 1000;

-- Set variable from query
SET VAR total_count = (SELECT COUNT(*) FROM orders);

-- Conditional logic
IF total_count > 10000 THEN
    INSERT INTO large_orders_log VALUES (current_timestamp(), total_count);
ELSEIF total_count > 1000 THEN
    INSERT INTO medium_orders_log VALUES (current_timestamp(), total_count);
ELSE
    INSERT INTO small_orders_log VALUES (current_timestamp(), total_count);
END IF;

-- Loop
WHILE batch_size > 0 DO
    -- Process batch
    SET VAR batch_size = batch_size - 100;
END WHILE;

-- Error handling with CONTINUE HANDLER
DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
BEGIN
    INSERT INTO error_log VALUES (current_timestamp(), 'Error occurred');
END;
```

## Approximate Aggregations (Spark 4.1+)

Efficient sketches for large-scale analytics:

```sql
-- Top-K frequent items (faster than GROUP BY + ORDER BY + LIMIT)
SELECT approx_top_k(product_id, 10) as top_products
FROM orders;

-- Combine top-k results from different sources
SELECT approx_top_k_combine(sketch_col, 10) as combined_top
FROM aggregated_sketches;

-- KLL Quantiles (approximate percentiles)
SELECT
    approx_percentile(amount, 0.5) as median,      -- Existing
    approx_percentile(amount, ARRAY(0.25, 0.5, 0.75)) as quartiles
FROM orders;

-- Theta Sketch for set operations (approximate distinct)
-- Useful for: unique visitors, distinct counts at scale
SELECT
    theta_sketch_distinct(user_id) as unique_users,
    theta_sketch_union(sketch_a, sketch_b) as combined_users
FROM user_events;
```

## ANSI Mode (Spark 4.x Default)

Spark 4.x enables ANSI SQL mode by default, which enforces stricter behavior:

```sql
-- Division by zero: RAISES ERROR
SELECT 1 / 0;  -- Error!

-- FIX: Use try_divide for safe division
SELECT try_divide(1, 0);  -- Returns NULL

-- Invalid casts: RAISE ERROR
SELECT CAST('abc' AS INT);  -- Error!

-- FIX: Use try_cast for safe conversion
SELECT try_cast('abc' AS INT);  -- Returns NULL

-- Integer overflow: RAISES ERROR
SELECT 2147483647 + 1;  -- Error!

-- FIX: Use BIGINT or handle explicitly
SELECT CAST(2147483647 AS BIGINT) + 1;

-- try_to_date for safe date parsing
SELECT try_to_date('2024-13-45', 'yyyy-MM-dd');  -- Returns NULL
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
| `DIVIDE_BY_ZERO` | ANSI mode division by zero | Use `try_divide()` or add `WHERE x != 0` |
| `CAST_INVALID_INPUT` | ANSI mode invalid cast | Use `try_cast()` for safe conversion |
| `NUMERIC_VALUE_OUT_OF_RANGE` | ANSI mode integer overflow | Use BIGINT or DECIMAL types |
| `Invalid call to QUALIFY` | QUALIFY without window function | QUALIFY requires window function in SELECT |

## See Also

- [PySpark.md](PySpark.md) - DataFrame equivalent operations
- [../table-formats/Iceberg.md](../table-formats/Iceberg.md) - Iceberg DDL and maintenance
