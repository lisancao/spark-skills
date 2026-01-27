# Spark Connect

> Client-server architecture for remote Spark execution.
>
> **Validated against Spark 4.1**

## When to Use This Skill

- Running Spark from remote applications, IDEs, or notebooks
- Building thin-client Spark applications
- Multi-tenant Spark deployments with process isolation
- Connecting BI tools to Spark clusters

## Spark 4.1 Requirements

| Requirement | Version |
|-------------|---------|
| Python | 3.10+ |
| pyspark (full) | 4.1.0+ |
| pyspark-connect (thin client) | 4.1.0+ |
| Scala | 2.13 |

## Quick Reference

| Task | Code |
|------|------|
| Start server | `./sbin/start-connect-server.sh` |
| Connect (Python) | `SparkSession.builder.remote("sc://host:15002").getOrCreate()` |
| Connect (env var) | `export SPARK_REMOTE="sc://localhost"` |
| Stop server | `./sbin/stop-connect-server.sh` |

## Architecture

```
┌─────────────────┐         gRPC/Protobuf        ┌─────────────────┐
│  Client App     │ ─────────────────────────────▶│  Spark Server   │
│  (Python/Scala) │                               │  (Driver + JVM) │
│                 │ ◀───────────────────────────── │                 │
│  Thin process   │      Arrow-encoded results    │  Full Spark     │
└─────────────────┘                               └─────────────────┘
```

**How it works:**
1. Client translates DataFrame operations into unresolved logical plans
2. Plans encoded as Protocol Buffers, sent via gRPC
3. Server executes using standard Spark engine
4. Results streamed back as Apache Arrow batches

## Server Setup

### Start Server
```bash
# Default: listens on localhost:15002
./sbin/start-connect-server.sh

# Custom port
./sbin/start-connect-server.sh --conf spark.connect.grpc.binding.port=15003

# With additional Spark config
./sbin/start-connect-server.sh \
    --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.5.0 \
    --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog
```

### Stop Server
```bash
./sbin/stop-connect-server.sh
```

### Server Configuration
```bash
# In spark-defaults.conf or via --conf flags
spark.connect.grpc.binding.port=15002
spark.connect.grpc.maxInboundMessageSize=134217728  # 128MB
```

## Python Client

### Installation

```bash
# Full PySpark (includes Connect client)
pip install pyspark==4.1.0

# OR thin client only (smaller footprint, no JVM required)
pip install pyspark-connect==4.1.0
```

### Connect to Server

```python
from pyspark.sql import SparkSession

# Method 1: Programmatic connection
spark = SparkSession.builder \
    .remote("sc://localhost:15002") \
    .appName("MyApp") \
    .getOrCreate()

# Method 2: Environment variable
# export SPARK_REMOTE="sc://localhost:15002"
spark = SparkSession.builder.getOrCreate()
```

### Connection String Format
```
sc://host:port/;param1=value1;param2=value2
```

Examples:
```python
# Basic
"sc://localhost:15002"

# With authentication token
"sc://spark.example.com:443/;token=ABCDEFG"

# With user_id
"sc://localhost:15002/;user_id=my_user"
```

### Basic Usage
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.remote("sc://localhost").getOrCreate()

# All standard DataFrame operations work
df = spark.read.table("iceberg.bronze.events")
result = (df
    .filter(f.col("event_type") == "click")
    .groupBy("user_id")
    .agg(f.count("*").alias("clicks"))
)
result.show()

# Write results
result.write.mode("overwrite").saveAsTable("iceberg.gold.user_clicks")

spark.stop()
```

### Using UDFs
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def normalize_email(email):
    return email.lower().strip() if email else None

df.withColumn("email_clean", normalize_email("email"))
```

## Scala Client

### Dependencies (build.sbt)
```scala
libraryDependencies += "org.apache.spark" %% "spark-connect-client-jvm" % "4.1.0"
```

### Connect and Use
```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
    .remote("sc://localhost:15002")
    .getOrCreate()

// Standard DataFrame operations
val df = spark.read.table("iceberg.bronze.events")
df.filter($"status" === "active").show()

spark.stop()
```

### Adding Dependencies at Runtime
```scala
// Upload JAR to server
spark.addArtifact("/path/to/my-library.jar")

// For UDFs with custom classes, register class finder
import org.apache.spark.sql.connect.client.REPLClassDirMonitor
val classFinder = new REPLClassDirMonitor("/path/to/classes")
spark.registerClassFinder(classFinder)
```

## Interactive Shell

### PySpark Shell
```bash
# Via environment variable
export SPARK_REMOTE="sc://localhost:15002"
./bin/pyspark

# Via command line
./bin/pyspark --remote "sc://localhost:15002"
```

### Spark Shell (Scala)
```bash
./bin/spark-shell --remote "sc://localhost:15002"
```

## Supported vs Unsupported APIs

### Supported
| API | Python | Scala |
|-----|--------|-------|
| DataFrame | ✅ | ✅ |
| Functions (f.*) | ✅ | ✅ |
| Column operations | ✅ | ✅ |
| UDFs | ✅ | ✅ |
| Streaming (DataStreamReader/Writer) | ✅ | ✅ |
| Catalog | ✅ | ✅ |
| ML (Spark 4.1+) | ✅ | ✅ |

### Not Supported
| API | Reason |
|-----|--------|
| SparkContext | Low-level, requires JVM access |
| RDD | Low-level, not part of logical plan protocol |
| `df._jdf` (Python) | No direct JVM access from client |
| Broadcast variables | Requires driver-side state |

## Operational Benefits

| Benefit | Description |
|---------|-------------|
| **Stability** | Client crashes don't affect Spark driver |
| **Isolation** | Each client runs in separate process |
| **Upgradability** | Upgrade driver without changing clients |
| **Debuggability** | Debug client code in your IDE |
| **Thin Client** | No JVM required on client machine |

## Streaming with Connect

```python
# Streaming works with Spark Connect
spark = SparkSession.builder.remote("sc://localhost").getOrCreate()

# Read stream
df_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "events")
    .load())

# Write stream
query = (df_stream
    .writeStream
    .format("iceberg")
    .option("checkpointLocation", "/checkpoints/events")
    .toTable("iceberg.bronze.events"))

query.awaitTermination()
```

## Error Handling

```python
from pyspark.errors import SparkConnectGrpcException

try:
    df = spark.read.table("nonexistent.table")
    df.show()
except SparkConnectGrpcException as e:
    print(f"Spark Connect error: {e}")
    # Access gRPC status code (Spark 4.1+)
    print(f"Status code: {e.getStatusCode()}")
```

## Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `Connection refused` | Server not running | Start with `start-connect-server.sh` |
| `UNAVAILABLE: Network closed` | Server stopped or network issue | Check server status, network connectivity |
| `Method not found` | API not supported in Connect | Use supported DataFrame APIs only |
| `RESOURCE_EXHAUSTED` | Message too large | Increase `maxInboundMessageSize` |

## Security

Spark Connect itself doesn't include authentication. Use:

1. **Authenticating Proxy**: Place server behind auth proxy (OAuth, mTLS)
2. **Network Security**: VPN, private networks, firewalls
3. **Token Parameter**: Pass tokens in connection string for custom auth

```python
# Custom token authentication
spark = SparkSession.builder \
    .remote("sc://spark.internal:15002/;token=my_secret_token") \
    .getOrCreate()
```

## Deployment Patterns

### Single Server
```
Clients ──▶ Spark Connect Server ──▶ Spark Cluster
```

### Load-Balanced (Multi-tenant)
```
                    ┌─▶ Connect Server 1 ──▶ Cluster 1
Clients ──▶ LB ─────┼─▶ Connect Server 2 ──▶ Cluster 2
                    └─▶ Connect Server 3 ──▶ Cluster 3
```

### With Kubernetes
```yaml
# Example: Expose Connect server via LoadBalancer
apiVersion: v1
kind: Service
metadata:
  name: spark-connect
spec:
  type: LoadBalancer
  ports:
    - port: 15002
      targetPort: 15002
  selector:
    app: spark-connect-server
```

## When NOT to Use Spark Connect

| Scenario | Use Instead |
|----------|-------------|
| Need RDD operations | Classic spark-submit |
| Need SparkContext | Classic spark-submit |
| Low-latency single queries | Spark Thrift Server (JDBC) |
| Legacy Spark 3.3 or earlier | Classic spark-submit |

## See Also

- [PySpark.md](PySpark.md) - DataFrame operations
- [Structured-Streaming.md](Structured-Streaming.md) - Streaming with Connect
- [Spark-Kubernetes.md](Spark-Kubernetes.md) - Deploying Connect server on K8s
