# Spark on Kubernetes

> Running Spark workloads on Kubernetes clusters.
>
> **Validated against Spark 4.1**

## When to Use This Skill

- Deploying Spark jobs to Kubernetes clusters
- Running Spark Declarative Pipelines (SDP) in containerized environments
- Auto-scaling Spark executors with K8s
- Integrating Spark with cloud-native infrastructure

## Spark 4.1 Requirements

| Requirement | Version |
|-------------|---------|
| Kubernetes | 1.30+ |
| Spark | 4.1.0+ |
| kubectl | Configured with cluster access |

## Quick Reference

| Task | Command/Config |
|------|----------------|
| Submit job | `spark-submit --master k8s://https://host:port --deploy-mode cluster` |
| Set image | `--conf spark.kubernetes.container.image=image:tag` |
| Driver resources | `--conf spark.driver.memory=4g` |
| Executor count | `--conf spark.executor.instances=5` |
| Kill job | `spark-submit --kill spark:driver-pod-name --master k8s://...` |
| View logs | `kubectl logs -f <driver-pod>` |

## Basic Submission

### Cluster Mode (Recommended)
```bash
./bin/spark-submit \
    --master k8s://https://$(kubectl cluster-info | grep -oP 'https://\S+') \
    --deploy-mode cluster \
    --name my-spark-job \
    --conf spark.kubernetes.container.image=apache/spark:4.1.0 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.executor.instances=3 \
    --conf spark.driver.memory=2g \
    --conf spark.executor.memory=4g \
    local:///opt/spark/examples/jars/spark-examples_2.13-4.1.0.jar
```

### Client Mode
```bash
# Driver runs locally, executors on K8s
./bin/spark-submit \
    --master k8s://https://k8s-apiserver:6443 \
    --deploy-mode client \
    --conf spark.kubernetes.container.image=apache/spark:4.1.0 \
    --conf spark.driver.host=$(hostname -i) \
    local:///path/to/app.jar
```

## Running SDP Pipelines on Kubernetes

### Option 1: spark-pipelines CLI in Container
```dockerfile
FROM apache/spark:4.1.0-python3

COPY pipeline.py /app/
COPY pipeline.yml /app/

WORKDIR /app
CMD ["spark-pipelines", "run", "--spec", "pipeline.yml"]
```

```bash
# Build and push
docker build -t myrepo/sdp-pipeline:v1 .
docker push myrepo/sdp-pipeline:v1

# Run as Kubernetes Job
kubectl apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: sdp-pipeline
spec:
  template:
    spec:
      containers:
      - name: spark
        image: myrepo/sdp-pipeline:v1
        env:
        - name: SPARK_MASTER
          value: "k8s://https://kubernetes.default:443"
      restartPolicy: Never
      serviceAccountName: spark
EOF
```

### Option 2: Submit Pipeline via spark-submit
```bash
spark-submit \
    --master k8s://https://k8s-apiserver:6443 \
    --deploy-mode cluster \
    --conf spark.kubernetes.container.image=apache/spark:4.1.0-python3 \
    --conf spark.kubernetes.file.upload.path=s3a://bucket/spark-uploads \
    --py-files pipeline.py \
    /path/to/run_pipeline.py
```

## Resource Configuration

### Driver Resources
```bash
--conf spark.driver.cores=2
--conf spark.driver.memory=4g
--conf spark.driver.memoryOverhead=1g

# Kubernetes-specific limits
--conf spark.kubernetes.driver.request.cores=1
--conf spark.kubernetes.driver.limit.cores=4
```

### Executor Resources
```bash
--conf spark.executor.instances=5
--conf spark.executor.cores=2
--conf spark.executor.memory=8g
--conf spark.executor.memoryOverhead=2g

# Kubernetes-specific
--conf spark.kubernetes.executor.request.cores=2
--conf spark.kubernetes.executor.limit.cores=4
```

### Dynamic Allocation
```bash
--conf spark.dynamicAllocation.enabled=true
--conf spark.dynamicAllocation.minExecutors=2
--conf spark.dynamicAllocation.maxExecutors=20
--conf spark.dynamicAllocation.executorIdleTimeout=60s
--conf spark.dynamicAllocation.shuffleTracking.enabled=true
```

### GPU Resources
```bash
--conf spark.executor.resource.gpu.amount=1
--conf spark.executor.resource.gpu.vendor=nvidia.com
--conf spark.executor.resource.gpu.discoveryScript=/opt/spark/scripts/getGpusResources.sh
```

## Docker Images

### Official Images
```bash
# Base Spark
apache/spark:4.1.0

# With Python
apache/spark:4.1.0-python3

# With R
apache/spark:4.1.0-r
```

### Building Custom Images
```bash
# From Spark distribution
./bin/docker-image-tool.sh \
    -r myrepo \
    -t 4.1.0-custom \
    -p ./kubernetes/dockerfiles/spark/bindings/python/Dockerfile \
    build

./bin/docker-image-tool.sh -r myrepo -t 4.1.0-custom push
```

### Custom Dockerfile
```dockerfile
FROM apache/spark:4.1.0-python3

# Install Python dependencies
COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

# Add application code
COPY pipeline.py /opt/spark/work-dir/
COPY pipeline.yml /opt/spark/work-dir/

# Add JARs (Iceberg, etc.)
COPY jars/*.jar /opt/spark/jars/

USER spark
WORKDIR /opt/spark/work-dir
```

## Pod Templates

For configurations not covered by Spark options:

### Driver Pod Template
```yaml
# driver-template.yaml
apiVersion: v1
kind: Pod
metadata:
  labels:
    app: spark-driver
spec:
  nodeSelector:
    node-type: spark-driver
  tolerations:
  - key: "spark"
    operator: "Equal"
    value: "driver"
    effect: "NoSchedule"
  containers:
  - name: spark-kubernetes-driver
    resources:
      requests:
        memory: "4Gi"
        cpu: "2"
```

### Executor Pod Template
```yaml
# executor-template.yaml
apiVersion: v1
kind: Pod
metadata:
  labels:
    app: spark-executor
spec:
  nodeSelector:
    node-type: spark-executor
  priorityClassName: spark-priority
  containers:
  - name: spark-kubernetes-executor
    resources:
      requests:
        memory: "8Gi"
        cpu: "2"
```

### Apply Templates
```bash
--conf spark.kubernetes.driver.podTemplateFile=/path/to/driver-template.yaml
--conf spark.kubernetes.executor.podTemplateFile=/path/to/executor-template.yaml
```

## Volumes & Storage

### Persistent Volume Claims
```bash
# Mount existing PVC
--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.data.mount.path=/data
--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.data.options.claimName=my-pvc

# On-demand PVC per executor (for shuffle/spill)
--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local.mount.path=/tmp/spark
--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local.options.claimName=OnDemand
--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local.options.storageClass=fast-ssd
--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local.options.sizeLimit=100Gi
```

### EmptyDir (RAM-backed)
```bash
# Use tmpfs for local storage (faster shuffles)
--conf spark.kubernetes.local.dirs.tmpfs=true
--conf spark.executor.memoryOverheadFactor=0.4
```

### ConfigMaps
```bash
--conf spark.kubernetes.driver.volumes.configMap.config.mount.path=/etc/spark-config
--conf spark.kubernetes.driver.volumes.configMap.config.options.name=spark-config
```

## Secrets

### Mount as Files
```bash
--conf spark.kubernetes.driver.secrets.db-creds=/etc/secrets
--conf spark.kubernetes.executor.secrets.db-creds=/etc/secrets
```

### As Environment Variables
```bash
--conf spark.kubernetes.driver.secretKeyRef.DB_PASSWORD=db-creds:password
--conf spark.kubernetes.executor.secretKeyRef.DB_PASSWORD=db-creds:password
```

## RBAC Setup

### Create Service Account
```bash
kubectl create namespace spark
kubectl create serviceaccount spark -n spark
kubectl create clusterrolebinding spark-role \
    --clusterrole=edit \
    --serviceaccount=spark:spark
```

### Minimal Permissions
```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-role
  namespace: spark
rules:
- apiGroups: [""]
  resources: ["pods", "services", "configmaps", "persistentvolumeclaims"]
  verbs: ["create", "get", "list", "watch", "delete"]
- apiGroups: [""]
  resources: ["pods/log"]
  verbs: ["get", "list"]
```

## Debugging

### View Driver Logs
```bash
# Stream logs
kubectl logs -f <driver-pod-name> -n spark

# Previous crashed pod
kubectl logs <driver-pod-name> --previous
```

### Access Spark UI
```bash
kubectl port-forward <driver-pod-name> 4040:4040 -n spark
# Open http://localhost:4040
```

### Driver Logs in UI (Spark 4.0+)
```bash
--conf spark.driver.log.localDir=/tmp/spark-logs
# Access via UI: http://localhost:4040/logs/
```

### Describe Pod
```bash
kubectl describe pod <driver-pod-name> -n spark
```

### Keep Executor Pods for Debugging
```bash
--conf spark.kubernetes.executor.deleteOnTermination=false
```

## Job Management

### Kill Job
```bash
spark-submit --kill spark:<driver-pod-name> \
    --master k8s://https://k8s-apiserver:6443

# With glob pattern
spark-submit --kill "spark:my-job-*" \
    --master k8s://https://k8s-apiserver:6443
```

### Check Status
```bash
spark-submit --status spark:<driver-pod-name> \
    --master k8s://https://k8s-apiserver:6443
```

### Graceful Termination
```bash
--conf spark.kubernetes.executor.terminationGracePeriodSeconds=60
--conf spark.kubernetes.appKillPodDeletionGracePeriod=60
```

## Dependency Management

### Using S3 for File Uploads
```bash
--packages org.apache.hadoop:hadoop-aws:3.4.1
--conf spark.kubernetes.file.upload.path=s3a://bucket/spark-uploads
--conf spark.hadoop.fs.s3a.access.key=ACCESS_KEY
--conf spark.hadoop.fs.s3a.secret.key=SECRET_KEY
my-local-app.jar
```

### Pre-built Image (Recommended)
```bash
# Use local:// for files baked into image
--conf spark.kubernetes.container.image=myrepo/spark-app:v1
local:///opt/spark/work-dir/app.jar
```

## Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `Forbidden` | RBAC permissions | Create service account with edit role |
| `ImagePullBackOff` | Can't pull image | Check image name, registry auth |
| `Pending` executors | Insufficient resources | Check node capacity, resource requests |
| `OOMKilled` | Memory exceeded | Increase memory, memoryOverhead |
| `CrashLoopBackOff` | Application error | Check logs: `kubectl logs <pod>` |
| Driver UI inaccessible | Network/port issue | Use `kubectl port-forward` |

## Production Best Practices

1. **Use dedicated namespace**: Isolate Spark workloads
2. **Set resource limits**: Prevent noisy neighbors
3. **Use pod templates**: Standardize configurations
4. **Enable dynamic allocation**: Scale with workload
5. **Use persistent volumes**: For checkpoints and shuffle
6. **Set pod priorities**: For critical jobs
7. **Configure node selectors**: Target appropriate nodes
8. **Use secrets**: Never hardcode credentials

## Spark Connect on Kubernetes

Deploy Spark Connect server for thin-client access:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spark-connect
spec:
  replicas: 1
  selector:
    matchLabels:
      app: spark-connect
  template:
    metadata:
      labels:
        app: spark-connect
    spec:
      containers:
      - name: spark-connect
        image: apache/spark:4.1.0
        command: ["/opt/spark/sbin/start-connect-server.sh"]
        ports:
        - containerPort: 15002
        resources:
          requests:
            memory: "4Gi"
            cpu: "2"
---
apiVersion: v1
kind: Service
metadata:
  name: spark-connect
spec:
  type: ClusterIP
  ports:
  - port: 15002
    targetPort: 15002
  selector:
    app: spark-connect
```

## See Also

- [Spark-Connect.md](Spark-Connect.md) - Client-server architecture
- [SDP.md](SDP.md) - Declarative Pipelines
- [Performance-Tuning.md](Performance-Tuning.md) - Optimization
