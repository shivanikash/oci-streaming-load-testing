# OCI Streaming Load Test Producer (Java)

A high-performance Java application for load testing OCI Streaming Service with realistic metric data.

## Architecture

```
Java Producer → OCI Streaming → OCI Function → New Relic
```

## Quick Start

### Prerequisites
- Java 11 or higher
- Maven 3.6+
- OCI CLI configured (`oci setup config`)

### Build and Run

```bash
# Build the project
mvn clean package

# Run a basic test
./run_test.sh --stream-id ocid1.stream.oc1.us-ashburn-1.your_stream_id --rate 50

# Run predefined scenarios
./run_test.sh --stream-id ocid1.stream.oc1.us-ashburn-1.your_stream_id --scenario baseline
./run_test.sh --stream-id ocid1.stream.oc1.us-ashburn-1.your_stream_id --scenario stress
./run_test.sh --stream-id ocid1.stream.oc1.us-ashburn-1.your_stream_id --scenario spike
```

## Usage

### Command Line Options

```bash
java -jar target/oci-stream-producer-1.0.0.jar [options]

Required:
  --stream-id <OCID>        OCI Stream OCID

Optional:
  --endpoint <URL>          OCI Streaming endpoint (auto-discovered if not provided)
                            Format: https://cell-1.streaming.{region}.oci.oraclecloud.com
  --rate <int>              Messages per second (default: 10)
  --duration <int>          Test duration in seconds (default: 60)
  --workers <int>           Number of worker threads (default: 2)
  --config <path>           OCI config file path (default: ~/.oci/config)
  --profile <name>          OCI config profile (default: DEFAULT)
  --scenario <name>         Predefined scenario: baseline|stress|spike
```

### Predefined Scenarios

#### Baseline Test
- **Duration**: 5 minutes
- **Rate**: 10 messages/second
- **Workers**: 2 threads
- **Purpose**: Establish performance baseline

#### Stress Test
- **Phases**: 20, 50, 100, 200 msg/s
- **Duration**: 2 minutes per phase
- **Workers**: 4 threads
- **Purpose**: Find breaking point

#### Spike Test
- **Pattern**: Normal → Spike → Normal
- **Spike Rate**: 500 msg/s for 30 seconds
- **Workers**: 2-10 threads
- **Purpose**: Test burst handling

## Generated Metrics

The producer generates realistic OCI metric messages across multiple namespaces:

- **oci_computeagent**: CPU, Memory, Disk, Network metrics
- **oci_lbaas**: Load balancer performance metrics
- **oci_blockvolumes**: Storage performance metrics
- **oci_autonomous_database**: Database performance metrics
- **oci_functions**: Function execution metrics

### Sample Message Structure

```json
{
  "timestamp": "2025-10-15T10:30:00Z",
  "namespace": "oci_computeagent",
  "name": "CpuUtilization",
  "dimensions": {
    "compartmentId": "ocid1.compartment.oc1..xxx",
    "region": "us-ashburn-1",
    "availabilityDomain": "AD-1",
    "resourceId": "ocid1.instance.oc1.us-ashburn-1.xxx",
    "resourceDisplayName": "compute-instance-123"
  },
  "metadata": {
    "unit": "percent",
    "displayName": "CPU Utilization",
    "source": "load-test-producer-java"
  },
  "datapoint": {
    "timestamp": "2025-10-15T10:30:00Z",
    "value": 67.5,
    "count": 1
  }
}
```

## Performance Features

- **Multi-threaded**: Configurable worker threads for high throughput
- **Rate limiting**: Precise message rate control
- **Auto-partitioning**: Automatic partition key distribution
- **Realistic data**: Metric values based on actual OCI service patterns
- **Real-time stats**: Live progress reporting during execution
- **Graceful shutdown**: Handles Ctrl+C gracefully

## Monitoring Output

```
🚀 Worker 1 started: 25 msg/s for 60 seconds
🚀 Worker 2 started: 25 msg/s for 60 seconds
📊 Sent: 500, Failed: 2, Rate: 49.8 msg/s
📊 Sent: 1000, Failed: 3, Rate: 50.2 msg/s

📊 LOAD TEST RESULTS
==================================================
⏱️  Duration: 60.12 seconds
✅ Messages Sent: 3000
❌ Messages Failed: 5
📊 Actual Rate: 49.9 messages/second
💾 Data Sent: 2.4 MB
📈 Success Rate: 99.8%
==================================================
```

## Configuration

### OCI Configuration
Ensure your OCI CLI is configured:

```bash
oci setup config
```

The producer uses standard OCI configuration files (`~/.oci/config`) and supports multiple profiles.

### Stream Requirements
- Stream must be in ACTIVE state
- Producer needs permissions to put messages
- Recommended partition count: 1-10 for load testing

## Building from Source

```bash
# Clone and build
git clone <repository>
cd oci-stream-producer
mvn clean package

# The fat JAR will be created at:
# target/oci-stream-producer-1.0.0.jar
```

## Troubleshooting

### Common Issues

1. **Authentication Error**
   ```
   Solution: Run 'oci setup config' to configure OCI CLI
   ```

2. **Stream Not Found**
   ```
   Solution: Verify stream OCID and ensure it's in ACTIVE state
   ```

3. **Permission Denied**
   ```
   Solution: Ensure user has 'stream_push' permission on the stream
   ```

4. **High Failure Rate**
   ```
   Solution: Reduce rate or increase stream partitions
   ```

## Integration with Your Function

This producer sends messages to your OCI Stream, which should trigger your OCI Function to:

1. **Receive** messages from the stream
2. **Process** the metric data
3. **Forward** to New Relic API

Monitor your function's performance during load tests:

```bash
# Monitor function invocations
oci fn function list --application-id <app-id>
oci logging search --log-group-id <log-group-id>

# Check New Relic for received metrics
# Use New Relic Query Language (NRQL) to verify data arrival
```

This Java producer provides high-performance, realistic load testing for your OCI Streaming → Function → New Relic pipeline!

Resources:
oci_computeagent (15 metrics) - 40% of resources
oci_lbaas (17 metrics) - 10% of resources
oci_blockvolumes (12 metrics) - 30% of resources
oci_autonomous_database (16 metrics) - 5% of resources
oci_objectstorage (14 metrics) - 5% of resources
oci_kubernetes (11 metrics) - 3% of resources
oci_functions (10 metrics) - 4% of resources
oci_apigateway (13 metrics) - 3% of resources
oci_streaming (11 metrics) - Available but not used in default distribution

Variable Base Resource Count depending on scenario:
multiply-1cr: 1000 base resources
multiply-5cr: 1000 base resources
multiply-10cr: 2000 base resources

Key Resource Distribution (for 1000 base resources):

Compute Instances:    400 resources (40%)
Block Volumes:        300 resources (30%) 
Load Balancers:       100 resources (10%)
Databases:             50 resources (5%)
Object Storage:        50 resources (5%)
Functions:             40 resources (4%)
Kubernetes:            30 resources (3%)
API Gateway:           30 resources (3%)