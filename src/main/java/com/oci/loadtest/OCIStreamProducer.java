package com.oci.loadtest;

import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.auth.AuthenticationDetailsProvider;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.PutMessagesDetails;
import com.oracle.bmc.streaming.model.PutMessagesDetailsEntry;
import com.oracle.bmc.streaming.model.PutMessagesResult;
import com.oracle.bmc.streaming.requests.PutMessagesRequest;
import com.oracle.bmc.streaming.responses.PutMessagesResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * OCI Streaming Producer for Load Testing
 * Generates realistic metric messages and sends them to OCI Streaming Service
 */
public class OCIStreamProducer {
    
    // Comprehensive OCI Services and their complete metric sets
    private static final String[] NAMESPACES = {
        "oci_computeagent", "oci_lbaas", "oci_blockvolumes", 
        "oci_apigateway", "oci_functions", "oci_streaming", "oci_objectstorage",
        "oci_postgresql", "oci_kubernetes", "oci_containerengine"
    };
    
    // Complete metrics for each OCI service (based on real OCI Monitoring metrics)
    private static final Map<String, String[]> METRICS_BY_NAMESPACE = new HashMap<>();
    static {
        // Compute Agent - All VM/Instance metrics
        METRICS_BY_NAMESPACE.put("oci_computeagent", new String[]{
            "CpuUtilization", "MemoryUtilization", "DiskBytesRead", "DiskBytesWritten", 
            "DiskIopsRead", "DiskIopsWritten", "NetworksBytesIn", "NetworksBytesOut",
            "NetworkPacketsIn", "NetworkPacketsOut", "LoadAverage", "ProcessCount",
            "DiskUtilization", "SwapUtilization", "FileSystemUtilization"
        });
        
        // Load Balancer - All LB metrics  
        METRICS_BY_NAMESPACE.put("oci_lbaas", new String[]{
            "HttpRequests", "HttpResponseTime", "ActiveConnections", "NewConnections",
            "ThroughputIn", "ThroughputOut", "Http2xxCount", "Http3xxCount", 
            "Http4xxCount", "Http5xxCount", "FailedSSLHandshake", "BackendConnectionErrors",
            "RequestProcessingTime", "ResponseProcessingTime", "HealthyBackendCount",
            "UnhealthyBackendCount", "RejectedConnections"
        });
        
        // Block Volumes - All storage metrics
        METRICS_BY_NAMESPACE.put("oci_blockvolumes", new String[]{
            "VolumeReadBytes", "VolumeWriteBytes", "VolumeReadOps", "VolumeWriteOps", 
            "VolumeThroughputPercentage", "VolumeIopsPercentage", "VolumeQueueDepth",
            "VolumeReadThroughput", "VolumeWriteThroughput", "VolumeLatency",
            "VolumeReadLatency", "VolumeWriteLatency"
        });
        
        // API Gateway - All API metrics
        METRICS_BY_NAMESPACE.put("oci_apigateway", new String[]{
            "TotalRequests", "ErrorRequests", "RequestLatency", "BackendLatency",
            "GatewayLatency", "ThrottledRequests", "AuthenticationErrors", 
            "AuthorizationErrors", "ResponseSize", "RequestSize", "CacheHits",
            "CacheMisses", "RateLimitExceeded"
        });
        
        // Functions - All serverless metrics
        METRICS_BY_NAMESPACE.put("oci_functions", new String[]{
            "FunctionInvocations", "FunctionDuration", "FunctionErrors",
            "FunctionConcurrentExecutions", "FunctionTimeouts", "FunctionMemoryUtilization",
            "FunctionThrottles", "FunctionColdStarts", "FunctionBillableInvocations",
            "FunctionProvisionedConcurrency"
        });
        
        // Streaming - All streaming metrics
        METRICS_BY_NAMESPACE.put("oci_streaming", new String[]{
            "IncomingRecords", "OutgoingRecords", "IncomingBytes", "OutgoingBytes",
            "RetentionPeriod", "PartitionCount", "ConsumerLag", "PutRecordsLatency",
            "GetRecordsLatency", "PutRecordsSuccessfulRecords", "GetRecordsSuccessfulRecords"
        });
        
        // Object Storage - All storage metrics
        METRICS_BY_NAMESPACE.put("oci_objectstorage", new String[]{
            "BucketSizeBytes", "ObjectCount", "RequestCount", "SuccessfulRequestCount",
            "ClientErrors", "ServerErrors", "ThrottledRequests", "TotalUploadBytes",
            "TotalDownloadBytes", "ListRequests", "HeadRequests", "GetRequests",
            "PutRequests", "DeleteRequests"
        });
        
        // PostgreSQL Database Service
        METRICS_BY_NAMESPACE.put("oci_postgresql", new String[]{
            "ActiveConnections", "CpuUtilization", "MemoryUtilization", "NetworkReceive",
            "NetworkTransmit", "DiskIORead", "DiskIOWrite", "QueriesPerSecond",
            "SlowQueries", "ConnectionErrors", "DatabaseSize", "BinlogUsage"
        });
        
        // Container Engine for Kubernetes
        METRICS_BY_NAMESPACE.put("oci_kubernetes", new String[]{
            "NodeCpuUtilization", "NodeMemoryUtilization", "PodCount", "ServiceCount",
            "DeploymentCount", "NamespaceCount", "ContainerRestarts", "NetworkPolicy",
            "PersistentVolumeUsage", "NodeReadiness", "ClusterAutoscalerEvents"
        });
    }
    
    private static final String[] REGIONS = {
        "us-ashburn-1"
    };
    
    private final StreamClient streamClient;
    private final String streamId;
    private final ObjectMapper objectMapper;
    private final Random random;
    private final AtomicBoolean running;
    
    // Large-scale customer simulation parameters
    private final int resourceCount;
    private final double metricsPerResourcePerMinute;
    private final List<String> resourceIds;
    private final Map<String, String> resourceToNamespace;
    
    // Statistics
    private final AtomicLong messagesSent = new AtomicLong(0);
    private final AtomicLong messagesFailed = new AtomicLong(0);
    private final AtomicLong bytesSent = new AtomicLong(0);
    private final List<String> errors = Collections.synchronizedList(new ArrayList<>());
    private long startTime;
    
    // Global counters for entire test session
    private final AtomicLong totalMessagesSent = new AtomicLong(0);
    private final AtomicLong totalMessagesFailed = new AtomicLong(0);
    
    public OCIStreamProducer(String streamId, String endpoint, String configFile, String profile) 
            throws IOException {
        this(streamId, endpoint, configFile, profile, 1000, 12.0); // Default: 1000 resources, 12 metrics/resource/minute
    }
    
    public OCIStreamProducer(String streamId, String endpoint, String configFile, String profile,
                           int resourceCount, double metricsPerResourcePerMinute) throws IOException {
        this.streamId = streamId;
        this.objectMapper = new ObjectMapper();
        this.random = new Random();
        this.running = new AtomicBoolean(false);
        this.resourceCount = resourceCount;
        this.metricsPerResourcePerMinute = metricsPerResourcePerMinute;
        
        // Initialize large-scale resource simulation
        this.resourceIds = new ArrayList<>();
        this.resourceToNamespace = new HashMap<>();
        initializeCustomerResources();
        
        // Initialize OCI client
        ConfigFileReader.ConfigFile config = ConfigFileReader.parse(configFile, profile);
        AuthenticationDetailsProvider provider = new ConfigFileAuthenticationDetailsProvider(config);
        
        // Auto-discover endpoint from stream OCID if not provided
        String streamingEndpoint;
        if (endpoint != null) {
            streamingEndpoint = endpoint;
            System.out.println("🔗 Using provided endpoint: " + streamingEndpoint);
        } else {
            streamingEndpoint = autoDiscoverEndpoint(streamId);
            System.out.println("🔍 Auto-discovered endpoint: " + streamingEndpoint);
        }
        
        this.streamClient = StreamClient.builder()
                .endpoint(streamingEndpoint)
                .build(provider);
        System.out.println("✅ OCI Stream Client initialized");
        System.out.println("🎯 Stream ID: " + streamId);
        System.out.println("🔗 Endpoint: " + streamingEndpoint);
        System.out.println("🏗️ Simulating " + resourceCount + " customer resources");
        System.out.println("📊 Target: " + (resourceCount * metricsPerResourcePerMinute) + " metrics/minute");
    }
    
    /**
     * Initialize a realistic customer environment with thousands of resources
     */
    private void initializeCustomerResources() {
        System.out.println("🏗️ Initializing customer resources...");
        
        // Distribute resources across services realistically
        Map<String, Integer> resourceDistribution = new HashMap<>();
        resourceDistribution.put("oci_computeagent", (int) (resourceCount * 0.45));     // 45% - VM instances
        resourceDistribution.put("oci_lbaas", (int) (resourceCount * 0.12));           // 12% - Load balancers  
        resourceDistribution.put("oci_blockvolumes", (int) (resourceCount * 0.30));    // 30% - Block volumes
        resourceDistribution.put("oci_objectstorage", (int) (resourceCount * 0.05));   // 5% - Object storage
        resourceDistribution.put("oci_kubernetes", (int) (resourceCount * 0.03));      // 3% - K8s clusters
        resourceDistribution.put("oci_functions", (int) (resourceCount * 0.03));       // 3% - Functions
        resourceDistribution.put("oci_apigateway", (int) (resourceCount * 0.02));      // 2% - API Gateways
        
        // Generate realistic resource IDs for each service
        for (Map.Entry<String, Integer> entry : resourceDistribution.entrySet()) {
            String namespace = entry.getKey();
            int count = entry.getValue();
            
            for (int i = 0; i < count; i++) {
                String resourceId = generateRealisticResourceId(namespace, i);
                resourceIds.add(resourceId);
                resourceToNamespace.put(resourceId, namespace);
            }
        }
        
        System.out.println("✅ Generated " + resourceIds.size() + " customer resources");
        System.out.println("📋 Resource breakdown:");
        for (Map.Entry<String, Integer> entry : resourceDistribution.entrySet()) {
            System.out.printf("   • %s: %d resources%n", entry.getKey(), entry.getValue());
        }
    }
    
    /**
     * Generate realistic resource IDs based on service type
     */
    private String generateRealisticResourceId(String namespace, int index) {
        String serviceType = namespace.replace("oci_", "");
        String region = "iad"; // us-ashburn-1
        
        StringBuilder uniqueId = new StringBuilder();
        for (int i = 0; i < 60; i++) {
            if (random.nextBoolean()) {
                uniqueId.append((char) ('a' + random.nextInt(26)));
            } else {
                uniqueId.append(random.nextInt(10));
            }
        }
        
        // Generate service-specific OCIDs
        switch (serviceType) {
            case "computeagent":
                return String.format("ocid1.instance.oc1.%s.%s", region, uniqueId);
            case "lbaas":
                return String.format("ocid1.loadbalancer.oc1.%s.%s", region, uniqueId);
            case "blockvolumes":
                return String.format("ocid1.volume.oc1.%s.%s", region, uniqueId);
            case "kubernetes":
                return String.format("ocid1.cluster.oc1.%s.%s", region, uniqueId);
            case "functions":
                return String.format("ocid1.fnfunc.oc1.%s.%s", region, uniqueId);
            default:
                return String.format("ocid1.%s.oc1.%s.%s", serviceType, region, uniqueId);
        }
    }
    
    private String autoDiscoverEndpoint(String streamId) {
        try {
            // Extract region from stream OCID
            // Format: ocid1.stream.oc1.{region}.{unique-id}
            String[] parts = streamId.split("\\.");
            if (parts.length >= 4) {
                String regionCode = parts[3];
                // Map region codes to full region names
                String fullRegion = mapRegionCodeToFullName(regionCode);
                String endpoint = String.format("https://cell-1.streaming.%s.oci.oraclecloud.com", fullRegion);
                System.out.println("🔍 Detected region code: " + regionCode + " → " + fullRegion);
                return endpoint;
            } else {
                throw new IllegalArgumentException("Invalid Stream OCID format. Expected: ocid1.stream.oc1.{region}.{unique-id}");
            }
        } catch (Exception e) {
            System.err.println("❌ Could not auto-discover endpoint from Stream OCID: " + streamId);
            System.err.println("❌ Error: " + e.getMessage());
            System.err.println("💡 Please provide endpoint manually using --endpoint parameter");
            System.err.println("💡 Example: --endpoint https://cell-1.streaming.your-region.oci.oraclecloud.com");
        }
        throw new RuntimeException("Could not determine streaming endpoint");
    }
    
    private String mapRegionCodeToFullName(String regionCode) {
        switch (regionCode.toLowerCase()) {
            case "iad": return "us-ashburn-1";
            case "phx": return "us-phoenix-1";
            case "lhr": return "uk-london-1";
            case "fra": return "eu-frankfurt-1";
            case "nrt": return "ap-tokyo-1";
            case "icn": return "ap-seoul-1";
            case "syd": return "ap-sydney-1";
            case "mumbai": return "ap-mumbai-1";
            case "gru": return "sa-saopaulo-1";
            case "yyz": return "ca-toronto-1";
            case "cwl": return "uk-cardiff-1";
            case "ams": return "eu-amsterdam-1";
            case "mel": return "ap-melbourne-1";
            case "bom": return "ap-mumbai-1";
            case "kix": return "ap-osaka-1";
            case "hyd": return "ap-hyderabad-1";
            case "jed": return "me-jeddah-1";
            case "dxb": return "me-dubai-1";
            default:
                System.out.println("⚠️  Unknown region code '" + regionCode + "', using as-is");
                return regionCode;
        }
    }
    
    /**
     * Generate multiple metrics for a single resource (realistic customer scenario)
     */
    public List<ObjectNode> generateResourceMetricBatch(String resourceId, String namespace) {
        List<ObjectNode> metrics = new ArrayList<>();
        String[] availableMetrics = METRICS_BY_NAMESPACE.getOrDefault(namespace, new String[]{"CustomMetric"});
        
        // Generate 3-8 different metrics per resource (realistic for 1-minute interval)
        int metricsToGenerate = 3 + random.nextInt(6);
        Set<String> usedMetrics = new HashSet<>();
        
        for (int i = 0; i < metricsToGenerate && usedMetrics.size() < availableMetrics.length; i++) {
            String metricName;
            do {
                metricName = availableMetrics[random.nextInt(availableMetrics.length)];
            } while (usedMetrics.contains(metricName));
            usedMetrics.add(metricName);
            
            metrics.add(generateSingleMetric(resourceId, namespace, metricName));
        }
        
        return metrics;
    }
    
    /**
     * Generate a single realistic OCI metric message for a specific resource
     */
    public ObjectNode generateSingleMetric(String resourceId, String namespace, String metricName) {
        
        // Generate realistic values based on metric type
        double value;
        String unit;
        
        if (metricName.contains("Utilization")) {
            value = 5.0 + random.nextDouble() * 90.0; // 5-95%
            unit = "percent";
        } else if (metricName.contains("Bytes")) {
            value = 1024 + random.nextInt(10485760); // 1KB to 10MB
            unit = "bytes";
        } else if (metricName.contains("Requests") || metricName.contains("Connections") || metricName.contains("Count")) {
            value = random.nextInt(1000);
            unit = "count";
        } else if (metricName.contains("Time") || metricName.contains("Duration")) {
            value = 0.1 + random.nextDouble() * 9.9; // 0.1-10 seconds
            unit = "seconds";
        } else {
            value = random.nextDouble() * 100.0;
            unit = "none";
        }
        
        // Create message structure matching real OCI Monitoring format
        ObjectNode message = objectMapper.createObjectNode();
        
        // Root level properties (OCI Monitoring Service format)
        message.put("eventType", "com.oraclecloud.monitoring.metric");
        message.put("cloudEventsVersion", "0.1");
        message.put("eventTypeVersion", "2.0");
        message.put("source", "oci.monitoring." + namespace.replace("oci_", ""));
        message.put("eventID", java.util.UUID.randomUUID().toString());
        message.put("eventTime", Instant.now().atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
        message.put("contentType", "application/json");
        
        // Data payload (the actual metric)
        ObjectNode data = objectMapper.createObjectNode();
        data.put("compartmentId", generateCompartmentId());
        data.put("namespace", namespace);
        data.put("name", metricName);
        data.put("resourceGroup", namespace.replace("oci_", ""));
        
        // Dimensions (resource identifiers)
        ObjectNode dimensions = objectMapper.createObjectNode();
        dimensions.put("compartmentId", generateCompartmentId());
        dimensions.put("region", REGIONS[random.nextInt(REGIONS.length)]);
        dimensions.put("availabilityDomain", "IAD-AD-" + (random.nextInt(3) + 1));
        dimensions.put("resourceId", generateResourceId());
        dimensions.put("resourceDisplayName", "loadtest-" + namespace.replace("oci_", "") + "-" + random.nextInt(999));
        
        // Add namespace-specific dimensions
        if (namespace.equals("oci_computeagent")) {
            dimensions.put("shape", "VM.Standard.E4.Flex");
            dimensions.put("faultDomain", "FAULT-DOMAIN-" + (random.nextInt(3) + 1));
        } else if (namespace.equals("oci_lbaas")) {
            dimensions.put("backendSet", "backend-set-" + random.nextInt(5));
            dimensions.put("listener", "listener-" + random.nextInt(3));
        } else if (namespace.equals("oci_database")) {
            dimensions.put("dbSystem", "db-system-" + random.nextInt(10));
            dimensions.put("pluggableDatabase", "pdb-" + random.nextInt(5));
        }
        
        data.set("dimensions", dimensions);
        
        // Metadata
        ObjectNode metadata = objectMapper.createObjectNode();
        metadata.put("unit", unit);
        metadata.put("displayName", metricName.replaceAll("([A-Z])", " $1").trim());
        metadata.put("aggregationMethod", "MEAN");
        metadata.put("interval", "PT1M"); // 1-minute interval
        data.set("metadata", metadata);
        
        // Datapoints array (OCI sends arrays of datapoints)
        ObjectNode datapoint = objectMapper.createObjectNode();
        datapoint.put("timestamp", Instant.now().atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
        datapoint.put("value", Math.round(value * 100.0) / 100.0);
        datapoint.put("count", 1);
        
        // Create datapoints array
        data.set("datapoints", objectMapper.createArrayNode().add(datapoint));
        
        // Add the data payload to the message
        message.set("data", data);
        
        return message;
    }
    
    /**
     * High-volume producer for large customer scenarios (lakhs of resources)
     */
    public void runHighVolumeTest(int totalRate, int duration, int numWorkers, int resourceMultiplier) {
        System.out.println("\n🏭 Starting HIGH-VOLUME Customer Simulation");
        System.out.printf("📈 Target Rate: %d messages/second%n", totalRate);
        System.out.printf("⏱️  Duration: %d seconds%n", duration);
        System.out.printf("👥 Workers: %d%n", numWorkers);
        System.out.printf("🏗️ Simulating: %d resources%n", resourceCount * resourceMultiplier);
        System.out.printf("📊 Expected metrics/minute: %.0f%n", (resourceCount * resourceMultiplier * metricsPerResourcePerMinute));
        System.out.printf("🎯 Stream: %s%n", streamId);
        
        startTime = System.currentTimeMillis();
        running.set(true);
        
        ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
        CountDownLatch latch = new CountDownLatch(numWorkers);
        
        int ratePerWorker = totalRate / numWorkers;
        
        for (int i = 1; i <= numWorkers; i++) {
            final int workerId = i;
            executor.submit(() -> {
                try {
                    highVolumeWorker(ratePerWorker, duration, workerId, resourceMultiplier);
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // Shutdown handling
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n🛑 Shutdown signal received...");
            running.set(false);
        }));
        
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        executor.shutdown();
        printFinalResults(duration);
    }
    
    /**
     * Worker for high-volume customer simulation
     */
    public void highVolumeWorker(int messagesPerSecond, int durationSeconds, int workerId, int resourceMultiplier) {
        System.out.printf("🚀 High-Volume Worker %d started: %d msg/s for %d seconds%n", 
                workerId, messagesPerSecond, durationSeconds);
        
        long intervalNanos = messagesPerSecond > 0 ? 1_000_000_000L / messagesPerSecond : 1_000_000_000L;
        long endTime = System.currentTimeMillis() + (durationSeconds * 1000L);
        long lastReportTime = System.currentTimeMillis();
        
        while (running.get() && System.currentTimeMillis() < endTime) {
            long startNanos = System.nanoTime();
            
            // Simulate realistic customer patterns - burst metrics from multiple resources
            int resourcesThisCycle = 1 + random.nextInt(Math.min(5, resourceIds.size() / 10));
            
            for (int r = 0; r < resourcesThisCycle; r++) {
                if (!running.get()) break;
                
                // Pick different resources to simulate concurrent metric generation
                String resourceId = resourceIds.get(random.nextInt(Math.min(resourceIds.size(), resourceIds.size() * resourceMultiplier / 100)));
                String namespace = resourceToNamespace.get(resourceId);
                
                // Generate 1-3 metrics per resource per cycle
                int metricsPerResource = 1 + random.nextInt(3);
                String[] availableMetrics = METRICS_BY_NAMESPACE.getOrDefault(namespace, new String[]{"CustomMetric"});
                
                for (int m = 0; m < metricsPerResource; m++) {
                    String metricName = availableMetrics[random.nextInt(availableMetrics.length)];
                    ObjectNode metric = generateSingleMetric(resourceId, namespace, metricName);
                    sendMessage(metric);
                }
            }
            
            // Report progress every 10 seconds for high-volume tests
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastReportTime >= 10000) {
                double rate = messagesSent.get() / ((currentTime - startTime) / 1000.0);
                System.out.printf("📊 Worker %d: Sent: %d, Failed: %d, Rate: %.1f msg/s%n", 
                        workerId, messagesSent.get(), messagesFailed.get(), rate);
                lastReportTime = currentTime;
            }
            
            // Control rate
            long elapsedNanos = System.nanoTime() - startNanos;
            if (elapsedNanos < intervalNanos) {
                try {
                    Thread.sleep((intervalNanos - elapsedNanos) / 1_000_000, 
                            (int) ((intervalNanos - elapsedNanos) % 1_000_000));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        System.out.printf("✅ High-Volume Worker %d completed%n", workerId);
    }
    
    /**
     * 20-minute GRADUAL PROGRESSIVE load test with smooth traffic increase
     * Extended duration with gentle, continuous ramp-up
     */
    public void runProgressiveLoadTest() {
        System.out.printf("%n🌊 Starting 20-MINUTE GRADUAL PROGRESSIVE LOAD TEST%n");
        System.out.println("⏱️  Duration: 20 minutes (1200 seconds)");
        System.out.println("📈 Pattern: Smooth gradual increase 200 → 1000 → 3000 → 5000 metrics/second");
        System.out.println("� Gentle ramp with longer phases for sustained load");
        System.out.println("🎯 Target: ~4.5 Million metrics over 20 minutes");
        System.out.println("============================================================");
        
        running.set(true);
        startTime = System.currentTimeMillis();
        
        // 20-minute gradual progressive configuration
        int totalDurationSeconds = 1200;  // 20 minutes
        
        // Phase definition: duration, rate, batchSize, workers
        int[][] gradualPhases = {
            // Gentle start (4 minutes)
            {120, 200, 20, 4},       // 0-2min: 200 metrics/s = 24K metrics
            {120, 400, 30, 6},       // 2-4min: 400 metrics/s = 48K metrics
            
            // Early ramp (4 minutes)
            {120, 600, 40, 8},       // 4-6min: 600 metrics/s = 72K metrics
            {120, 800, 50, 10},      // 6-8min: 800 metrics/s = 96K metrics
            
            // Mid ramp (4 minutes)
            {120, 1200, 70, 12},     // 8-10min: 1.2K metrics/s = 144K metrics
            {120, 1600, 90, 14},     // 10-12min: 1.6K metrics/s = 192K metrics
            
            // Higher load (4 minutes)
            {120, 2200, 120, 16},    // 12-14min: 2.2K metrics/s = 264K metrics
            {120, 2800, 150, 18},    // 14-16min: 2.8K metrics/s = 336K metrics
            
            // Peak phase (3 minutes)
            {90, 3500, 200, 22},     // 16-17.5min: 3.5K metrics/s = 315K metrics
            {90, 4200, 250, 25},     // 17.5-19min: 4.2K metrics/s = 378K metrics
            
            // Final burst (1 minute)
            {60, 5000, 300, 30}      // 19-20min: 5K metrics/s = 300K metrics
        };                           // Total: ~2.27M metrics (conservative estimate)
        
        System.out.println("🎯 GRADUAL PHASE SCHEDULE:");
        int totalExpected = 0;
        int cumulativeTime = 0;
        for (int i = 0; i < gradualPhases.length; i++) {
            int duration = gradualPhases[i][0];
            int rate = gradualPhases[i][1];
            int expected = rate * duration;
            totalExpected += expected;
            cumulativeTime += duration;
            System.out.printf("   Phase %d: %d-%dmin | %,d/s | %,d metrics%n", 
                    i + 1, cumulativeTime/60 - duration/60, cumulativeTime/60, rate, expected);
        }
        System.out.printf("📊 TOTAL EXPECTED: %,d metrics in %d minutes%n", totalExpected, totalDurationSeconds/60);
        System.out.println("============================================================");
        
        long totalMetricsSent = 0;
        
        // Execute each gradual phase
        for (int phaseIndex = 0; phaseIndex < gradualPhases.length; phaseIndex++) {
            int duration = gradualPhases[phaseIndex][0];
            int rate = gradualPhases[phaseIndex][1];
            int batchSize = gradualPhases[phaseIndex][2];
            int workers = gradualPhases[phaseIndex][3];
            
            System.out.printf("%n� PHASE %d: %,d metrics/s for %ds (batch=%d, workers=%d)%n", 
                    phaseIndex + 1, rate, duration, batchSize, workers);
            
            // Reset counters for this phase
            resetStats();
            
            // Run gradual batch producer for this phase
            runAggressiveBatchProducer(rate, duration, batchSize, workers);
            
            totalMetricsSent += messagesSent.get();
            
            double phaseRate = messagesSent.get() / (duration > 0 ? duration : 1.0);
            System.out.printf("✅ Phase %d completed: %,d metrics (%.0f/s actual)%n", 
                    phaseIndex + 1, messagesSent.get(), phaseRate);
            
            // Brief pause between phases (except last)
            if (phaseIndex < gradualPhases.length - 1) {
                System.out.println("⏸️  5-second gradual transition pause...");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        long totalDuration = System.currentTimeMillis() - startTime;
        double overallRate = totalMetricsSent / (totalDuration / 1000.0);
        
        System.out.printf("%n🏆 20-MINUTE GRADUAL PROGRESSIVE TEST COMPLETED%n");
        System.out.println("============================================================");
        System.out.printf("⏱️  Total Duration: %.1f minutes%n", totalDuration / 60000.0);
        System.out.printf("📊 Total Metrics Sent: %,d%n", totalMetricsSent);
        System.out.printf("📈 Overall Average Rate: %,.0f metrics/second%n", overallRate);
        System.out.printf("🎯 Target Achievement: %.1f%%\n", (totalMetricsSent * 100.0) / totalExpected);
        System.out.printf("🌊 Peak Rate: 5,000 metrics/second in final phase%n");
        System.out.println("============================================================");
        
        printTotalStats();
    }
    
    /**
     * GRADUAL batch producer optimized for sustained throughput over long duration
     */
    private void runAggressiveBatchProducer(int targetRate, int duration, int batchSize, int workers) {
        System.out.printf("🌊 Starting GRADUAL producer: %,d/s target%n", targetRate);
        
        running.set(true);
        long phaseStartTime = System.currentTimeMillis();
        
        ExecutorService executor = Executors.newFixedThreadPool(workers);
        List<Future<?>> futures = new ArrayList<>();
        
        // Calculate work distribution
        int ratePerWorker = targetRate / workers;
        int remainder = targetRate % workers;
        
        // Launch aggressive workers
        for (int i = 0; i < workers; i++) {
            final int workerId = i + 1;
            final int workerRate = ratePerWorker + (i < remainder ? 1 : 0);
            
            Future<?> future = executor.submit(() -> 
                gradualBatchWorker(workerRate, duration, batchSize, workerId, phaseStartTime));
            futures.add(future);
        }
        
        // Monitor progress
        long endTime = phaseStartTime + (duration * 1000L);
        long nextReportTime = phaseStartTime + 15000; // Report every 15 seconds
        
        while (running.get() && System.currentTimeMillis() < endTime) {
            long currentTime = System.currentTimeMillis();
            
            if (currentTime >= nextReportTime) {
                double elapsed = (currentTime - phaseStartTime) / 1000.0;
                double currentRate = messagesSent.get() / Math.max(elapsed, 1.0);
                double progress = (elapsed / duration) * 100.0;
                
                System.out.printf("� Progress: %.1f%% | Rate: %,.0f/s | Sent: %,d | Workers: %d%n", 
                        progress, currentRate, messagesSent.get(), workers);
                nextReportTime = currentTime + 15000;
            }
            
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        // Stop workers
        running.set(false);
        
        // Wait for completion
        for (Future<?> future : futures) {
            try {
                future.get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                System.err.println("⚠️ Worker completion error: " + e.getMessage());
            }
        }
        
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }
    
    /**
     * Gradual batch worker with optimized rate limiting for sustained load
     */
    private void gradualBatchWorker(int targetRate, int duration, int batchSize, int workerId, long startTime) {
        System.out.printf("🌊 Gradual Worker %d: %,d/s target, batch=%d%n", workerId, targetRate, batchSize);
        
        long endTime = startTime + (duration * 1000L);
        
        // Calculate optimal batch timing
        double batchesPerSecond = (double) targetRate / batchSize;
        long batchIntervalNanos = batchesPerSecond > 0 ? (long) (1_000_000_000.0 / batchesPerSecond) : 1_000_000_000L;
        
        while (running.get() && System.currentTimeMillis() < endTime) {
            long batchStart = System.nanoTime();
            
            // Generate and send batch efficiently
            List<ObjectNode> batch = generateOptimizedBatch(batchSize);
            sendBatch(batch);
            
            // Precise rate control using nanoseconds
            long elapsed = System.nanoTime() - batchStart;
            long sleepNanos = batchIntervalNanos - elapsed;
            
            if (sleepNanos > 0) {
                try {
                    Thread.sleep(sleepNanos / 1_000_000, (int) (sleepNanos % 1_000_000));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        System.out.printf("✅ Gradual Worker %d completed%n", workerId);
    }
    
    /**
     * Generate optimized batch for maximum efficiency
     */
    private List<ObjectNode> generateOptimizedBatch(int batchSize) {
        List<ObjectNode> batch = new ArrayList<>(batchSize);
        
        // Pre-select resources to reduce random access overhead
        String[] batchResources = new String[Math.min(10, resourceIds.size())];
        String[] batchNamespaces = new String[batchResources.length];
        
        for (int i = 0; i < batchResources.length; i++) {
            int resourceIndex = random.nextInt(resourceIds.size());
            batchResources[i] = resourceIds.get(resourceIndex);
            batchNamespaces[i] = resourceToNamespace.get(batchResources[i]);
        }
        
        // Generate batch using pre-selected resources
        for (int i = 0; i < batchSize; i++) {
            int resourceIndex = i % batchResources.length;
            String resourceId = batchResources[resourceIndex];
            String namespace = batchNamespaces[resourceIndex];
            
            String[] metrics = METRICS_BY_NAMESPACE.getOrDefault(namespace, new String[]{"CustomMetric"});
            String metricName = metrics[i % metrics.length];
            
            ObjectNode metric = generateSingleMetric(resourceId, namespace, metricName);
            batch.add(metric);
        }
        
        return batch;
    }
    


    /**
     * High-volume batch producer for lakhs of metrics
     * Generates massive volumes of metrics efficiently using batching
     */
    public void runBatchProducer(int metricsPerSecond, int durationSeconds, int batchSize, int numWorkers) {
        System.out.printf("%n🎯 Starting High-Volume Batch Producer%n");
        System.out.printf("📊 Target: %,d metrics/second%n", metricsPerSecond);
        System.out.printf("⏱️  Duration: %d seconds%n", durationSeconds);
        System.out.printf("📦 Batch Size: %d metrics per batch%n", batchSize);
        System.out.printf("👥 Workers: %d%n", numWorkers);
        System.out.printf("🎯 Total Expected: %,d metrics%n", metricsPerSecond * durationSeconds);
        System.out.println("------------------------------------------------------------");
        
        running.set(true);
        startTime = System.currentTimeMillis();
        
        // Create thread pool
        ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
        List<Future<?>> futures = new ArrayList<>();
        
        // Calculate metrics per worker
        int metricsPerWorker = metricsPerSecond / numWorkers;
        int remainder = metricsPerSecond % numWorkers;
        
        // Start workers
        for (int i = 0; i < numWorkers; i++) {
            final int workerId = i + 1;
            final int workerRate = metricsPerWorker + (i < remainder ? 1 : 0);
            
            Future<?> future = executor.submit(() -> 
                batchWorker(workerRate, durationSeconds, batchSize, workerId));
            futures.add(future);
        }
        
        // Wait for completion
        try {
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (Exception e) {
            System.err.println("❌ Worker error: " + e.getMessage());
        } finally {
            executor.shutdown();
            running.set(false);
        }
        
        printFinalResults(durationSeconds);
    }
    
    /**
     * Batch worker that generates metrics in batches for maximum efficiency
     */
    private void batchWorker(int metricsPerSecond, int durationSeconds, int batchSize, int workerId) {
        System.out.printf("🚀 Batch Worker %d started: %,d metrics/s in batches of %d%n", 
                workerId, metricsPerSecond, batchSize);
        
        long endTime = System.currentTimeMillis() + (durationSeconds * 1000L);
        long lastReportTime = System.currentTimeMillis();
        
        // Calculate timing for batches
        double batchesPerSecond = (double) metricsPerSecond / batchSize;
        long batchIntervalNanos = batchesPerSecond > 0 ? (long) (1_000_000_000.0 / batchesPerSecond) : 1_000_000_000L;
        
        while (running.get() && System.currentTimeMillis() < endTime) {
            long batchStartNanos = System.nanoTime();
            
            // Generate and send batch
            List<ObjectNode> batch = generateMetricBatch(batchSize);
            sendBatch(batch);
            
            // Report progress every 10 seconds
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastReportTime >= 10000) {
                double rate = messagesSent.get() / ((currentTime - startTime) / 1000.0);
                System.out.printf("📊 Batch Worker %d: Sent: %,d, Failed: %,d, Rate: %,.1f metrics/s%n", 
                        workerId, messagesSent.get(), messagesFailed.get(), rate);
                lastReportTime = currentTime;
            }
            
            // Control batch rate
            long elapsedNanos = System.nanoTime() - batchStartNanos;
            if (elapsedNanos < batchIntervalNanos) {
                try {
                    Thread.sleep((batchIntervalNanos - elapsedNanos) / 1_000_000, 
                            (int) ((batchIntervalNanos - elapsedNanos) % 1_000_000));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        System.out.printf("✅ Batch Worker %d completed%n", workerId);
    }
    
    /**
     * Generate a batch of metrics efficiently using resource multiplication
     * Simulates the same resources generating multiple metrics (realistic pattern)
     */
    private List<ObjectNode> generateMetricBatch(int batchSize) {
        List<ObjectNode> batch = new ArrayList<>(batchSize);
        
        // Define base resource pool (smaller set that gets multiplied)
        int baseResourceCount = 100; // 100 base resources
        String[] baseResourceIds = new String[baseResourceCount];
        String[] baseNamespaces = new String[baseResourceCount];
        
        // Initialize base resources once per batch
        for (int i = 0; i < baseResourceCount; i++) {
            baseNamespaces[i] = NAMESPACES[i % NAMESPACES.length];
            baseResourceIds[i] = generateResourceIdForNamespace(baseNamespaces[i]);
        }
        
        // Generate batch using resource multiplication
        for (int i = 0; i < batchSize; i++) {
            // Pick a base resource (this simulates the same resource sending multiple metrics)
            int resourceIndex = i % baseResourceCount;
            String baseResourceId = baseResourceIds[resourceIndex];
            String namespace = baseNamespaces[resourceIndex];
            
            // For each resource, cycle through all its possible metrics
            String[] availableMetrics = METRICS_BY_NAMESPACE.getOrDefault(namespace, new String[]{"CustomMetric"});
            String metricName = availableMetrics[i % availableMetrics.length];
            
            // Add resource multiplier suffix to simulate multiple instances of the same resource type
            int resourceMultiplier = (i / baseResourceCount) + 1;
            String multipliedResourceId = baseResourceId + "-instance-" + resourceMultiplier;
            
            ObjectNode metric = generateSingleMetric(multipliedResourceId, namespace, metricName);
            batch.add(metric);
        }
        
        return batch;
    }
    
    /**
     * Generate resource ID specific to namespace for more realistic simulation
     */
    private String generateResourceIdForNamespace(String namespace) {
        String region = REGIONS[random.nextInt(REGIONS.length)];
        StringBuilder uniqueId = new StringBuilder();
        for (int i = 0; i < 60; i++) {
            if (random.nextBoolean()) {
                uniqueId.append((char) ('a' + random.nextInt(26)));
            } else {
                uniqueId.append(random.nextInt(10));
            }
        }
        
        // Generate appropriate resource type based on namespace
        String resourceType;
        switch (namespace) {
            case "oci_computeagent":
                resourceType = "instance";
                break;
            case "oci_lbaas":
                resourceType = "loadbalancer";
                break;
            case "oci_postgresql":
                resourceType = "dbsystem";
                break;
            case "oci_functions":
                resourceType = "function";
                break;
            default:
                resourceType = "resource";
        }
        
        return String.format("ocid1.%s.oc1.%s.%s", resourceType, region, uniqueId);
    }
    
    /**
     * Ultra high-volume producer using resource multiplication strategy
     * Uses for loops to multiply the same base resources for maximum efficiency
     */
    public void runMultipliedResourceTest(int baseResources, int metricsPerResource, int multiplier, int durationSeconds, int batchSize, int numWorkers) {
        System.out.printf("%n🎯 Starting Ultra High-Volume Test with Resource Multiplication%n");
        System.out.printf("🏗️  Base Resources: %,d%n", baseResources);
        System.out.printf("📊 Metrics per Resource: %d%n", metricsPerResource);
        System.out.printf("🔢 Multiplier: %,dx (simulating %,d total resources)%n", multiplier, baseResources * multiplier);
        System.out.printf("📈 Total Metrics: %,d%n", baseResources * metricsPerResource * multiplier);
        System.out.printf("⏱️  Duration: %d seconds%n", durationSeconds);
        System.out.printf("📦 Batch Size: %d%n", batchSize);
        System.out.printf("👥 Workers: %d%n", numWorkers);
        System.out.println("------------------------------------------------------------");
        
        running.set(true);
        startTime = System.currentTimeMillis();
        
        // Pre-generate base resource templates
        List<ResourceTemplate> baseResourceTemplates = generateBaseResourceTemplates(baseResources);
        
        // Create thread pool
        ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
        List<Future<?>> futures = new ArrayList<>();
        
        // Calculate work distribution
        int totalBatches = (baseResources * metricsPerResource * multiplier) / batchSize;
        int batchesPerWorker = totalBatches / numWorkers;
        int remainder = totalBatches % numWorkers;
        
        // Start workers
        for (int i = 0; i < numWorkers; i++) {
            final int workerId = i + 1;
            final int workerBatches = batchesPerWorker + (i < remainder ? 1 : 0);
            
            Future<?> future = executor.submit(() -> 
                multipliedResourceWorker(baseResourceTemplates, metricsPerResource, multiplier, 
                                       workerBatches, batchSize, durationSeconds, workerId));
            futures.add(future);
        }
        
        // Wait for completion
        try {
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (Exception e) {
            System.err.println("❌ Worker error: " + e.getMessage());
        } finally {
            executor.shutdown();
            running.set(false);
        }
        
        printFinalResults(durationSeconds);
    }
    
    /**
     * Resource template for efficient multiplication
     */
    private static class ResourceTemplate {
        String baseResourceId;
        String namespace;
        String[] availableMetrics;
        
        ResourceTemplate(String baseResourceId, String namespace, String[] availableMetrics) {
            this.baseResourceId = baseResourceId;
            this.namespace = namespace;
            this.availableMetrics = availableMetrics;
        }
    }
    
    /**
     * Generate base resource templates that will be multiplied
     */
    private List<ResourceTemplate> generateBaseResourceTemplates(int count) {
        List<ResourceTemplate> templates = new ArrayList<>(count);
        
        for (int i = 0; i < count; i++) {
            String namespace = NAMESPACES[i % NAMESPACES.length];
            String baseResourceId = generateResourceIdForNamespace(namespace);
            String[] availableMetrics = METRICS_BY_NAMESPACE.getOrDefault(namespace, new String[]{"CustomMetric"});
            
            templates.add(new ResourceTemplate(baseResourceId, namespace, availableMetrics));
        }
        
        return templates;
    }
    
    /**
     * Worker that uses resource multiplication for ultra-high volume
     */
    private void multipliedResourceWorker(List<ResourceTemplate> baseTemplates, int metricsPerResource, 
                                        int multiplier, int batchesToProcess, int batchSize, 
                                        int durationSeconds, int workerId) {
        System.out.printf("🚀 Multiplied Worker %d started: %,d batches of %d metrics%n", 
                workerId, batchesToProcess, batchSize);
        
        long endTime = System.currentTimeMillis() + (durationSeconds * 1000L);
        long lastReportTime = System.currentTimeMillis();
        int processedBatches = 0;
        
        while (running.get() && processedBatches < batchesToProcess && System.currentTimeMillis() < endTime) {
            List<ObjectNode> batch = new ArrayList<>(batchSize);
            
            // Generate batch using resource multiplication pattern
            for (int i = 0; i < batchSize; i++) {
                // Use for loops to simulate the same resources generating multiple metrics
                int templateIndex = i % baseTemplates.size();
                ResourceTemplate template = baseTemplates.get(templateIndex);
                
                // Multiply resources: each base resource generates metrics for multiple instances
                int resourceInstance = (i / baseTemplates.size()) % multiplier + 1;
                int metricIndex = i % metricsPerResource;
                
                // Create multiplied resource ID
                String multipliedResourceId = template.baseResourceId + "-copy-" + resourceInstance;
                String metricName = template.availableMetrics[metricIndex % template.availableMetrics.length];
                
                ObjectNode metric = generateSingleMetric(multipliedResourceId, template.namespace, metricName);
                batch.add(metric);
            }
            
            sendBatch(batch);
            processedBatches++;
            
            // Report progress
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastReportTime >= 15000) { // Report every 15 seconds for ultra-high volume
                double rate = messagesSent.get() / ((currentTime - startTime) / 1000.0);
                System.out.printf("📊 Multiplied Worker %d: Batches: %,d/%,d, Rate: %,.0f metrics/s%n", 
                        workerId, processedBatches, batchesToProcess, rate);
                lastReportTime = currentTime;
            }
        }
        
        System.out.printf("✅ Multiplied Worker %d completed: %,d batches processed%n", workerId, processedBatches);
    }
    private void sendBatch(List<ObjectNode> batch) {
        try {
            List<PutMessagesDetailsEntry> entries = new ArrayList<>();
            
            for (ObjectNode metric : batch) {
                String partitionKey = String.valueOf(random.nextInt(100)); // More partitions for better distribution
                String messageJson = objectMapper.writeValueAsString(metric);
                
                PutMessagesDetailsEntry entry = PutMessagesDetailsEntry.builder()
                        .key(partitionKey.getBytes(StandardCharsets.UTF_8))
                        .value(messageJson.getBytes(StandardCharsets.UTF_8))
                        .build();
                
                entries.add(entry);
                bytesSent.addAndGet(messageJson.length());
            }
            
            // Send entire batch in single request
            PutMessagesDetails putMessagesDetails = PutMessagesDetails.builder()
                    .messages(entries)
                    .build();
            
            PutMessagesRequest request = PutMessagesRequest.builder()
                    .streamId(streamId)
                    .putMessagesDetails(putMessagesDetails)
                    .build();
            
            PutMessagesResponse response = streamClient.putMessages(request);
            PutMessagesResult result = response.getPutMessagesResult();
            
            if (result.getFailures() != null && result.getFailures() > 0) {
                messagesFailed.addAndGet(result.getFailures());
                messagesSent.addAndGet(batch.size() - result.getFailures());
                errors.add("Batch failed: " + result.getFailures() + " out of " + batch.size());
            } else {
                messagesSent.addAndGet(batch.size());
            }
            
        } catch (Exception e) {
            String error = "Batch send error: " + e.getMessage();
            errors.add(error);
            messagesFailed.addAndGet(batch.size());
            
            // Log first few batch errors for debugging
            if (errors.size() <= 3) {
                System.err.println("🚨 BATCH ERROR #" + errors.size() + ": " + e.getClass().getSimpleName() + " - " + e.getMessage());
                if (e.getCause() != null) {
                    System.err.println("   Caused by: " + e.getCause().getMessage());
                }
            }
        }
    }
    private void printFinalResults(int expectedDuration) {
        long actualDuration = System.currentTimeMillis() - startTime;
        double actualRate = messagesSent.get() / (actualDuration / 1000.0);
        double successRate = messagesSent.get() > 0 ? 
            (messagesSent.get() / (double)(messagesSent.get() + messagesFailed.get())) * 100.0 : 0.0;
        
        System.out.println("\n📊 HIGH-VOLUME LOAD TEST RESULTS");
        System.out.println("==================================================");
        System.out.printf("⏱️  Duration: %.2f seconds%n", actualDuration / 1000.0);
        System.out.printf("✅ Messages Sent: %d%n", messagesSent.get());
        System.out.printf("❌ Messages Failed: %d%n", messagesFailed.get());
        System.out.printf("📊 Actual Rate: %.2f messages/second%n", actualRate);
        System.out.printf("💾 Data Sent: %.2f KB%n", bytesSent.get() / 1024.0);
        System.out.printf("📈 Success Rate: %.1f%%%n", successRate);
        System.out.printf("🏗️ Simulated Resources: %d%n", resourceIds.size());
        System.out.printf("📊 Avg Metrics/Resource: %.1f%n", (double)messagesSent.get() / resourceIds.size());
        System.out.println("==================================================");
        
        // Show errors if any
        if (!errors.isEmpty()) {
            System.out.println("\n🚨 ERRORS ENCOUNTERED:");
            errors.stream().distinct().limit(5).forEach(error -> 
                System.out.println("   • " + error));
            if (errors.size() > 5) {
                System.out.printf("   ... and %d more errors%n", errors.size() - 5);
            }
        }
        
        System.out.println("\n✅ High-volume load test completed");
    }
    
    private String generateCompartmentId() {
        StringBuilder sb = new StringBuilder("ocid1.compartment.oc1..");
        for (int i = 0; i < 60; i++) {
            if (random.nextBoolean()) {
                sb.append((char) ('a' + random.nextInt(26)));
            } else {
                sb.append(random.nextInt(10));
            }
        }
        return sb.toString();
    }
    
    private String generateResourceId() {
        String region = REGIONS[random.nextInt(REGIONS.length)];
        StringBuilder uniqueId = new StringBuilder();
        for (int i = 0; i < 60; i++) {
            if (random.nextBoolean()) {
                uniqueId.append((char) ('a' + random.nextInt(26)));
            } else {
                uniqueId.append(random.nextInt(10));
            }
        }
        return String.format("ocid1.instance.oc1.%s.%s", region, uniqueId);
    }
    
    /**
     * Send a single message to OCI Streaming
     */
    public boolean sendMessage(ObjectNode message) {
        try {
            String partitionKey = String.valueOf(random.nextInt(10)); // Simple partitioning
            String messageJson = objectMapper.writeValueAsString(message);
            
            PutMessagesDetailsEntry entry = PutMessagesDetailsEntry.builder()
                    .key(partitionKey.getBytes(StandardCharsets.UTF_8))
                    .value(messageJson.getBytes(StandardCharsets.UTF_8))
                    .build();
            
            PutMessagesDetails putMessagesDetails = PutMessagesDetails.builder()
                    .messages(Arrays.asList(entry))
                    .build();
            
            PutMessagesRequest request = PutMessagesRequest.builder()
                    .streamId(streamId)
                    .putMessagesDetails(putMessagesDetails)
                    .build();
            
            PutMessagesResponse response = streamClient.putMessages(request);
            PutMessagesResult result = response.getPutMessagesResult();
            
            if (result.getFailures() != null && result.getFailures() > 0) {
                String error = "Message failed: " + result.getFailures() + " failures";
                errors.add(error);
                messagesFailed.incrementAndGet();
                return false;
            } else {
                messagesSent.incrementAndGet();
                bytesSent.addAndGet(messageJson.length());
                return true;
            }
            
        } catch (Exception e) {
            String error = "Send error: " + e.getMessage();
            errors.add(error);
            messagesFailed.incrementAndGet();
            
            // Log first few errors for debugging
            if (messagesFailed.get() <= 3) {
                System.err.println("🚨 ERROR #" + messagesFailed.get() + ": " + e.getClass().getSimpleName() + " - " + e.getMessage());
                if (e.getCause() != null) {
                    System.err.println("   Caused by: " + e.getCause().getMessage());
                }
            }
            return false;
        }
    }
    
    /**
     * Reset statistics for fresh test run
     */
    public void resetStats() {
        // Add current counts to totals before resetting
        totalMessagesSent.addAndGet(messagesSent.get());
        totalMessagesFailed.addAndGet(messagesFailed.get());
        
        // Reset current phase counters
        messagesSent.set(0);
        messagesFailed.set(0);
        bytesSent.set(0);
        errors.clear();
        running.set(false);
        // Don't reset startTime here - let runLoadTest set it
        System.out.println("📊 Statistics reset for new test phase");
        System.out.printf("📈 Total messages sent so far: %d%n", totalMessagesSent.get());
    }
    
    /**
     * Burst test - send messages as fast as possible to trigger function scaling
     */
    public void runBurstTest(int totalMessages, int timeWindowSeconds) {
        System.out.printf("💥 Starting BURST: %d messages in %d seconds%n", totalMessages, timeWindowSeconds);
        
        running.set(true);
        startTime = System.currentTimeMillis();
        
        // Use many workers to send messages as fast as possible
        int workerCount = Math.min(32, totalMessages / 10); // Lots of workers for burst
        int messagesPerWorker = totalMessages / workerCount;
        int remainder = totalMessages % workerCount;
        
        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        CountDownLatch latch = new CountDownLatch(workerCount);
        
        long endTime = System.currentTimeMillis() + (timeWindowSeconds * 1000L);
        
        // Launch burst workers
        for (int i = 0; i < workerCount; i++) {
            int workerMessages = messagesPerWorker + (i < remainder ? 1 : 0);
            final int workerId = i + 1;
            
            executor.submit(() -> {
                try {
                    System.out.printf("💥 Burst Worker %d: sending %d messages%n", workerId, workerMessages);
                    
                    for (int msg = 0; msg < workerMessages && running.get() && System.currentTimeMillis() < endTime; msg++) {
                        // Send messages as fast as possible - use random resource and metric
                        String resourceId = resourceIds.get(random.nextInt(resourceIds.size()));
                        String namespace = resourceToNamespace.get(resourceId);
                        String[] availableMetrics = METRICS_BY_NAMESPACE.getOrDefault(namespace, new String[]{"CustomMetric"});
                        String metricName = availableMetrics[random.nextInt(availableMetrics.length)];
                        
                        ObjectNode message = generateSingleMetric(resourceId, namespace, metricName);
                        sendMessage(message);
                        
                        // Remove all delays for maximum throughput
                        // Only yield CPU occasionally
                        if (msg % 200 == 0) {
                            Thread.yield();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        executor.shutdown();
        
        long actualDuration = System.currentTimeMillis() - startTime;
        double actualRate = messagesSent.get() / (actualDuration / 1000.0);
        
        System.out.printf("💥 BURST COMPLETED: %d messages sent in %.1f seconds (%.1f msg/s)%n", 
                messagesSent.get(), actualDuration / 1000.0, actualRate);
    }
    
    /**
     * Instant flood - send messages as fast as possible with NO rate limiting
     */
    public void runInstantFlood(int totalMessages) {
        System.out.printf("🌊 Starting INSTANT FLOOD: %d messages as fast as possible%n", totalMessages);
        
        running.set(true);
        startTime = System.currentTimeMillis();
        
        // Use maximum workers for instant flood
        int workerCount = Math.min(64, totalMessages / 50); // Many workers
        int messagesPerWorker = totalMessages / workerCount;
        int remainder = totalMessages % workerCount;
        
        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        CountDownLatch latch = new CountDownLatch(workerCount);
        
        // Launch flood workers (NO TIME LIMIT - just send as fast as possible)
        for (int i = 0; i < workerCount; i++) {
            int workerMessages = messagesPerWorker + (i < remainder ? 1 : 0);
            final int workerId = i + 1;
            
            executor.submit(() -> {
                try {
                    System.out.printf("🌊 Flood Worker %d: sending %d messages INSTANTLY%n", workerId, workerMessages);
                    
                    for (int msg = 0; msg < workerMessages && running.get(); msg++) {
                        // Send messages with ZERO delay - just flood the system
                        String resourceId = resourceIds.get(random.nextInt(resourceIds.size()));
                        String namespace = resourceToNamespace.get(resourceId);
                        String[] availableMetrics = METRICS_BY_NAMESPACE.getOrDefault(namespace, new String[]{"CustomMetric"});
                        String metricName = availableMetrics[random.nextInt(availableMetrics.length)];
                        
                        ObjectNode message = generateSingleMetric(resourceId, namespace, metricName);
                        sendMessage(message);
                        
                        // NO DELAYS - flood as fast as possible!
                        // Only yield CPU occasionally to prevent total system lockup
                        if (msg % 100 == 0) {
                            Thread.yield();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        executor.shutdown();
        
        long actualDuration = System.currentTimeMillis() - startTime;
        double actualRate = messagesSent.get() / Math.max(actualDuration / 1000.0, 0.1);
        
        System.out.printf("🌊 FLOOD COMPLETED: %d messages sent in %.1f seconds (%.0f msg/s)%n", 
                messagesSent.get(), actualDuration / 1000.0, actualRate);
    }
    
    /**
     * Producer worker that sends messages at specified rate
     */
    public void producerWorker(int messagesPerSecond, int durationSeconds, int workerId) {
        System.out.printf("🚀 Worker %d started: %d msg/s for %d seconds%n", 
                workerId, messagesPerSecond, durationSeconds);
        
        // Improved rate limiting - batch messages for better performance
        long intervalMillis = messagesPerSecond > 0 ? 1000L / messagesPerSecond : 1000L;
        long endTime = System.currentTimeMillis() + (durationSeconds * 1000L);
        long lastReportTime = System.currentTimeMillis();
        long lastSendTime = System.currentTimeMillis();
        
        while (running.get() && System.currentTimeMillis() < endTime) {
            long currentTime = System.currentTimeMillis();
            
            // Send message immediately, control rate with sleep
            if (resourceIds.isEmpty()) {
                // Fallback to simple generation
                ObjectNode message = generateSingleMetric(generateResourceId(), "oci_computeagent", "CpuUtilization");
                sendMessage(message);
            } else {
                // Realistic customer scenario - pick random resource and generate its metrics
                String randomResourceId = resourceIds.get(random.nextInt(resourceIds.size()));
                String namespace = resourceToNamespace.get(randomResourceId);
                
                // For high rates, send just one metric instead of batch to avoid overload
                if (messagesPerSecond > 200) {
                    String[] availableMetrics = METRICS_BY_NAMESPACE.get(namespace);
                    if (availableMetrics != null && availableMetrics.length > 0) {
                        String metricName = availableMetrics[random.nextInt(availableMetrics.length)];
                        ObjectNode metric = generateSingleMetric(randomResourceId, namespace, metricName);
                        sendMessage(metric);
                    }
                } else {
                    // For lower rates, send metric batches
                    List<ObjectNode> metrics = generateResourceMetricBatch(randomResourceId, namespace);
                    for (ObjectNode metric : metrics) {
                        sendMessage(metric);
                    }
                }
            }
            
            // Report progress every 5 seconds
            if (currentTime - lastReportTime >= 5000) {
                double rate = messagesSent.get() / ((currentTime - startTime) / 1000.0);
                System.out.printf("📊 Worker %d - Sent: %d, Failed: %d, Rate: %.1f msg/s%n", 
                        workerId, messagesSent.get(), messagesFailed.get(), rate);
                lastReportTime = currentTime;
            }
            
            // Improved rate control - only sleep if we're ahead of schedule
            long expectedTime = lastSendTime + intervalMillis;
            if (currentTime < expectedTime) {
                try {
                    Thread.sleep(expectedTime - currentTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            lastSendTime = Math.max(currentTime, expectedTime);
        }
        
        System.out.printf("✅ Worker %d completed%n", workerId);
    }
    
    /**
     * Run load test with specified parameters
     */
    public void runLoadTest(int totalRate, int duration, int numWorkers) {
        System.out.println("\n🎯 Starting Load Test");
        System.out.printf("📈 Target Rate: %d messages/second%n", totalRate);
        System.out.printf("⏱️  Duration: %d seconds%n", duration);
        System.out.printf("👥 Workers: %d%n", numWorkers);
        System.out.printf("🎯 Stream: %s%n", streamId);
        System.out.println("-".repeat(60));
        
        running.set(true);
        startTime = System.currentTimeMillis();
        
        // Calculate rate per worker
        int ratePerWorker = totalRate / numWorkers;
        
        // Start worker threads
        ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
        List<Future<?>> futures = new ArrayList<>();
        
        for (int i = 0; i < numWorkers; i++) {
            final int workerId = i + 1;
            Future<?> future = executor.submit(() -> 
                producerWorker(ratePerWorker, duration, workerId));
            futures.add(future);
        }
        
        // Set up shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n🛑 Shutdown signal received...");
            running.set(false);
            executor.shutdownNow();
        }));
        
        // Wait for completion
        try {
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("❌ Worker execution error: " + e.getMessage());
            running.set(false);
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }
        
        printFinalStats();
    }
    
    /**
     * Print final test statistics
     */
    private void printFinalStats() {
        long duration = System.currentTimeMillis() - startTime;
        double actualRate = messagesSent.get() / (duration / 1000.0);
        long totalMessages = messagesSent.get() + messagesFailed.get();
        double successRate = totalMessages > 0 ? (messagesSent.get() * 100.0 / totalMessages) : 0;
        
        System.out.println("\n📊 LOAD TEST RESULTS");
        System.out.println("=".repeat(50));
        System.out.printf("⏱️  Duration: %.2f seconds%n", duration / 1000.0);
        System.out.printf("✅ Messages Sent: %d%n", messagesSent.get());
        System.out.printf("❌ Messages Failed: %d%n", messagesFailed.get());
        System.out.printf("📊 Actual Rate: %.2f messages/second%n", actualRate);
        System.out.printf("💾 Data Sent: %.2f KB%n", bytesSent.get() / 1024.0);
        System.out.printf("📈 Success Rate: %.1f%%%n", successRate);
        
        if (!errors.isEmpty()) {
            System.out.printf("%n❌ Recent Errors (%d):%n", errors.size());
            int errorCount = Math.min(5, errors.size());
            for (int i = errors.size() - errorCount; i < errors.size(); i++) {
                System.out.println("   • " + errors.get(i));
            }
        }
        
        System.out.println("=".repeat(50));
    }
    
    /**
     * Print total session statistics
     */
    public void printTotalStats() {
        // Add current counts to totals
        long finalTotalSent = totalMessagesSent.get() + messagesSent.get();
        long finalTotalFailed = totalMessagesFailed.get() + messagesFailed.get();
        long grandTotal = finalTotalSent + finalTotalFailed;
        
        System.out.println("\n🎯 TOTAL SESSION STATISTICS");
        System.out.println("=".repeat(60));
        System.out.printf("📊 TOTAL Messages Sent: %,d%n", finalTotalSent);
        System.out.printf("❌ TOTAL Messages Failed: %,d%n", finalTotalFailed);
        System.out.printf("📈 GRAND TOTAL Messages: %,d%n", grandTotal);
        System.out.printf("✅ Overall Success Rate: %.1f%%%n", 
                grandTotal > 0 ? (finalTotalSent * 100.0 / grandTotal) : 0);
        System.out.println("=".repeat(60));
    }
    
    /**
     * Main method for running the producer
     */
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("🚀 OCI Streaming Load Test Producer");
            System.out.println("");
            System.out.println("Usage: java OCIStreamProducer [options]");
            System.out.println("");
            System.out.println("Required:");
            System.out.println("  --stream-id <OCID>        OCI Stream OCID");
            System.out.println("");
            System.out.println("Optional:");
            System.out.println("  --endpoint <URL>          OCI Streaming endpoint (auto-discovered if not provided)");
            System.out.println("  --rate <int>              Messages per second (default: 10)");
            System.out.println("  --duration <int>          Test duration in seconds (default: 60)");
            System.out.println("  --workers <int>           Number of worker threads (default: 2)");
            System.out.println("  --config <path>           OCI config file path (default: ~/.oci/config)");
            System.out.println("  --profile <name>          OCI config profile (default: DEFAULT)");
            System.out.println("  --scenario <name>         Predefined scenario: baseline|stress|spike");
            System.out.println("");
            System.out.println("Examples:");
            System.out.println("  # Basic test");
            System.out.println("  java OCIStreamProducer --stream-id ocid1.stream.oc1.us-ashburn-1.xxx --rate 50");
            System.out.println("");
            System.out.println("  # Stress test");
            System.out.println("  java OCIStreamProducer --stream-id ocid1.stream.oc1.us-ashburn-1.xxx --scenario stress");
            return;
        }
        
        // Parse arguments
        String streamId = null;
        String endpoint = null;
        int rate = 10;
        int duration = 60;
        int workers = 2;
        String configFile = System.getProperty("user.home") + "/.oci/config";
        String profile = "DEFAULT";
        String scenario = "custom";
        
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--stream-id":
                    streamId = args[++i];
                    break;
                case "--endpoint":
                    endpoint = args[++i];
                    break;
                case "--rate":
                    rate = Integer.parseInt(args[++i]);
                    break;
                case "--duration":
                    duration = Integer.parseInt(args[++i]);
                    break;
                case "--workers":
                    workers = Integer.parseInt(args[++i]);
                    break;
                case "--config":
                    configFile = args[++i];
                    break;
                case "--profile":
                    profile = args[++i];
                    break;
                case "--scenario":
                    scenario = args[++i];
                    break;
            }
        }
        
        if (streamId == null) {
            System.err.println("❌ Stream ID is required. Use --stream-id <OCID>");
            System.exit(1);
        }
        
        try {
            // Determine customer scale and create appropriate producer
            OCIStreamProducer producer;
            boolean isHighVolume = scenario.toLowerCase().contains("customer") || rate > 100;
            
            if (isHighVolume) {
                int resourceCount = 50000; // 50k resources for large customer simulation
                double metricsPerResource = 15.0; // 15 metrics per resource per minute
                producer = new OCIStreamProducer(streamId, endpoint, configFile, profile, resourceCount, metricsPerResource);
            } else {
                producer = new OCIStreamProducer(streamId, endpoint, configFile, profile);
            }
            
            // Run scenarios
            switch (scenario.toLowerCase()) {
                case "baseline":
                    System.out.println("🎯 Running Baseline Test Scenario");
                    producer.runLoadTest(10, 300, 2);
                    break;
                case "basic":
                    System.out.println("🎯 Running Basic Test Scenario (10K metrics)");
                    producer.runLoadTest(50, 200, 4);
                    break;
                case "medium":
                    System.out.println("🎯 Running Medium Test Scenario (100K metrics)");
                    producer.runLoadTest(100, 1000, 8);
                    break;
                case "high":
                    System.out.println("🎯 Running High Test Scenario (1M metrics)");
                    producer.runBatchProducer(2000, 500, 100, 16);
                    break;
                case "stress":
                    System.out.println("🎯 Running Progressive High-Volume Stress Test");
                    producer.runProgressiveLoadTest();
                    break;
                case "debug-stress":
                    System.out.println("🔧 Running Debug Stress Test (Simplified)");
                    // Simplified stress test for debugging
                    int[] debugRates = {50, 100, 200};
                    for (int i = 0; i < debugRates.length; i++) {
                        int testRate = debugRates[i];
                        System.out.printf("%n🔥 Debug Phase %d: %d msg/s with 4 workers for 1 minute%n", 
                                i + 1, testRate);
                        
                        // Reset and run
                        producer.resetStats();
                        producer.runLoadTest(testRate, 60, 4);
                        
                        System.out.printf("✅ Debug Phase %d completed%n", i + 1);
                        if (i < debugRates.length - 1) {
                            System.out.println("⏸️  Cool down (15 seconds)...");
                            Thread.sleep(15000);
                        }
                    }
                    break;
                case "ramp-up":
                    System.out.println("📈 Running RAMP-UP Test (Gradual Load Increase)");
                    
                    // Gradual ramp up from 10 to 1000 msg/s over 20 minutes
                    int[] rampRates = {10, 25, 50, 100, 200, 350, 500, 750, 1000};
                    int[] rampWorkers = {2, 3, 4, 6, 8, 12, 16, 20, 24};
                    
                    for (int i = 0; i < rampRates.length; i++) {
                        int rampRate = rampRates[i];
                        int rampWorkerCount = rampWorkers[i];
                        
                        System.out.printf("📈 Ramp Step %d: %d msg/s with %d workers for 2.5 minutes%n", 
                                i + 1, rampRate, rampWorkerCount);
                        
                        producer.resetStats();
                        producer.runLoadTest(rampRate, 150, rampWorkerCount); // 2.5 minutes per step
                        
                        System.out.printf("✅ Step %d completed (%d msg/s)%n", i + 1, rampRate);
                        
                        // Very short pause between steps (just to reset stats)
                        Thread.sleep(5000);
                    }
                    
                    System.out.println("📈 RAMP-UP Test Completed - Graph should show steady increase!");
                    producer.printTotalStats();
                    break;
                case "smooth-ramp":
                    System.out.println("📊 Running SMOOTH RAMP Test (Very Gradual Increase)");
                    
                    // Very smooth gradual increase - 30 steps over 30 minutes
                    for (int step = 1; step <= 30; step++) {
                        int smoothRate = step * 20; // 20, 40, 60, ... up to 600 msg/s
                        int smoothWorkers = Math.min(2 + (step / 3), 16); // Gradually increase workers too
                        
                        System.out.printf("📊 Smooth Step %d/30: %d msg/s with %d workers for 1 minute%n", 
                                step, smoothRate, smoothWorkers);
                        
                        producer.resetStats();
                        producer.runLoadTest(smoothRate, 60, smoothWorkers); // 1 minute per step
                        
                        // No pause - continuous ramp
                    }
                    
                    System.out.println("📊 SMOOTH RAMP Test Completed - Graph should show smooth curve!");
                    producer.printTotalStats();
                    break;
                case "wave":
                    System.out.println("🌊 Running WAVE Test (Up and Down Pattern)");
                    
                    // Wave pattern: low -> high -> low -> higher -> low
                    int[] waveRates = {20, 100, 50, 200, 75, 350, 100, 500, 150, 750, 200, 1000, 300, 800, 150, 50, 20};
                    
                    for (int i = 0; i < waveRates.length; i++) {
                        int waveRate = waveRates[i];
                        int waveWorkers = Math.min(waveRate / 25 + 2, 24);
                        
                        System.out.printf("🌊 Wave Step %d: %d msg/s for 1.5 minutes%n", i + 1, waveRate);
                        
                        producer.resetStats();
                        producer.runLoadTest(waveRate, 90, waveWorkers); // 1.5 minutes per step
                        
                        Thread.sleep(3000); // Short pause between waves
                    }
                    
                    System.out.println("🌊 WAVE Test Completed - Graph should show wave pattern!");
                    break;
                case "flood":
                    System.out.println("🌊 Running FLOOD Test (Immediate Message Flood)");
                    
                    // Phase 1: Complete silence
                    System.out.println("🔇 Phase 1: Complete silence (30 seconds)");
                    Thread.sleep(30000);
                    
                    // Phase 2: INSTANT FLOOD - Send 10,000 messages as fast as possible
                    System.out.println("🌊 Phase 2: INSTANT FLOOD (10,000 messages immediately!)");
                    producer.resetStats();
                    producer.runInstantFlood(10000);
                    System.out.println("✅ Flood completed");
                    
                    // Phase 3: Another silence
                    System.out.println("🔇 Phase 3: Silence (30 seconds)");
                    Thread.sleep(30000);
                    
                    // Phase 4: Another flood
                    System.out.println("🌊 Phase 4: SECOND FLOOD (15,000 messages immediately!)");
                    producer.resetStats();
                    producer.runInstantFlood(15000);
                    
                    System.out.println("🎉 FLOOD Test Completed - Should see massive spikes!");
                    break;
                case "burst":
                    System.out.println("💥 Running BURST Test (Force Function Scaling)");
                    
                    // Phase 1: Baseline (very low)
                    System.out.println("📊 Phase 1: Baseline (2 msg/s for 1 minute)");
                    producer.resetStats();
                    producer.runLoadTest(2, 60, 1);
                    System.out.println("✅ Baseline established");
                    
                    // Phase 2: Multiple SHORT BURSTS (this forces scaling)
                    System.out.println("💥 Phase 2: SHORT INTENSE BURSTS (force scaling)");
                    for (int burst = 1; burst <= 5; burst++) {
                        System.out.printf("🚀 Burst %d: Sending 1000 messages in 10 seconds%n", burst);
                        producer.resetStats();
                        
                        // Send 1000 messages as fast as possible in 10 seconds
                        producer.runBurstTest(1000, 10);
                        
                        System.out.printf("✅ Burst %d completed%n", burst);
                        
                        // Short pause between bursts (let functions scale)
                        if (burst < 5) {
                            System.out.println("⏸️  Pause (20 seconds - allow scaling)...");
                            Thread.sleep(20000);
                        }
                    }
                    
                    System.out.println("🎉 BURST Test Completed - Functions should have scaled!");
                    break;
                case "function-scale":
                    System.out.println("🏗️ Running FUNCTION SCALING Test");
                    
                    // Phase 1: Overwhelming burst
                    System.out.println("🌊 Phase 1: OVERWHELMING BURST (5000 msgs in 30 seconds)");
                    producer.resetStats();
                    producer.runBurstTest(5000, 30);
                    System.out.println("✅ Overwhelming burst completed");
                    
                    // Phase 2: Sustained high load (maintain scaling)
                    System.out.println("🔥 Phase 2: SUSTAINED HIGH LOAD (500 msg/s for 5 minutes)");
                    producer.resetStats();
                    producer.runLoadTest(500, 300, 16);
                    System.out.println("✅ Sustained load completed");
                    
                    // Phase 3: Gradual scale down
                    System.out.println("📉 Phase 3: GRADUAL SCALE DOWN");
                    int[] scaleDown = {200, 100, 50, 20, 5};
                    for (int i = 0; i < scaleDown.length; i++) {
                        int scaleRate = scaleDown[i];
                        System.out.printf("📉 Scale down step %d: %d msg/s for 2 minutes%n", i + 1, scaleRate);
                        producer.resetStats();
                        producer.runLoadTest(scaleRate, 120, Math.min(scaleRate / 10 + 1, 8));
                        Thread.sleep(10000); // Brief pause
                    }
                    
                    System.out.println("🎉 FUNCTION SCALING Test Completed!");
                    break;
                case "spike":
                    System.out.println("🎯 Running Aggressive Spike Test Scenario");
                    
                    // Phase 1: Low baseline (very low to create contrast)
                    System.out.println("📊 Phase 1: Low baseline (5 msg/s for 1 minute)");
                    producer.resetStats();
                    producer.runLoadTest(5, 60, 1);
                    System.out.println("✅ Baseline phase completed");
                    
                    // Cool down
                    System.out.println("⏸️  Cool down (10 seconds)...");
                    Thread.sleep(10000);
                    
                    // Phase 2: MASSIVE SPIKE (create huge contrast)
                    System.out.println("🚀 Phase 2: MASSIVE SPIKE (2000 msg/s for 2 minutes)");
                    producer.resetStats();
                    producer.runBatchProducer(2000, 120, 500, 32); // Use batch for high throughput
                    System.out.println("🔥 SPIKE phase completed");
                    
                    // Cool down
                    System.out.println("⏸️  Cool down (30 seconds)...");
                    Thread.sleep(30000);
                    
                    // Phase 3: Return to baseline
                    System.out.println("📊 Phase 3: Return to baseline (10 msg/s for 1 minute)");
                    producer.resetStats();
                    producer.runLoadTest(10, 60, 2);
                    System.out.println("✅ Return to baseline completed");
                    
                    System.out.println("🎉 Aggressive Spike Test Completed!");
                    break;
                case "ultra-spike":
                    System.out.println("🎯 Running ULTRA Spike Test (Maximum Contrast)");
                    
                    // Phase 1: Silent period (almost no traffic)
                    System.out.println("🔇 Phase 1: Silent period (1 msg/s for 30 seconds)");
                    producer.resetStats();
                    producer.runLoadTest(1, 30, 1);
                    
                    System.out.println("⏸️  Preparation pause (15 seconds)...");
                    Thread.sleep(15000);
                    
                    // Phase 2: EXPLOSIVE SPIKE
                    System.out.println("💥 Phase 2: EXPLOSIVE SPIKE (5000 msg/s for 3 minutes)");
                    producer.resetStats();
                    producer.runBatchProducer(5000, 180, 1000, 64); // Maximum throughput
                    System.out.println("🔥 EXPLOSIVE SPIKE completed");
                    
                    System.out.println("⏸️  Recovery pause (60 seconds)...");
                    Thread.sleep(60000);
                    
                    // Phase 3: Gradual ramp down
                    System.out.println("📉 Phase 3: Gradual ramp down");
                    int[] rampDown = {1000, 500, 100, 20, 5};
                    for (int i = 0; i < rampDown.length; i++) {
                        int testRate = rampDown[i];
                        System.out.printf("📉 Ramp down step %d: %d msg/s for 30 seconds%n", i + 1, testRate);
                        producer.resetStats();
                        producer.runLoadTest(testRate, 30, Math.min(testRate / 10 + 1, 16));
                        if (i < rampDown.length - 1) {
                            Thread.sleep(5000); // Brief pause between ramp down steps
                        }
                    }
                    
                    System.out.println("🎉 ULTRA Spike Test Completed!");
                    break;
                case "customer-small":
                    System.out.println("🏢 Running Small Customer Simulation (10k resources)");
                    producer = new OCIStreamProducer(streamId, endpoint, configFile, profile, 10000, 12.0);
                    producer.runHighVolumeTest(200, 600, 8, 1);
                    break;
                case "customer-medium":
                    System.out.println("🏢 Running Medium Customer Simulation (50k resources)");
                    producer = new OCIStreamProducer(streamId, endpoint, configFile, profile, 50000, 15.0);
                    producer.runHighVolumeTest(500, 900, 16, 1);
                    break;
                case "customer-large":
                    System.out.println("🏢 Running Large Customer Simulation (1 Lakh resources)");
                    producer = new OCIStreamProducer(streamId, endpoint, configFile, profile, 100000, 18.0);
                    producer.runHighVolumeTest(1000, 1200, 32, 1);
                    break;
                case "customer-enterprise":
                    System.out.println("🏢 Running Enterprise Customer Simulation (5 Lakh resources)");
                    producer = new OCIStreamProducer(streamId, endpoint, configFile, profile, 500000, 20.0);
                    producer.runHighVolumeTest(2000, 1800, 64, 1);
                    break;
                case "metrics-1lakh":
                    System.out.println("📊 Running 1 Lakh Metrics Load Test");
                    producer.runBatchProducer(1000, 100, 50, 8);
                    break;
                case "metrics-5lakh":
                    System.out.println("📊 Running 5 Lakh Metrics Load Test");
                    producer.runBatchProducer(2500, 200, 100, 16);
                    break;
                case "metrics-10lakh":
                    System.out.println("📊 Running 10 Lakh Metrics Load Test");
                    producer.runBatchProducer(5000, 200, 100, 32);
                    break;
                case "metrics-50lakh":
                    System.out.println("📊 Running 50 Lakh Metrics Benchmark Test");
                    producer.runBatchProducer(10000, 500, 200, 64);
                    break;
                case "benchmark":
                    System.out.println("🏆 Running Ultimate Benchmark Test (1 Crore metrics in 10 minutes)");
                    producer.runBatchProducer(16667, 600, 500, 128);
                    break;
                case "multiply-1cr":
                    System.out.println("🔢 Running Multiplied Resource Test (1 Crore metrics - 1000 resources × 50 metrics × 200x)");
                    producer.runMultipliedResourceTest(1000, 50, 200, 600, 100, 32);
                    break;
                case "multiply-5cr":
                    System.out.println("🔢 Running Multiplied Resource Test (5 Crore metrics - 1000 resources × 50 metrics × 1000x)");
                    producer.runMultipliedResourceTest(1000, 50, 1000, 900, 200, 64);
                    break;
                case "multiply-10cr":
                    System.out.println("🔢 Running Multiplied Resource Test (10 Crore metrics - 2000 resources × 50 metrics × 1000x)");
                    producer.runMultipliedResourceTest(2000, 50, 1000, 1200, 500, 128);
                    break;
                default:
                    if (isHighVolume) {
                        producer.runHighVolumeTest(rate, duration, workers, 1);
                    } else {
                        producer.runLoadTest(rate, duration, workers);
                    }
                    break;
            }
            
        } catch (Exception e) {
            System.err.println("❌ Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}