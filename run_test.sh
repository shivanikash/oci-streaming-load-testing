#!/bin/bash

# OCI Streaming Load Test Runner
# Simplified runner script for the Java-based OCI Stream Producer

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

print_header() {
    echo -e "${BLUE}"
    echo "=================================================="
    echo "    OCI Streaming Load Test Runner"
    echo "=================================================="
    echo -e "${NC}"
}

print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Required:"
    echo "  --stream-id <OCID>        OCI Stream OCID"
    echo ""
    echo "Optional:"
    echo "  --rate <int>              Messages per second (default: 10)"
    echo "  --duration <int>          Test duration in seconds (default: 60)"
    echo "  --workers <int>           Number of worker threads (default: 2)"
    echo "  --scenario <name>         Predefined scenario:"
    echo "                            baseline|stress|spike"
    echo "                            metrics-1lakh|metrics-5lakh|metrics-10lakh"
    echo "                            metrics-50lakh|benchmark"
    echo "                            multiply-1cr|multiply-5cr|multiply-10cr"
    echo "  --profile <name>          OCI config profile (default: DEFAULT)"
    echo "  --build                   Build the project before running"
    echo ""
    echo "High-Volume Scenarios:"
    echo "  metrics-1lakh             1 Lakh metrics in ~100 seconds"
    echo "  metrics-5lakh             5 Lakh metrics in ~200 seconds"
    echo "  metrics-10lakh            10 Lakh metrics in ~200 seconds"
    echo "  metrics-50lakh            50 Lakh metrics in ~500 seconds"
    echo "  benchmark                 1 Crore metrics in 10 minutes"
    echo ""
    echo "Resource Multiplication Scenarios:"
    echo "  multiply-1cr              1 Crore (1000 resources × 50 metrics × 200x)"
    echo "  multiply-5cr              5 Crore (1000 resources × 50 metrics × 1000x)"
    echo "  multiply-10cr             10 Crore (2000 resources × 50 metrics × 1000x)"
    echo ""
    echo "Examples:"
    echo "  $0 --stream-id ocid1.stream.oc1.us-ashburn-1.xxx --rate 50"
    echo "  $0 --stream-id ocid1.stream.oc1.us-ashburn-1.xxx --scenario stress"
    echo "  $0 --stream-id ocid1.stream.oc1.us-ashburn-1.xxx --scenario metrics-1lakh"
    echo "  $0 --stream-id ocid1.stream.oc1.us-ashburn-1.xxx --scenario multiply-1cr"
    echo "  $0 --stream-id ocid1.stream.oc1.us-ashburn-1.xxx --scenario benchmark"
}

# Default values
STREAM_ID=""
RATE=10
DURATION=60
WORKERS=2
SCENARIO="custom"
PROFILE="DEFAULT"
BUILD=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --stream-id)
            STREAM_ID="$2"
            shift 2
            ;;
        --rate)
            RATE="$2"
            shift 2
            ;;
        --duration)
            DURATION="$2"
            shift 2
            ;;
        --workers)
            WORKERS="$2"
            shift 2
            ;;
        --scenario)
            SCENARIO="$2"
            shift 2
            ;;
        --profile)
            PROFILE="$2"
            shift 2
            ;;
        --build)
            BUILD=true
            shift
            ;;
        --help|-h)
            print_usage
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            print_usage
            exit 1
            ;;
    esac
done

# Validate required parameters
if [[ -z "$STREAM_ID" ]]; then
    echo -e "${RED}❌ Stream ID is required${NC}"
    print_usage
    exit 1
fi

print_header

# Check if Maven is installed
if ! command -v mvn &> /dev/null; then
    echo -e "${RED}❌ Maven is not installed${NC}"
    echo "Please install Maven: https://maven.apache.org/install.html"
    exit 1
fi

# Check if Java 11+ is available
if ! command -v java &> /dev/null; then
    echo -e "${RED}❌ Java is not installed${NC}"
    echo "Please install Java 11 or higher"
    exit 1
fi

JAVA_VERSION=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | sed '/^1\./s///' | cut -d'.' -f1)
if [[ "$JAVA_VERSION" -lt 11 ]]; then
    echo -e "${RED}❌ Java 11 or higher is required (found Java $JAVA_VERSION)${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Java $JAVA_VERSION found${NC}"

# Build if requested or if JAR doesn't exist
JAR_FILE="target/oci-stream-producer-1.0.0.jar"
if [[ "$BUILD" == true ]] || [[ ! -f "$JAR_FILE" ]]; then
    echo -e "${YELLOW}🔨 Building project...${NC}"
    mvn clean package -q
    
    if [[ $? -eq 0 ]]; then
        echo -e "${GREEN}✅ Build successful${NC}"
    else
        echo -e "${RED}❌ Build failed${NC}"
        exit 1
    fi
fi

# Prepare Java arguments
JAVA_ARGS=(
    "-jar" "$JAR_FILE"
    "--stream-id" "$STREAM_ID"
    "--profile" "$PROFILE"
)

# Add scenario-specific or custom arguments
if [[ "$SCENARIO" != "custom" ]]; then
    JAVA_ARGS+=("--scenario" "$SCENARIO")
else
    JAVA_ARGS+=("--rate" "$RATE")
    JAVA_ARGS+=("--duration" "$DURATION")
    JAVA_ARGS+=("--workers" "$WORKERS")
fi

# Display configuration
echo -e "${BLUE}📋 Configuration:${NC}"
echo "  Stream ID: $STREAM_ID"
echo "  Profile: $PROFILE"
if [[ "$SCENARIO" != "custom" ]]; then
    echo "  Scenario: $SCENARIO"
else
    echo "  Rate: $RATE messages/second"
    echo "  Duration: $DURATION seconds"
    echo "  Workers: $WORKERS"
fi
echo ""

# Run the load test
echo -e "${YELLOW}🚀 Starting load test...${NC}"
java "${JAVA_ARGS[@]}"

echo -e "${GREEN}✅ Load test completed${NC}"