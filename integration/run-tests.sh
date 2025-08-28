#!/bin/bash

# Integration test runner for tail sampling processor
# Usage: ./run-tests.sh [scenario] [options]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Default values
SCENARIO="${1:-all}"
TIMEOUT="${2:-300}"
VERBOSE="${VERBOSE:-false}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."
    
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    if ! docker compose version &> /dev/null; then
        error "Docker Compose v2+ is required"
        exit 1
    fi
    
    # Check available memory
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        MEM_GB=$(free -g | awk '/^Mem:/{print $7}')
        if [[ $MEM_GB -lt 4 ]]; then
            warn "Less than 4GB available memory detected. Tests may fail."
        fi
    fi
    
    success "Prerequisites check passed"
}

# Run a specific test scenario
run_scenario() {
    local scenario=$1
    local scenario_dir="$SCRIPT_DIR/$scenario"
    
    if [[ ! -d "$scenario_dir" ]]; then
        error "Scenario directory not found: $scenario_dir"
        return 1
    fi
    
    log "Running scenario: $scenario"
    
    cd "$scenario_dir"
    
    # Clean up any existing containers
    docker compose down -v --remove-orphans 2>/dev/null || true
    
    # Start services
    log "Starting services for $scenario..."
    if [[ "$VERBOSE" == "true" ]]; then
        docker compose up --build --timeout "$TIMEOUT"
    else
        docker compose up --build --timeout "$TIMEOUT" -d
        
        # Wait for services to be ready
        log "Waiting for services to be ready..."
        sleep 30
        
        # Check service health
        if docker compose ps | grep -q "unhealthy\|exited"; then
            error "Some services failed to start properly"
            docker compose logs
            return 1
        fi
        
        # Wait for test completion or timeout
        log "Running tests (timeout: ${TIMEOUT}s)..."
        sleep "$TIMEOUT"
        
        # Collect results
        log "Collecting test results for $scenario..."
        docker compose logs otel-collector > "test-results-$scenario.log" 2>&1
        
        # Clean up
        docker compose down -v --remove-orphans
    fi
    
    success "Scenario $scenario completed"
    cd "$SCRIPT_DIR"
}

# Validate test results
validate_results() {
    local scenario=$1
    local log_file="$scenario/test-results-$scenario.log"
    
    if [[ ! -f "$log_file" ]]; then
        warn "No log file found for $scenario"
        return 1
    fi
    
    log "Validating results for $scenario..."
    
    # Check for critical errors
    if grep -q "panic\|fatal\|FATAL" "$log_file"; then
        error "Critical errors found in $scenario logs"
        return 1
    fi
    
    # Check for sampling decisions
    if ! grep -q "sampling decision" "$log_file"; then
        warn "No sampling decisions found in $scenario logs"
    fi
    
    # Check for spillover activity (if applicable)
    if [[ "$scenario" != "object-only" ]] && ! grep -q "spill" "$log_file"; then
        warn "No spillover activity detected in $scenario"
    fi
    
    success "Results validation passed for $scenario"
}

# Main execution
main() {
    log "Starting tail sampling integration tests"
    log "Scenario: $SCENARIO, Timeout: ${TIMEOUT}s"
    
    check_prerequisites
    
    case "$SCENARIO" in
        # Foundation scenarios
        "redis-gcs")
            run_scenario "redis-gcs"
            validate_results "redis-gcs"
            ;;
        "redis-s3")
            run_scenario "redis-s3"
            validate_results "redis-s3"
            ;;
        "object-only")
            run_scenario "object-only"
            validate_results "object-only"
            ;;
        
        # Advanced Phase 4.4 scenarios
        "large-traces")
            run_scenario "large-traces"
            validate_results "large-traces"
            ;;
        "complex-policies")
            run_scenario "complex-policies"
            validate_results "complex-policies"
            ;;
        "performance-optimised")
            run_scenario "performance-optimised"
            validate_results "performance-optimised"
            ;;
        "minimal-memory")
            run_scenario "minimal-memory"
            validate_results "minimal-memory"
            ;;
        
        "foundation")
            log "Running foundation test scenarios..."
            
            run_scenario "object-only"
            validate_results "object-only"
            
            run_scenario "redis-s3"
            validate_results "redis-s3"
            
            run_scenario "redis-gcs"
            validate_results "redis-gcs"
            
            log "Foundation scenarios completed"
            ;;
        
        "advanced")
            log "Running advanced Phase 4.4 test scenarios..."
            
            run_scenario "large-traces"
            validate_results "large-traces"
            
            run_scenario "complex-policies"
            validate_results "complex-policies"
            
            run_scenario "performance-optimised"
            validate_results "performance-optimised"
            
            run_scenario "minimal-memory"
            validate_results "minimal-memory"
            
            log "Advanced scenarios completed"
            ;;
        
        "all")
            log "Running all test scenarios (foundation + advanced)..."
            
            # Foundation tests first
            run_scenario "object-only"
            validate_results "object-only"
            
            run_scenario "redis-s3"
            validate_results "redis-s3"
            
            run_scenario "redis-gcs"
            validate_results "redis-gcs"
            
            # Advanced Phase 4.4 tests
            run_scenario "large-traces"
            validate_results "large-traces"
            
            run_scenario "complex-policies"
            validate_results "complex-policies"
            
            run_scenario "performance-optimised"
            validate_results "performance-optimised"
            
            run_scenario "minimal-memory"
            validate_results "minimal-memory"
            
            log "All scenarios completed"
            ;;
        *)
            error "Unknown scenario: $SCENARIO"
            echo "Available scenarios:"
            echo "  Foundation: redis-gcs, redis-s3, object-only"
            echo "  Advanced:   large-traces, complex-policies, performance-optimised, minimal-memory"
            echo "  Groups:     foundation, advanced, all"
            exit 1
            ;;
    esac
    
    success "Integration tests completed successfully"
}

# Handle script interruption
trap 'error "Tests interrupted"; exit 130' INT TERM

# Show help
if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    cat << EOF
Tail Sampling Processor Integration Test Runner

Usage: $0 [scenario] [timeout]

Foundation Scenarios:
  redis-gcs     Test Redis coordination with GCS spillover
  redis-s3      Test Redis coordination with S3 spillover  
  object-only   Test object storage only (no Redis)

Advanced Scenarios (Phase 4.4):
  large-traces          Test streaming evaluation for >10K span traces
  complex-policies      Test meta-policies (AND, Composite, Drop)
  performance-optimised Test high-throughput optimised settings
  minimal-memory        Test severe memory constraints (256MB)

Groups:
  foundation    Run all foundation tests
  advanced      Run all Phase 4.4 tests
  all           Run all scenarios (default)

Options:
  timeout       Test timeout in seconds (default: 300)
  
Environment Variables:
  VERBOSE=true  Show detailed docker compose output

Examples:
  $0 redis-gcs 600      # Run Redis+GCS test with 10min timeout
  $0 object-only        # Run object-only test with default timeout
  VERBOSE=true $0 all   # Run all tests with verbose output

EOF
    exit 0
fi

# Run main function
main "$@"