#!/bin/bash
set -e

echo "======================================"
echo "EM-√ COMPREHENSIVE TEST SUITE"
echo "======================================"
echo ""

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

FAILED_TESTS=()
PASSED_TESTS=()

run_test_suite() {
    local name=$1
    local command=$2
    
    echo -e "${YELLOW}Running: $name${NC}"
    if eval "$command" > /tmp/test_output.log 2>&1; then
        echo -e "${GREEN}✓ $name PASSED${NC}"
        echo ""
        PASSED_TESTS+=("$name")
        return 0
    else
        echo -e "${RED}✗ $name FAILED${NC}"
        echo -e "${RED}Last 20 lines of output:${NC}"
        tail -20 /tmp/test_output.log
        echo ""
        FAILED_TESTS+=("$name")
        return 1
    fi
}

# Print header
echo -e "${BLUE}Running with workspace: $(pwd)${NC}"
echo ""

# 1. Unit Tests
echo "======== PHASE 1: UNIT TESTS ========"
run_test_suite "SpillManager Tests" "cargo test --test spill_manager_tests --no-default-features"
run_test_suite "RowBatch Helper Tests" "cargo test --test rowbatch_helpers_tests --no-default-features"
run_test_suite "Memory Budget Tests" "cargo test --test memory_budget_tests --no-default-features"

# 2. Integration Tests
echo "======== PHASE 2: INTEGRATION TESTS ========"
run_test_suite "Full Pipeline Tests" "cargo test --test integration_tests --no-default-features"

# 3. Existing E2E Tests
echo "======== PHASE 3: E2E SMOKE TESTS ========"
run_test_suite "E2E Smoke Test" "cargo test --test e2e_smoke --no-default-features"

# 4. All other unit tests in crates
echo "======== PHASE 4: CRATE-LEVEL TESTS ========"
run_test_suite "All Crate Tests" "cargo test --all --no-default-features --lib"

# Summary
echo ""
echo "======================================"
echo "TEST SUMMARY"
echo "======================================"
echo ""

echo -e "${BLUE}Tests Passed: ${#PASSED_TESTS[@]}${NC}"
for test in "${PASSED_TESTS[@]}"; do
    echo -e "${GREEN}  ✓ $test${NC}"
done

echo ""

if [ ${#FAILED_TESTS[@]} -eq 0 ]; then
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}ALL TESTS PASSED! ✓${NC}"
    echo -e "${GREEN}========================================${NC}"
    exit 0
else
    echo -e "${RED}Tests Failed: ${#FAILED_TESTS[@]}${NC}"
    for test in "${FAILED_TESTS[@]}"; do
        echo -e "${RED}  ✗ $test${NC}"
    done
    echo ""
    echo -e "${RED}========================================${NC}"
    echo -e "${RED}SOME TESTS FAILED${NC}"
    echo -e "${RED}========================================${NC}"
    exit 1
fi

