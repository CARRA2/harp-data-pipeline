#!/usr/bin/env bash

# examples_submit_ecf_selected.sh - Example usage patterns for submit_ecf_selected.sh
#
# This script contains common usage examples for the selective ECF submission script.
# You can run these examples directly or use them as templates for your own submissions.

# Set the base script path
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SUBMIT_SCRIPT="$SCRIPT_DIR/submit_ecf_selected.sh"

# Check if the main script exists
if [ ! -f "$SUBMIT_SCRIPT" ]; then
    echo "Error: submit_ecf_selected.sh not found at $SUBMIT_SCRIPT"
    exit 1
fi

echo "=== ECF Selected Submission Examples ==="
echo "Script location: $SUBMIT_SCRIPT"
echo ""

# Function to run example with confirmation
run_example() {
    local description="$1"
    local command="$2"
    
    echo "Example: $description"
    echo "Command: $command"
    echo ""
    
    read -p "Run this example? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Running: $command"
        eval "$command"
        echo ""
        echo "--- Example completed ---"
    else
        echo "Skipped."
    fi
    echo ""
}

# Function to show example without running
show_example() {
    local description="$1"
    local command="$2"
    
    echo "Example: $description"
    echo "Command: $command"
    echo ""
}

# Display usage first
echo "First, let's see the help:"
$SUBMIT_SCRIPT --help
echo ""
echo "Press Enter to continue with examples..."
read

echo "=== COMMON USAGE EXAMPLES ==="
echo ""

# Example 1: Basic daily sums and monthly means
show_example "Basic: Daily sums and monthly means for January 2023" \
    "$SUBMIT_SCRIPT -p 202301 --daily-sums --monthly-sums"

# Example 2: Custom parameters for daily sums
show_example "Custom parameters: Daily sums with specific accumulated parameters" \
    "$SUBMIT_SCRIPT -p 202301 --daily-sums --params-acc \"47,146,169,175,176\""

# Example 3: Surface analysis with custom parameters
show_example "Surface analysis: Daily means with custom surface parameters" \
    "$SUBMIT_SCRIPT -p 202301 --daily-means-an --params-sfc \"31,34,78,79,134,151\""

# Example 4: Multiple level types with custom parameters
show_example "Multiple levels: Daily means for surface and pressure levels" \
    "$SUBMIT_SCRIPT -p 202301 --daily-means-an --params-sfc \"31,34,78\" --params-pl \"60,75,76,129\""

# Example 5: Scheduled processing
show_example "Scheduled: Process at 23:50 with custom suite name" \
    "$SUBMIT_SCRIPT -p 202301 --daily-sums --monthly-sums -t 2350 -n \"my_january_run\""

# Example 6: High parallelization
show_example "High parallelization: Use 8 batches for faster processing" \
    "$SUBMIT_SCRIPT -p 202301 --daily-sums -b 8"

# Example 7: Comprehensive processing
show_example "Comprehensive: All processing types with default parameters" \
    "$SUBMIT_SCRIPT -p 202301 --daily-sums --monthly-sums --daily-means-an --monthly-means-an --daily-minmax --monthly-minmax"

# Example 8: Minimal precipitation processing
show_example "Precipitation focus: Only precipitation-related parameters" \
    "$SUBMIT_SCRIPT -p 202301 --daily-sums --monthly-sums --params-acc \"228228,47,146,169\""

# Example 9: Temperature and humidity focus
show_example "Temperature/Humidity: Focus on temperature and humidity parameters" \
    "$SUBMIT_SCRIPT -p 202301 --daily-means-an --params-sfc \"167,168\" --params-pl \"130,133\" --params-ml \"130,133\""

# Example 10: Force replace existing suite
show_example "Force replace: Replace existing suite with same name" \
    "$SUBMIT_SCRIPT -p 202301 --daily-sums --force"

echo "=== INTERACTIVE EXAMPLES ==="
echo "The following examples can be run interactively:"
echo ""

# Interactive examples that can actually be run
run_example "Test run: Daily sums for a small parameter set (safe for testing)" \
    "$SUBMIT_SCRIPT -p 202301 --daily-sums --params-acc \"47,146\" -n \"test_run\" --force"

run_example "Quick analysis: Daily means for basic surface parameters" \
    "$SUBMIT_SCRIPT -p 202301 --daily-means-an --params-sfc \"31,34,78,79\" -n \"quick_analysis\" --force"

echo "=== PARAMETER REFERENCE ==="
echo ""
echo "Common parameter groups from env.sh:"
echo ""
echo "Surface Analysis (CARRA_PAR_AN_SFC):"
echo "  31  - Sea ice cover"
echo "  34  - Sea surface temperature"
echo "  78  - Total column liquid water"
echo "  79  - Total column ice water"
echo "  134 - Surface pressure"
echo "  151 - Mean sea level pressure"
echo "  165 - 10m u-component of wind"
echo "  166 - 10m v-component of wind"
echo "  167 - 2m temperature"
echo ""
echo "Accumulated Parameters (CARRA_PAR_FC_ACC):"
echo "  47    - Total precipitation"
echo "  146   - Surface sensible heat flux"
echo "  169   - Surface solar radiation downwards"
echo "  175   - Surface thermal radiation downwards"
echo "  176   - Surface solar radiation net"
echo "  177   - Surface thermal radiation net"
echo "  228228 - Total precipitation rate"
echo ""
echo "Pressure Level Analysis (CARRA_PAR_AN_PL):"
echo "  60  - Potential vorticity"
echo "  75  - Specific humidity"
echo "  76  - Relative humidity"
echo "  129 - Geopotential"
echo "  130 - Temperature"
echo "  157 - Relative humidity"
echo ""
echo "=== TIPS FOR USAGE ==="
echo ""
echo "1. Start with small parameter sets for testing"
echo "2. Use --force when developing/testing to replace existing suites"
echo "3. Monitor ECF server for job progress and resource usage"
echo "4. Increase --nbatch for large parameter sets to improve parallelization"
echo "5. Use scheduled runs (-t option) for off-peak processing"
echo "6. Custom suite names help organize multiple runs"
echo ""
echo "=== TROUBLESHOOTING ==="
echo ""
echo "Common issues and solutions:"
echo ""
echo "1. 'env.sh not found' error:"
echo "   - Run the script from the bin/ directory where env.sh is located"
echo ""
echo "2. 'ecfproj_start not found' error:"
echo "   - Ensure you're in the correct directory with ecfproj_start"
echo ""
echo "3. ECF suite already exists:"
echo "   - Use --force flag to replace existing suite"
echo "   - Or use a different suite name with -n option"
echo ""
echo "4. Parameter format errors:"
echo "   - Use comma-separated (31,34,78) or slash-separated (31/34/78)"
echo "   - Check parameter IDs against env.sh file"
echo ""
echo "5. Memory/resource issues:"
echo "   - Reduce parameter count or increase --nbatch value"
echo "   - Check system configuration (-c option)"
echo ""

echo "=== END OF EXAMPLES ==="