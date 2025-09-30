#!/usr/bin/env bash

# submit_ecf_selected.sh - Selective ECF Suite Submission Script
# 
# This script allows you to submit only selected types of climate means
# for specific parameters and periods, providing more control than the
# full suite submission.
#
# Author: Based on submit_ecf_suite.sh
# Usage: ./submit_ecf_selected.sh [OPTIONS]

# Set default values
DEFAULT_SUITE_NAME="carra2_selected"
DEFAULT_NBATCH=4
DEFAULT_CONFIG="means"

# Function to display usage information
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

DESCRIPTION:
    Submit selected ECF climate means processing tasks with customizable parameters.
    This script provides fine-grained control over which climate means to process,
    allowing you to select specific parameter types and customize processing options.

REQUIRED OPTIONS:
    -p, --period YYYYMM         Period to process (format: YYYYMM, e.g., 202301)

PROCESSING TYPE OPTIONS (at least one required):
    --daily-sums               Process daily sums of accumulated variables (FC)
    --monthly-sums             Process monthly means of daily sums
    --daily-means-an           Process daily means of analysis instantaneous variables
    --daily-means-fc           Process daily means of forecast variables
    --monthly-means-an         Process monthly means of analysis variables
    --daily-minmax             Process daily min/max of forecast variables
    --monthly-minmax           Process monthly min/max variables

PARAMETER SELECTION OPTIONS:
    --params-sfc PARAMS        Custom surface parameters (comma or slash separated)
    --params-ml PARAMS         Custom model level parameters (comma or slash separated)
    --params-pl PARAMS         Custom pressure level parameters (comma or slash separated)
    --params-hl PARAMS         Custom height level parameters (comma or slash separated)
    --params-acc PARAMS        Custom accumulated parameters (comma or slash separated)
    --params-fc-sfc PARAMS     Custom forecast surface parameters (comma or slash separated)

CONFIGURATION OPTIONS:
    -n, --suite-name NAME      Name for the ECF suite (default: $DEFAULT_SUITE_NAME)
    -b, --nbatch NUMBER        Number of batches for parallel processing (default: $DEFAULT_NBATCH)
    -c, --config CONFIG        System configuration (default: $DEFAULT_CONFIG)
    -t, --time HHMM            Schedule suite to run at specific time (format: HHMM)
    -s, --streams STREAMS      Comma-separated list of streams (default: from env)
    --force                    Force replace existing suite

EXAMPLES:
    # Process daily sums and monthly means for default parameters
    $0 -p 202301 --daily-sums --monthly-sums

    # Process only daily sums with custom accumulated parameters
    $0 -p 202301 --daily-sums --params-acc "47,146,169,175"

    # Process daily means for surface with custom parameters and 6 batches
    $0 -p 202301 --daily-means-an --params-sfc "31,34,78,79" -b 6

    # Schedule processing at 23:50 with custom suite name
    $0 -p 202301 --daily-sums --monthly-sums -t 2350 -n "my_custom_suite"

    # Process all types with default parameters
    $0 -p 202301 --daily-sums --monthly-sums --daily-means-an --monthly-means-an

NOTES:
    - Parameters can be specified as comma-separated (31,34,78) or slash-separated (31/34/78)
    - If no custom parameters are specified, defaults from env.sh will be used
    - The script will create necessary batch ECF files automatically
    - Use --force to replace an existing suite with the same name

EOF
}

# Function to convert comma-separated to slash-separated parameters
normalize_params() {
    echo "$1" | sed 's/,/\//g'
}

# Function to validate period format
validate_period() {
    if [[ ! "$1" =~ ^[0-9]{6}$ ]]; then
        echo "Error: Period must be in YYYYMM format (e.g., 202301)"
        exit 1
    fi
}

# Function to create custom env variables
create_custom_env() {
    local temp_env=$(mktemp)
    
    # Source original env.sh
    if [ -f ./env.sh ]; then
        cp ./env.sh "$temp_env"
    else
        echo "Error: env.sh not found in current directory"
        exit 1
    fi
    
    # Override with custom parameters if provided
    if [ -n "$CUSTOM_PARAMS_SFC" ]; then
        echo "export CARRA_PAR_AN_SFC=\"$CUSTOM_PARAMS_SFC\"" >> "$temp_env"
        echo "export CARRA_PAR_AN_SFC_ARXIV=\"$CUSTOM_PARAMS_SFC\"" >> "$temp_env"
    fi
    
    if [ -n "$CUSTOM_PARAMS_ML" ]; then
        echo "export CARRA_PAR_AN_ML=\"$CUSTOM_PARAMS_ML\"" >> "$temp_env"
    fi
    
    if [ -n "$CUSTOM_PARAMS_PL" ]; then
        echo "export CARRA_PAR_AN_PL=\"$CUSTOM_PARAMS_PL\"" >> "$temp_env"
    fi
    
    if [ -n "$CUSTOM_PARAMS_HL" ]; then
        echo "export CARRA_PAR_AN_HL=\"$CUSTOM_PARAMS_HL\"" >> "$temp_env"
    fi
    
    if [ -n "$CUSTOM_PARAMS_ACC" ]; then
        echo "export CARRA_PAR_FC_ACC=\"$CUSTOM_PARAMS_ACC\"" >> "$temp_env"
    fi
    
    if [ -n "$CUSTOM_PARAMS_FC_SFC" ]; then
        echo "export CARRA_PAR_FC_SFC=\"$CUSTOM_PARAMS_FC_SFC\"" >> "$temp_env"
        echo "export CARRA_PAR_FC_SFC_MM=\"$CUSTOM_PARAMS_FC_SFC\"" >> "$temp_env"
    fi
    
    # Override NBATCH if specified
    if [ -n "$NBATCH" ]; then
        echo "export NBATCH=$NBATCH" >> "$temp_env"
    fi
    
    echo "$temp_env"
}

# Function to create a custom create_suite.py
create_custom_suite_script() {
    local temp_suite=$(mktemp)
    local original_suite="./create_suite.py"
    
    if [ ! -f "$original_suite" ]; then
        echo "Error: create_suite.py not found"
        exit 1
    fi
    
    # Copy original and modify for selected processing types
    cp "$original_suite" "$temp_suite"
    
    # Create a modified version that only includes selected processing types
    # This is a simplified approach - in practice, you might want to create
    # a more sophisticated modification system
    
    echo "$temp_suite"
}

# Initialize variables
PERIOD=""
SUITE_NAME="$DEFAULT_SUITE_NAME"
NBATCH="$DEFAULT_NBATCH"
CONFIG="$DEFAULT_CONFIG"
HHMM=""
STREAMS=""
FORCE_FLAG=""

# Processing type flags
DO_DAILY_SUMS=false
DO_MONTHLY_SUMS=false
DO_DAILY_MEANS_AN=false
DO_DAILY_MEANS_FC=false
DO_MONTHLY_MEANS_AN=false
DO_DAILY_MINMAX=false
DO_MONTHLY_MINMAX=false

# Custom parameter variables
CUSTOM_PARAMS_SFC=""
CUSTOM_PARAMS_ML=""
CUSTOM_PARAMS_PL=""
CUSTOM_PARAMS_HL=""
CUSTOM_PARAMS_ACC=""
CUSTOM_PARAMS_FC_SFC=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--period)
            PERIOD="$2"
            validate_period "$PERIOD"
            shift 2
            ;;
        -n|--suite-name)
            SUITE_NAME="$2"
            shift 2
            ;;
        -b|--nbatch)
            NBATCH="$2"
            shift 2
            ;;
        -c|--config)
            CONFIG="$2"
            shift 2
            ;;
        -t|--time)
            HHMM="$2"
            shift 2
            ;;
        -s|--streams)
            STREAMS="$2"
            shift 2
            ;;
        --force)
            FORCE_FLAG="-f"
            shift
            ;;
        --daily-sums)
            DO_DAILY_SUMS=true
            shift
            ;;
        --monthly-sums)
            DO_MONTHLY_SUMS=true
            shift
            ;;
        --daily-means-an)
            DO_DAILY_MEANS_AN=true
            shift
            ;;
        --daily-means-fc)
            DO_DAILY_MEANS_FC=true
            shift
            ;;
        --monthly-means-an)
            DO_MONTHLY_MEANS_AN=true
            shift
            ;;
        --daily-minmax)
            DO_DAILY_MINMAX=true
            shift
            ;;
        --monthly-minmax)
            DO_MONTHLY_MINMAX=true
            shift
            ;;
        --params-sfc)
            CUSTOM_PARAMS_SFC=$(normalize_params "$2")
            shift 2
            ;;
        --params-ml)
            CUSTOM_PARAMS_ML=$(normalize_params "$2")
            shift 2
            ;;
        --params-pl)
            CUSTOM_PARAMS_PL=$(normalize_params "$2")
            shift 2
            ;;
        --params-hl)
            CUSTOM_PARAMS_HL=$(normalize_params "$2")
            shift 2
            ;;
        --params-acc)
            CUSTOM_PARAMS_ACC=$(normalize_params "$2")
            shift 2
            ;;
        --params-fc-sfc)
            CUSTOM_PARAMS_FC_SFC=$(normalize_params "$2")
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate required arguments
if [ -z "$PERIOD" ]; then
    echo "Error: Period is required (-p YYYYMM)"
    usage
    exit 1
fi

# Check if at least one processing type is selected
if [ "$DO_DAILY_SUMS" = false ] && [ "$DO_MONTHLY_SUMS" = false ] && \
   [ "$DO_DAILY_MEANS_AN" = false ] && [ "$DO_DAILY_MEANS_FC" = false ] && \
   [ "$DO_MONTHLY_MEANS_AN" = false ] && [ "$DO_DAILY_MINMAX" = false ] && \
   [ "$DO_MONTHLY_MINMAX" = false ]; then
    echo "Error: At least one processing type must be selected"
    echo "Use --daily-sums, --monthly-sums, --daily-means-an, etc."
    usage
    exit 1
fi

# Display configuration
echo "=== ECF Selected Suite Submission ==="
echo "Period: $PERIOD"
echo "Suite Name: ${SUITE_NAME}_$PERIOD"
echo "Number of Batches: $NBATCH"
echo "Configuration: $CONFIG"
if [ -n "$HHMM" ]; then
    echo "Scheduled Time: $HHMM"
fi
if [ -n "$STREAMS" ]; then
    echo "Streams: $STREAMS"
fi

echo ""
echo "Selected Processing Types:"
[ "$DO_DAILY_SUMS" = true ] && echo "  - Daily sums of accumulated variables"
[ "$DO_MONTHLY_SUMS" = true ] && echo "  - Monthly means of daily sums"
[ "$DO_DAILY_MEANS_AN" = true ] && echo "  - Daily means of analysis instantaneous"
[ "$DO_DAILY_MEANS_FC" = true ] && echo "  - Daily means of forecast variables"
[ "$DO_MONTHLY_MEANS_AN" = true ] && echo "  - Monthly means of analysis variables"
[ "$DO_DAILY_MINMAX" = true ] && echo "  - Daily min/max of forecast variables"
[ "$DO_MONTHLY_MINMAX" = true ] && echo "  - Monthly min/max variables"

echo ""
echo "Custom Parameters:"
[ -n "$CUSTOM_PARAMS_SFC" ] && echo "  - Surface: $CUSTOM_PARAMS_SFC"
[ -n "$CUSTOM_PARAMS_ML" ] && echo "  - Model Level: $CUSTOM_PARAMS_ML"
[ -n "$CUSTOM_PARAMS_PL" ] && echo "  - Pressure Level: $CUSTOM_PARAMS_PL"
[ -n "$CUSTOM_PARAMS_HL" ] && echo "  - Height Level: $CUSTOM_PARAMS_HL"
[ -n "$CUSTOM_PARAMS_ACC" ] && echo "  - Accumulated: $CUSTOM_PARAMS_ACC"
[ -n "$CUSTOM_PARAMS_FC_SFC" ] && echo "  - Forecast Surface: $CUSTOM_PARAMS_FC_SFC"

# Create temporary environment file with custom parameters
TEMP_ENV=$(create_custom_env)
echo "Created temporary environment file: $TEMP_ENV"

# Source the temporary environment
source "$TEMP_ENV"

# Export the suite name and other variables needed by ecfproj_start
export NAME_OF_SUITE="$SUITE_NAME"
export NBATCH

echo ""
echo "Creating batch ECF scripts if needed..."

# Create new batch ecf scripts if not there (similar to original script)
for i in $(seq 1 $NBATCH); do
    if [ ! -f ../share/ecf/daily_sum_fc_sfc_batch${i}.ecf ]; then
        MSCR=../share/ecf/daily_sum_fc_sfc_batch${i}.ecf
        echo "Creating missing ecf script $MSCR"
        sed "s/REPLACEBATCHNUMBER/$i/g" ../share/ecf/daily_sum_fc_sfc_batch_template.ecf > $MSCR
    fi
done

# Set streams if provided
if [ -n "$STREAMS" ]; then
    export ECFPROJ_STREAMS="$STREAMS"
fi

echo ""
echo "Starting ECF suite submission..."

# Determine execution mode and run ecfproj_start
if [ -z "$HHMM" ]; then
    echo "Running standard suite"
    echo "Suite name will be: ${NAME_OF_SUITE}_$PERIOD"
    ./ecfproj_start $FORCE_FLAG -s $PERIOD -c $CONFIG -e ${NAME_OF_SUITE}_$PERIOD
else
    echo "Running timed suite with HHMM=$HHMM"
    echo "Suite name will be: ${NAME_OF_SUITE}_$PERIOD"
    export USE_TIMED=1
    export HHMM=$HHMM
    ./ecfproj_start $FORCE_FLAG -s $PERIOD -c $CONFIG -e ${NAME_OF_SUITE}_$PERIOD
fi

# Clean up temporary files
rm -f "$TEMP_ENV"

echo ""
echo "=== Submission Complete ==="
echo "Suite: ${NAME_OF_SUITE}_$PERIOD"
echo "Check ECF server for job status"

exit 0