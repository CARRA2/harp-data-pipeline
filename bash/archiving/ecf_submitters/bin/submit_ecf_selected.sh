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

# Function to create a custom create_suite.py that only includes selected tasks
create_custom_suite_script() {
    local temp_suite=$(mktemp --suffix=.py)
    local original_suite="./create_suite.py"
    
    if [ ! -f "$original_suite" ]; then
        echo "Error: create_suite.py not found"
        exit 1
    fi
    
    # Read the original file and create a modified version
    cat > "$temp_suite" << 'EOF'
import os, sys
#import time, datetime
from datetime import datetime, timedelta
import time
from time import gmtime as gmtime
from time import strftime as tstrftime
import getpass
import argparse

import ecflow as ec

# System configuration
ECFPROJ_LIB = os.environ["ECFPROJ_LIB"]
ECFPROJ_CONFIG = os.environ["ECFPROJ_CONFIG"]
ECFPROJ_WORK   = os.environ["ECFPROJ_WORK"]

# Common
EXP = os.environ['EXP']
CLUSTER = os.environ['HOSTNAME']
USER = os.environ["USER"]
ECF_HOST = os.environ["ECF_HOST"]
ECF_PORT = os.environ["ECF_PORT"]

# Force replace suite
FORCE = os.environ["FORCE"]

# Start YMD/HH
CARRA_PERIOD = os.environ["CARRA_PERIOD"]
MEANS_SCR = os.environ["MEANS_SCR"]

CARRA_PAR_FC_ACC = os.environ["CARRA_PAR_FC_ACC"]

# List of streams to process
get_streams = os.getenv('ECFPROJ_STREAMS')
if "," in get_streams:
    ecfproj_streams = get_streams.split(",")
else:
    ecfproj_streams = [get_streams]

print(f"Doing {ecfproj_streams}")

# Get selected processing types from environment
SELECTED_DAILY_SUMS = os.getenv('SELECTED_DAILY_SUMS', 'false').lower() == 'true'
SELECTED_MONTHLY_SUMS = os.getenv('SELECTED_MONTHLY_SUMS', 'false').lower() == 'true'
SELECTED_DAILY_MEANS_AN = os.getenv('SELECTED_DAILY_MEANS_AN', 'false').lower() == 'true'
SELECTED_DAILY_MEANS_FC = os.getenv('SELECTED_DAILY_MEANS_FC', 'false').lower() == 'true'
SELECTED_MONTHLY_MEANS_AN = os.getenv('SELECTED_MONTHLY_MEANS_AN', 'false').lower() == 'true'
SELECTED_DAILY_MINMAX = os.getenv('SELECTED_DAILY_MINMAX', 'false').lower() == 'true'
SELECTED_MONTHLY_MINMAX = os.getenv('SELECTED_MONTHLY_MINMAX', 'false').lower() == 'true'

print(f"Selected processing types:")
print(f"  Daily sums: {SELECTED_DAILY_SUMS}")
print(f"  Monthly sums: {SELECTED_MONTHLY_SUMS}")
print(f"  Daily means AN: {SELECTED_DAILY_MEANS_AN}")
print(f"  Daily means FC: {SELECTED_DAILY_MEANS_FC}")
print(f"  Monthly means AN: {SELECTED_MONTHLY_MEANS_AN}")
print(f"  Daily minmax: {SELECTED_DAILY_MINMAX}")
print(f"  Monthly minmax: {SELECTED_MONTHLY_MINMAX}")

defs = ec.Defs()
suite = defs.add_suite(EXP)
suite.add_variable("USER",           USER)
suite.add_variable("ECFPROJ_LIB",       ECFPROJ_LIB)
suite.add_variable("ECFPROJ_CONFIG",     ECFPROJ_CONFIG)
suite.add_variable("EXP",            EXP)
suite.add_variable("ECF_HOME",       "%s"%ECFPROJ_WORK)
suite.add_variable("ECF_INCLUDE",    "%s/share/ecf"%ECFPROJ_LIB)
suite.add_variable("ECF_FILES",      "%s/share/ecf"%ECFPROJ_LIB)
suite.add_variable("CARRA_PAR_FC_ACC",CARRA_PAR_FC_ACC)

SCHOST= 'hpc'
ECF_JOB_CMD = '%TROIKA% -c %TROIKA_CONFIG% submit -o %ECF_JOBOUT% %SCHOST% %ECF_JOB%'
ECF_KILL_CMD = '%TROIKA% -c %TROIKA_CONFIG% kill %SCHOST% %ECF_JOB%'
ECF_STATUS_CMD = '%TROIKA% -c %TROIKA_CONFIG% monitor %SCHOST% %ECF_JOB%'
suite.add_variable("SCHOST",            SCHOST)
suite.add_variable("ECF_JOB_CMD",       ECF_JOB_CMD)
suite.add_variable("ECF_KILL_CMD",      ECF_KILL_CMD)
suite.add_variable("ECF_STATUS_CMD",    ECF_STATUS_CMD)
suite.add_variable("QUEUE",             'nf')
suite.add_variable("SBU_CARRA_MEANS",             'c3srrp')
suite.add_variable("SUB_H",             "sbatch." + ECFPROJ_CONFIG + ".h")
suite.add_variable("TASK",           "")
suite.add_variable("CARRA_PERIOD",CARRA_PERIOD)
suite.add_variable("MEANS_SCR", MEANS_SCR)

# Add common "par" limit to jobs
suite.add_limit("par", 10)

SPLIT_SUM_VARS = CARRA_PAR_FC_ACC.split("/")
NBATCH = int(os.environ['NBATCH'])

# Only create batch variables if daily sums are selected
if SELECTED_DAILY_SUMS:
    split_size = len(SPLIT_SUM_VARS)//NBATCH
    rem_split = len(SPLIT_SUM_VARS) % NBATCH
    start_chunk = 0
    for i in range(0,NBATCH):
        end_chunk = start_chunk + split_size + (1 if i < rem_split else 0)
        chunks_sum = SPLIT_SUM_VARS[start_chunk:end_chunk]
        start_chunk = end_chunk
        print(f"Adding {chunks_sum} to CARRA_PAR_FC_ACC_batch{i+1}")
        suite.add_variable(f"CARRA_PAR_FC_ACC_batch{i+1}"," ".join(chunks_sum))

# ecflow does not like dashes, so renaming streams here
names_dict={"no-ar-cw":"west","no-ar-ce":"east","no-ar-pa":"pan_arctic"}

def create_selective_daily_monthly_means(stream:str):
    """
    Create only the selected processing tasks based on user selection
    """
    this_stream = names_dict[stream]
    run = ec.Family(f"{this_stream}")
    run.add_variable("ECFPROJ_STREAM", stream)

    # Track which tasks are created for dependency management
    created_daily_an_tasks = []
    created_daily_sum_tasks = []
    
    # Daily means for analysis files (if selected)
    if SELECTED_DAILY_MEANS_AN:
        for ltype in ["hl","pl","sfc","ml"]:
            t1 = run.add_task(f"daily_mean_an_insta_{ltype}")
            created_daily_an_tasks.append(ltype)

    # Daily means for forecast files (if selected)
    if SELECTED_DAILY_MEANS_FC:
        t1 = run.add_task(f"daily_mean_fc_sfc")

    # Daily min/max for forecast files (if selected)
    if SELECTED_DAILY_MINMAX:
        t1 = run.add_task(f"daily_minmax_fc_sfc")

    # Daily sums (if selected)
    if SELECTED_DAILY_SUMS:
        for i in range(0,NBATCH):
            t1 = run.add_task(f"daily_sum_fc_sfc_batch{i+1}")
            created_daily_sum_tasks.append(i+1)

    # Monthly means of analysis (if selected and daily means AN were created)
    if SELECTED_MONTHLY_MEANS_AN and created_daily_an_tasks:
        t1 = run.add_task(f"monthly_means_an_insta")
        # Only add trigger if we have the required daily tasks
        if len(created_daily_an_tasks) > 0:
            mm = []
            for ltype in created_daily_an_tasks:
                mm.append(f"(daily_mean_an_insta_{ltype} == complete)")
            if len(mm) > 1:
                long_rule = "(" + " and ".join(mm) + ")"
            else:
                long_rule = mm[0]
            t1.add_trigger(long_rule)

    # Monthly means of daily sums (if selected and daily sums were created)
    if SELECTED_MONTHLY_SUMS and created_daily_sum_tasks:
        t1 = run.add_task("monthly_means_of_daily_sums")
        # Only add trigger if we have the required daily sum tasks
        if len(created_daily_sum_tasks) > 0:
            mm = []
            for bat in created_daily_sum_tasks:
                mm.append(f"(daily_sum_fc_sfc_batch{bat} == complete)")
            long_rule = "(" + " and ".join(mm) + ")"
            t1.add_trigger(long_rule)

    # Monthly min/max (if selected and daily minmax was created)
    if SELECTED_MONTHLY_MINMAX and SELECTED_DAILY_MINMAX:
        t1 = run.add_task("monthly_minmax")
        t1.add_trigger(f"daily_minmax_fc_sfc == complete")

    # Archive task (only if we have something to archive)
    archive_conditions = []
    if SELECTED_MONTHLY_SUMS and created_daily_sum_tasks:
        archive_conditions.append("(monthly_means_of_daily_sums == complete)")
    if SELECTED_MONTHLY_MINMAX and SELECTED_DAILY_MINMAX:
        archive_conditions.append("(monthly_minmax == complete)")
    if SELECTED_MONTHLY_MEANS_AN and created_daily_an_tasks:
        archive_conditions.append("(monthly_means_an_insta == complete)")
    
    if archive_conditions:
        t1 = run.add_task(f"archive_to_marsscratch")
        if len(archive_conditions) > 1:
            long_rule = "(" + " and ".join(archive_conditions) + ")"
        else:
            long_rule = archive_conditions[0]
        t1.add_trigger(long_rule)

        # Clean scratch (only if archive task exists)
        t1 = run.add_task(f"clean_scratch")
        t1.add_trigger("(archive_to_marsscratch == complete)")

    return run

# Create the families in the suite
fs = suite.add_family(CARRA_PERIOD)
for ecfproj_stream in ecfproj_streams:
    print(f"Creating selective family for {ecfproj_stream}")
    fs.add_family(create_selective_daily_monthly_means(ecfproj_stream))

if __name__=="__main__":
    # Define a client object with the target ecFlow server
    client = ec.Client(ECF_HOST, ECF_PORT)
    
    # Save the definition to a .def file
    print("Saving definition to file '%s.def'"%EXP)
    defs.save_as_defs("%s.def"%EXP)
    
    # If the force flag is set, load the suite regardless of whether an
    # experiment of the same name exists in the ecFlow server
    if FORCE == "True":
        client.load(defs, force=True)
    else:
        try:
            client.load(defs, force=False)
        except:
            print("ERROR: Could not load %s on %s@%s" %(suite.name(), ECF_HOST, ECF_PORT))
            print("Use the force option to replace an existing suite:")
            print("    ./ecfproj_start -f")
            exit(1)
    
    print("loading on %s@%s" %(ECF_HOST,ECF_PORT))
    
    # Suspend the suite to allow cycles to be forced complete
    client.suspend("/%s" %suite.name())
    # Begin the suite
    client.begin_suite("/%s" % suite.name(), True)
    
    # Resume the suite
    client.resume("/%s" %suite.name())
    
    exit(0)
EOF
    
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

# Export selected processing types for the custom suite script
export SELECTED_DAILY_SUMS="$DO_DAILY_SUMS"
export SELECTED_MONTHLY_SUMS="$DO_MONTHLY_SUMS"
export SELECTED_DAILY_MEANS_AN="$DO_DAILY_MEANS_AN"
export SELECTED_DAILY_MEANS_FC="$DO_DAILY_MEANS_FC"
export SELECTED_MONTHLY_MEANS_AN="$DO_MONTHLY_MEANS_AN"
export SELECTED_DAILY_MINMAX="$DO_DAILY_MINMAX"
export SELECTED_MONTHLY_MINMAX="$DO_MONTHLY_MINMAX"

echo ""
echo "Creating custom suite generation script..."

# Create custom suite script
TEMP_SUITE=$(create_custom_suite_script)
echo "Created temporary suite script: $TEMP_SUITE"

# Backup original create_suite.py and replace with custom version
if [ -f "./create_suite.py" ]; then
    cp "./create_suite.py" "./create_suite.py.backup.$$"
    echo "Backed up original create_suite.py to create_suite.py.backup.$$"
fi

cp "$TEMP_SUITE" "./create_suite.py"
echo "Replaced create_suite.py with selective version"

echo ""
echo "Creating batch ECF scripts if needed..."

# Create new batch ecf scripts if not there (only if daily sums are selected)
if [ "$DO_DAILY_SUMS" = true ]; then
    for i in $(seq 1 $NBATCH); do
        if [ ! -f ../share/ecf/daily_sum_fc_sfc_batch${i}.ecf ]; then
            MSCR=../share/ecf/daily_sum_fc_sfc_batch${i}.ecf
            echo "Creating missing ecf script $MSCR"
            sed "s/REPLACEBATCHNUMBER/$i/g" ../share/ecf/daily_sum_fc_sfc_batch_template.ecf > $MSCR
        fi
    done
fi

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
    ECFPROJ_EXIT_CODE=$?
else
    echo "Running timed suite with HHMM=$HHMM"
    echo "Suite name will be: ${NAME_OF_SUITE}_$PERIOD"
    export USE_TIMED=1
    export HHMM=$HHMM
    ./ecfproj_start $FORCE_FLAG -s $PERIOD -c $CONFIG -e ${NAME_OF_SUITE}_$PERIOD
    ECFPROJ_EXIT_CODE=$?
fi

# Restore original create_suite.py
if [ -f "./create_suite.py.backup.$$" ]; then
    mv "./create_suite.py.backup.$$" "./create_suite.py"
    echo "Restored original create_suite.py"
fi

# Clean up temporary files
rm -f "$TEMP_ENV"
rm -f "$TEMP_SUITE"

echo ""
if [ $ECFPROJ_EXIT_CODE -eq 0 ]; then
    echo "=== Submission Complete ==="
    echo "Suite: ${NAME_OF_SUITE}_$PERIOD"
    echo "Only selected processing types were included in the suite"
    echo "Check ECF server for job status"
else
    echo "=== Submission Failed ==="
    echo "ecfproj_start returned exit code: $ECFPROJ_EXIT_CODE"
fi

exit $ECFPROJ_EXIT_CODE