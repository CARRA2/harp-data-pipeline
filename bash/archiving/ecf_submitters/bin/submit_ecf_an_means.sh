#!/usr/bin/env bash

DEFAULT_SUITE_NAME="carra2_an_means"
DEFAULT_CONFIG="means"

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

DESCRIPTION:
    Submit ECF climate means processing for analysis instantaneous variables.
    This script only processes analysis (AN) data for specified level types.

REQUIRED OPTIONS:
    -p, --period YYYYMM         Period to process (format: YYYYMM, e.g., 202301)

PROCESSING TYPE OPTIONS (at least one required):
    --daily-means              Process daily means of analysis instantaneous variables
    --monthly-means            Process monthly means of analysis variables

PARAMETER SELECTION OPTIONS (at least one required):
    --params-sfc PARAMS        Surface parameters (comma or slash separated)
    --params-ml PARAMS         Model level parameters (comma or slash separated)
    --params-pl PARAMS         Pressure level parameters (comma or slash separated)
    --params-hl PARAMS         Height level parameters (comma or slash separated)

CONFIGURATION OPTIONS:
    -n, --suite-name NAME      Name for the ECF suite (default: $DEFAULT_SUITE_NAME)
    -c, --config CONFIG        System configuration (default: $DEFAULT_CONFIG)
    -t, --time HHMM            Schedule suite to run at specific time (format: HHMM)
    -s, --streams STREAMS      Comma-separated list of streams (default: from env)
    --force                    Force replace existing suite

EXAMPLES:
    # Process daily and monthly means for pressure levels
    $0 -p 198901 --daily-means --monthly-means --params-pl "10,129,130,157,246,247,260028,260238,260257,3014,3031,60,75,76"

    # Process only daily means for surface and pressure levels
    $0 -p 202301 --daily-means --params-sfc "31,34,78,79" --params-pl "129,130"

    # Process with custom suite name and scheduled time
    $0 -p 202301 --daily-means --monthly-means --params-pl "129,130" -n "my_an_means" -t 2350

NOTES:
    - Parameters can be specified as comma-separated (31,34,78) or slash-separated (31/34/78)
    - Only level types with specified parameters will be processed
    - Use --force to replace an existing suite with the same name

EOF
}

normalize_params() {
    echo "$1" | sed 's/,/\//g'
}

validate_period() {
    if [[ ! "$1" =~ ^[0-9]{6}$ ]]; then
        echo "Error: Period must be in YYYYMM format (e.g., 202301)"
        exit 1
    fi
}

create_custom_env() {
    local temp_env=$(mktemp)
    
    if [ -f ./env.sh ]; then
        cp ./env.sh "$temp_env"
    else
        echo "Error: env.sh not found in current directory"
        exit 1
    fi
    
    # Unset all parameter environment variables first to ensure only selected ones are active
    echo "unset CARRA_PAR_AN_SFC" >> "$temp_env"
    echo "unset CARRA_PAR_AN_SFC_ARXIV" >> "$temp_env"
    echo "unset CARRA_PAR_AN_ML" >> "$temp_env"
    echo "unset CARRA_PAR_AN_PL" >> "$temp_env"
    echo "unset CARRA_PAR_AN_HL" >> "$temp_env"
    
    # Now export only the specified custom parameters
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
    
    echo "$temp_env"
}


create_an_means_suite_script() {
    local temp_suite=$(mktemp --suffix=.py)
    
    cat > "$temp_suite" << 'EOF'
import os, sys
from datetime import datetime, timedelta
import time
from time import gmtime as gmtime
from time import strftime as tstrftime
import getpass

import ecflow as ec

ECFPROJ_LIB = os.environ["ECFPROJ_LIB"]
ECFPROJ_CONFIG = os.environ["ECFPROJ_CONFIG"]
ECFPROJ_WORK   = os.environ["ECFPROJ_WORK"]

EXP = os.environ['EXP']
CLUSTER = os.environ['HOSTNAME']
USER = os.environ["USER"]
ECF_HOST = os.environ["ECF_HOST"]
ECF_PORT = os.environ["ECF_PORT"]

FORCE = os.environ["FORCE"]

CARRA_PERIOD = os.environ["CARRA_PERIOD"]
MEANS_SCR = os.environ["MEANS_SCR"]

get_streams = os.getenv('ECFPROJ_STREAMS')
if "," in get_streams:
    ecfproj_streams = get_streams.split(",")
else:
    ecfproj_streams = [get_streams]

print(f"Doing {ecfproj_streams}")

DO_DAILY_MEANS = os.getenv('DO_DAILY_MEANS', 'false').lower() == 'true'
DO_MONTHLY_MEANS = os.getenv('DO_MONTHLY_MEANS', 'false').lower() == 'true'

LEVEL_TYPES = os.getenv('LEVEL_TYPES', '').split(',')
LEVEL_TYPES = [lt.strip() for lt in LEVEL_TYPES if lt.strip()]

print(f"Processing types:")
print(f"  Daily means: {DO_DAILY_MEANS}")
print(f"  Monthly means: {DO_MONTHLY_MEANS}")
print(f"  Level types: {LEVEL_TYPES}")

for ltype in LEVEL_TYPES:
    param_var = f"CARRA_PAR_AN_{ltype.upper()}"
    param_value = os.getenv(param_var, "")
    print(f"  {ltype}: {param_value}")

defs = ec.Defs()
suite = defs.add_suite(EXP)
suite.add_variable("USER",           USER)
suite.add_variable("ECFPROJ_LIB",       ECFPROJ_LIB)
suite.add_variable("ECFPROJ_CONFIG",     ECFPROJ_CONFIG)
suite.add_variable("EXP",            EXP)
suite.add_variable("ECF_HOME",       "%s"%ECFPROJ_WORK)
suite.add_variable("ECF_INCLUDE",    "%s/share/ecf"%ECFPROJ_LIB)
suite.add_variable("ECF_FILES",      "%s/share/ecf"%ECFPROJ_LIB)

for ltype in LEVEL_TYPES:
    param_var = f"CARRA_PAR_AN_{ltype.upper()}"
    param_value = os.getenv(param_var, "")
    if param_value:
        suite.add_variable(param_var, param_value)

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

suite.add_limit("par", 10)

names_dict={"no-ar-cw":"west","no-ar-ce":"east","no-ar-pa":"pan_arctic"}

def create_an_means_family(stream:str):
    this_stream = names_dict[stream]
    run = ec.Family(f"{this_stream}")
    run.add_variable("ECFPROJ_STREAM", stream)

    created_daily_tasks = []
    
    if DO_DAILY_MEANS:
        for ltype in LEVEL_TYPES:
            t1 = run.add_task(f"daily_mean_an_insta_{ltype}")
            created_daily_tasks.append(ltype)

    if DO_MONTHLY_MEANS and created_daily_tasks:
        t1 = run.add_task(f"monthly_means_an_insta_level")
        if len(created_daily_tasks) > 0:
            mm = []
            for ltype in created_daily_tasks:
                mm.append(f"(daily_mean_an_insta_{ltype} == complete)")
            if len(mm) > 1:
                long_rule = "(" + " and ".join(mm) + ")"
            else:
                long_rule = mm[0]
            t1.add_trigger(long_rule)

    if DO_MONTHLY_MEANS and created_daily_tasks:
        t1 = run.add_task(f"archive_to_marsscratch")
        t1.add_trigger("(monthly_means_an_insta_level == complete)")

    return run

fs = suite.add_family(CARRA_PERIOD)

for ecfproj_stream in ecfproj_streams:
    print(f"Creating AN means family for {ecfproj_stream}")
    fs.add_family(create_an_means_family(ecfproj_stream))

if __name__=="__main__":
    client = ec.Client(ECF_HOST, ECF_PORT)
    
    print("Saving definition to file '%s.def'"%EXP)
    defs.save_as_defs("%s.def"%EXP)
    
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
    
    client.suspend("/%s" %suite.name())
    client.begin_suite("/%s" % suite.name(), True)
    
    client.resume("/%s" %suite.name())
    
    exit(0)
EOF
    
    echo "$temp_suite"
}

PERIOD=""
SUITE_NAME="$DEFAULT_SUITE_NAME"
CONFIG="$DEFAULT_CONFIG"
HHMM=""
STREAMS=""
FORCE_FLAG=""

DO_DAILY_MEANS=false
DO_MONTHLY_MEANS=false

CUSTOM_PARAMS_SFC=""
CUSTOM_PARAMS_ML=""
CUSTOM_PARAMS_PL=""
CUSTOM_PARAMS_HL=""

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
        --daily-means)
            DO_DAILY_MEANS=true
            shift
            ;;
        --monthly-means)
            DO_MONTHLY_MEANS=true
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

if [ -z "$PERIOD" ]; then
    echo "Error: Period is required (-p YYYYMM)"
    usage
    exit 1
fi

if [ "$DO_DAILY_MEANS" = false ] && [ "$DO_MONTHLY_MEANS" = false ]; then
    echo "Error: At least one processing type must be selected (--daily-means or --monthly-means)"
    usage
    exit 1
fi

if [ -z "$CUSTOM_PARAMS_SFC" ] && [ -z "$CUSTOM_PARAMS_ML" ] && \
   [ -z "$CUSTOM_PARAMS_PL" ] && [ -z "$CUSTOM_PARAMS_HL" ]; then
    echo "Error: At least one parameter type must be specified"
    echo "Use --params-sfc, --params-ml, --params-pl, or --params-hl"
    usage
    exit 1
fi

LEVEL_TYPES=""
[ -n "$CUSTOM_PARAMS_SFC" ] && LEVEL_TYPES="${LEVEL_TYPES}sfc,"
[ -n "$CUSTOM_PARAMS_ML" ] && LEVEL_TYPES="${LEVEL_TYPES}ml,"
[ -n "$CUSTOM_PARAMS_PL" ] && LEVEL_TYPES="${LEVEL_TYPES}pl,"
[ -n "$CUSTOM_PARAMS_HL" ] && LEVEL_TYPES="${LEVEL_TYPES}hl,"
LEVEL_TYPES=${LEVEL_TYPES%,}

echo "=== ECF Analysis Means Suite Submission ==="
echo "Period: $PERIOD"
echo "Suite Name: ${SUITE_NAME}_$PERIOD"
echo "Configuration: $CONFIG"
if [ -n "$HHMM" ]; then
    echo "Scheduled Time: $HHMM"
fi
if [ -n "$STREAMS" ]; then
    echo "Streams: $STREAMS"
fi

echo ""
echo "Selected Processing Types:"
[ "$DO_DAILY_MEANS" = true ] && echo "  - Daily means of analysis instantaneous"
[ "$DO_MONTHLY_MEANS" = true ] && echo "  - Monthly means of analysis variables"

echo ""
echo "Level Types and Parameters:"
[ -n "$CUSTOM_PARAMS_SFC" ] && echo "  - Surface: $CUSTOM_PARAMS_SFC"
[ -n "$CUSTOM_PARAMS_ML" ] && echo "  - Model Level: $CUSTOM_PARAMS_ML"
[ -n "$CUSTOM_PARAMS_PL" ] && echo "  - Pressure Level: $CUSTOM_PARAMS_PL"
[ -n "$CUSTOM_PARAMS_HL" ] && echo "  - Height Level: $CUSTOM_PARAMS_HL"

TEMP_ENV=$(create_custom_env)
echo ""
echo "Created temporary environment file: $TEMP_ENV"

source "$TEMP_ENV"

export NAME_OF_SUITE="$SUITE_NAME"

export DO_DAILY_MEANS
export DO_MONTHLY_MEANS
export LEVEL_TYPES

echo ""
echo "Creating custom suite generation script..."

TEMP_SUITE=$(create_an_means_suite_script)
echo "Created temporary suite script: $TEMP_SUITE"

if [ -f "./create_suite.py" ]; then
    cp "./create_suite.py" "./create_suite.py.backup.$$"
    echo "Backed up original create_suite.py to create_suite.py.backup.$$"
fi

cp "$TEMP_SUITE" "./create_suite.py"
echo "Replaced create_suite.py with AN means version"

if [ -n "$STREAMS" ]; then
    export ECFPROJ_STREAMS="$STREAMS"
fi

echo ""
echo "Starting ECF suite submission..."

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

if [ -f "./create_suite.py.backup.$$" ]; then
    mv "./create_suite.py.backup.$$" "./create_suite.py"
    echo "Restored original create_suite.py"
fi

rm -f "$TEMP_ENV"
rm -f "$TEMP_SUITE"

echo ""
if [ $ECFPROJ_EXIT_CODE -eq 0 ]; then
    echo "=== Submission Complete ==="
    echo "Suite: ${NAME_OF_SUITE}_$PERIOD"
    echo "Only specified level types were included in the suite"
    echo "Check ECF server for job status"
else
    echo "=== Submission Failed ==="
    echo "ecfproj_start returned exit code: $ECFPROJ_EXIT_CODE"
fi

exit $ECFPROJ_EXIT_CODE

def create_an_means_family(stream:str):
    this_stream = names_dict[stream]
    run = ec.Family(f"{this_stream}")
    run.add_variable("ECFPROJ_STREAM", stream)

    created_daily_tasks = []
    
    if DO_DAILY_MEANS:
        for ltype in LEVEL_TYPES:
            t1 = run.add_task(f"daily_mean_an_insta_{ltype}")
            created_daily_tasks.append(ltype)

    if DO_MONTHLY_MEANS and created_daily_tasks:
        for ltype in created_daily_tasks:
            t1 = run.add_task(f"monthly_means_an_insta_{ltype.upper()}")
            t1.add_trigger(f"(daily_mean_an_insta_{ltype} == complete)")

    if DO_MONTHLY_MEANS and created_daily_tasks:
        t1 = run.add_task(f"archive_to_marsscratch")
        mm = []
        for ltype in created_daily_tasks:
            mm.append(f"(monthly_means_an_insta_{ltype.upper()} == complete)")
        if len(mm) > 1:
            long_rule = "(" + " and ".join(mm) + ")"
        else:
            long_rule = mm[0]
        t1.add_trigger(long_rule)

    return run
