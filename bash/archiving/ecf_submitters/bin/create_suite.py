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
