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
# Archive
#ARCHIVE = os.environ["ARCHIVE"]

# Start YMD/HH
# This will setup the first date of processing. If it is not set it will not work!
CARRA_PERIOD = os.environ["CARRA_PERIOD"]
MEANS_SCR = os.environ["MEANS_SCR"]

CARRA_PAR_FC_ACC = os.environ["CARRA_PAR_FC_ACC"]

# List of streams to process
get_streams = os.getenv('ECFPROJ_STREAMS') #.split(',')
if "," in get_streams:
    ecfproj_streams = get_streams.split(",")
else:
    ecfproj_streams = [get_streams]

print(f"Doing {ecfproj_streams}")

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
suite.add_variable("SUB_H",             "sbatch." + ECFPROJ_CONFIG + ".h")
# Added
suite.add_variable("TASK",           "")
suite.add_variable("CARRA_PERIOD",CARRA_PERIOD)
suite.add_variable("MEANS_SCR", MEANS_SCR)

# Add common "par" limit to jobs
suite.add_limit("par", 10)

SPLIT_SUM_VARS = CARRA_PAR_FC_ACC.split("/")
# add variables for the sums
#Split sums in n batches to speed up processing
NBATCH = int(os.environ['NBATCH'])

#Note that this implies there should be 1 to NBATCH+1 ecf templates for the corresponding
#scripts to be used.
#Consider using a function to create them below? Or offline in main submission script

#divide the sums into nbatches
split_size = len(SPLIT_SUM_VARS)//NBATCH
rem_split = len(SPLIT_SUM_VARS) % NBATCH
start_chunk = 0
for i in range(0,NBATCH):
    end_chunk = start_chunk + split_size + (1 if i < rem_split else 0)
    chunks_sum = SPLIT_SUM_VARS[start_chunk:end_chunk]
    start_chunk = end_chunk
    print(f"Adding {chunks_sum} to CARRA_PAR_FC_ACC_batch{i+1}")
    suite.add_variable(f"CARRA_PAR_FC_ACC_batch{i+1}"," ".join(chunks_sum))

#suite.add_variable("CARRA_PAR_FC_ACC_batch1"," ".join(SPLIT_SUM_VARS[0:5]))
#suite.add_variable("CARRA_PAR_FC_ACC_batch2"," ".join(SPLIT_SUM_VARS[5:11]))
#suite.add_variable("CARRA_PAR_FC_ACC_batch3", " ".join(SPLIT_SUM_VARS[11:16]))
#suite.add_variable("CARRA_PAR_FC_ACC_batch4"," ".join(SPLIT_SUM_VARS[16:]))

# ecflow does not like dashes, so renaming streams here
names_dict={"no-ar-cw":"west","no-ar-ce":"east","no-ar-pa":"pan_arctic"}

# Create the vobs conversion family
def create_daily_means(stream:str):
    this_stream = names_dict[stream]

    run = ec.Family(f"daily_{this_stream}")
    run.add_variable("ECFPROJ_STREAM", stream)
    #first do for an files
    for ltype in ["hl","pl","sfc","sol","ml"]:
        t1 = run.add_task(f"daily_mean_an_{ltype}")#f"daily_means_{this_stream}")
    #then do fc files
    t1 = run.add_task(f"daily_mean_fc_sfc")
    t1 = run.add_task(f"daily_sum_fc_sfc")

    return run


def create_daily_monthly_means(stream:str):
    """
    Monthly means run.
    Triggered only if all files are there for given level type
    Following https://confluence.ecmwf.int/display/ECFLOW/Adding+Triggers+and+Complete
    """
    this_stream = names_dict[stream]
    run = ec.Family(f"{this_stream}")
    run.add_variable("ECFPROJ_STREAM", stream)

    #first do for an files
    for ltype in ["hl","pl","sfc","ml"]:
        t1 = run.add_task(f"daily_mean_an_insta_{ltype}")

    #then do fc files
    t1 = run.add_task(f"daily_minmax_fc_sfc")
    #t1 = run.add_task(f"daily_sum_fc_sfc")
    for i in range(0,NBATCH):
      t1 = run.add_task(f"daily_sum_fc_sfc_batch{i+1}")
    #t1 = run.add_task(f"daily_sum_fc_sfc_batch2")
    #t1 = run.add_task(f"daily_sum_fc_sfc_batch3")
    #t1 = run.add_task(f"daily_sum_fc_sfc_batch4")
    #for param in CARRA_PAR_FC_ACC.split("/"):
    #    t1 = run.add_task(f"daily_sum_fc_sfc_{param}")

    t1 = run.add_task(f"monthly_means_an_insta")
    mm=[]
    for ltype in ["hl","pl","sfc","ml"]:
        mm.append(f"(daily_mean_an_insta_{ltype} == complete)")
    long_rule=f"({mm[0]} and {mm[1]} and {mm[2]} and {mm[3]})"
    t1.add_trigger(long_rule)

    #then do sums
    t1 = run.add_task("monthly_means_of_daily_sums")
    mm=[]
    #for bat in range(1,5):
    for bat in range(0,NBATCH):
        mm.append(f"(daily_sum_fc_sfc_batch{bat+1} == complete)")
    long_rule="("+" and ".join(mm)+")"
    t1.add_trigger(long_rule)

    t1 = run.add_task("monthly_minmax")
    t1.add_trigger( f"daily_minmax_fc_sfc == complete" )

    t1 = run.add_task(f"archive_to_marsscratch")
    long_rule = "((monthly_means_of_daily_sums == complete) and (monthly_minmax == complete) and (monthly_means_an_insta == complete))"
    t1.add_trigger(long_rule)

    t1 = run.add_task(f"clean_scratch")
    t1.add_trigger("(archive_to_marsscratch == complete)")

    return run


def create_report(stream:str):
    """
    Creates report for the whole processing
    """
    pass

# Create the vfld conversion family
def create_sqlite_vfld(run_hhmm):
    run = ec.Family("run")
    run.add_inlimit("par")
    run.add_repeat(ec.RepeatDate("YMD",int(start_ymd), 20990101, 1))
    #run.add_trigger("((run:YMD + %s) < :ECF_DATE) or ((run:YMD + %s) == :ECF_DATE and :TIME >= %s)" %(DELAY_VFLD,DELAY_VFLD,run_hhmm))
    run.add_trigger(f"((run:YMD == :ECF_DATE) and (:TIME >= {run_hhmm}))") #%(DELAY_VFLD,DELAY_VFLD,run_hhmm))
    t1 = run.add_task("vfld2sql")

    return run


# Create the families in the suite
fs = suite.add_family(CARRA_PERIOD)
for ecfproj_stream in ecfproj_streams:
    #fs.add_variable("ECFPROJ_STREAM", ecfproj_stream)
    print(f"Creating family for {ecfproj_stream}")
    fs.add_family(create_daily_monthly_means(ecfproj_stream))

    #original that works
    #print(f"Creating family for {ecfproj_stream}")
    #fs.add_family(create_daily_means(ecfproj_stream))

    # to correct: reference to completed task from family below!
    #fs.add_family(create_monthly_means(ecfproj_stream))

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
