# Comprehensive Script Summary - HARP Data Pipeline for CARRA2

## Overview
This repository contains a data pipeline for processing, archiving, and verifying CARRA2 climate means. The pipeline handles daily and monthly aggregations, MARS archival, data retrieval, error correction, and verification workflows.

---

## Main Scripts

### 1. bash/archiving/ecf_submitters/bin/check_submit_new_period.sh

**Purpose**: SLURM job submission script that automatically checks for and submits new periods for processing

**Key Features**:
- SLURM configuration: 16GB memory, 30-minute time limit
- Configures ECFlow server connection (port 3141, host: ecflow-gen-${USER}-001)
- Loads Python3 and ECFlow modules
- Executes `run_new_period.py` to check and submit new processing periods

**Dependencies**:
- Python3 module
- ECFlow module
- run_new_period.py script

**Usage**: Submitted as SLURM job to periodically check for new data periods

---

### 2. bash/archiving/ecf_submitters/bin/submit_ecf_suite.sh

**Purpose**: Main submission script for ECFlow suites that process CARRA2 daily means

**Key Features**:
- Accepts period in YYYYMM format and optional HHMM time for scheduled runs
- Creates ECFlow suite named "carra2_means" by default
- Splits processing into 4 batches (configurable via NBATCH variable)
- Automatically generates missing ECF batch scripts from templates
- Supports both immediate and timed suite execution
- Calls `ecfproj_start` with appropriate configuration

**Parameters**:
- `$1`: Period in YYYYMM format (required)
- `$2`: HHMM time for scheduled execution (optional)

**Workflow**:
1. Validates period argument
2. Creates batch ECF scripts if missing (daily_sum_fc_sfc_batch${i}.ecf)
3. Executes standard suite OR timed suite based on arguments
4. Uses "means" configuration for daily means processing

**Usage Examples**:
```bash
./submit_ecf_suite.sh 200001              # Standard suite
./submit_ecf_suite.sh 200001 2350         # Timed suite at 23:50
```

---

### 3. bash/archiving/retrieve_and_archive/fetch_and_prepare_for_archival_from_fac2.sh

**Purpose**: Fetches data from MARS scratch and prepares archival scripts for fac2 execution

**Key Features**:
- SLURM job with 16GB memory, QOS=nf, account=c3srrp
- Fetches data from marsscratch (archived by nhd periodically)
- Creates archival scripts for fac2 to execute
- Uses two configuration files:
  - mars_config.yaml: For data fetching
  - mars_config_archive.yaml: For creating archival scripts
- Validates successful execution by checking script line count (>170 lines)
- Maintains fetching registry with timestamps

**Workflow**:
1. Validates period argument (YYYYMM format)
2. Fetches data from marsscratch using `fetch_from_marsscr.py`
3. Creates archival scripts using `archive_to_mars.py`
4. Sets permissions (755) on fetched data
5. Records successful operations in fetching_registry.txt

**Output**:
- Data dumped to: `/ec/res4/scratch/nhd/mars-pull/carra2/fetch_to_archive/$PERIOD`
- Archival script: `archive_${PERIOD}_from_fac2.sh`
- Registry: `fetching_registry.txt`

**Usage**:
```bash
sbatch fetch_and_prepare_for_archival_from_fac2.sh 199010
```

---

## ECF Submitters Directory (bash/archiving/ecf_submitters/bin/)

### Processing Scripts

#### daily_mean_an_insta_sfc.sh
**Purpose**: Calculates daily means for analysis instantaneous surface parameters

**Key Features**:
- Processes surface level (sfc) analysis data
- SLURM: 16GB memory, 48-hour time limit
- Stages and retrieves data from MARS
- Calculates 8-point daily means (00Z to 21Z, 3-hourly)
- Uses grib_mean.x tool for averaging
- Removes static variables (31/172/173/228002) and accumulated variables (174008, 260430)

**Parameters**:
- $1: Period (YYYYMM)
- $2: Origin (e.g., no-ar-pa, no-ar-ce)
- $3: Start day (optional)
- $4: End day (optional)

**Output**: Daily mean GRIB2 files in `$MEANS_OUTPUT/$origin/$YYYY/$MM/`

---

#### daily_mean_an_insta_ml.sh / pl.sh / hl.sh
**Purpose**: Similar to sfc script but for model levels (ml), pressure levels (pl), and height levels (hl)

**Key Differences**:
- Processes 3D atmospheric data on different vertical coordinate systems
- Larger memory requirements for ML processing
- Same temporal averaging methodology

---

#### daily_sum_fc_accum_sfc.sh
**Purpose**: Calculates daily sums for accumulated forecast variables (precipitation, etc.)

**Key Features**:
- Processes accumulated forecast parameters
- Implements complex 24-hour accumulation formula:
  - acc24(N) = acc0to6 + acc6to18 + acc18to24
  - acc0to6 = M(N-1;Z=12;t=18) - M(N-1;Z=12;t=12)
  - acc6to18 = M(N,Z=0,t=18) - M(N,Z=0,t=06)
  - acc18to24 = M(N,Z=12,t=12) - M(N,Z=12,t=06)
- Uses MARS compute functionality for field arithmetic
- Can process all parameters or single parameter
- Supports re-write control (RE_WRITE flag)

**Parameters**:
- $1: Period (YYYYMM)
- $2: Origin
- $3: Parameter (optional, processes all if not specified)
- $4: Start day (optional)
- $5: End day (optional)

---

#### daily_minmax_fc_sfc.sh
**Purpose**: Calculates daily minimum and maximum values for forecast surface parameters

**Key Features**:
- Processes temperature extremes and other min/max variables
- Uses calc_daily_minmax.py Python script
- Processes 8 forecast times per day

---

#### monthly_means_an_insta.sh
**Purpose**: Aggregates daily means into monthly means for analysis instantaneous parameters

**Key Features**:
- SLURM: 64GB memory, 48-hour time limit
- Processes all level types: sfc, hl, ml, pl
- Creates both monthly means and merged daily files
- Validates data completeness before processing
- Uses parameter dictionaries for different level types
- Merges all parameters into single files per level type

**Workflow**:
1. Calculate monthly means for each parameter separately
2. Merge monthly means into single file per level type
3. Merge daily means into single file per day per level type
4. Clean up temporary directories

**Output**:
- Monthly: `monthly_mean_${origin}_${type}_${levtype}_${period}.grib2`
- Daily: `daily_mean_${origin}_${type}_${levtype}_${date}.grib2`

---

#### monthly_means_fc_sfc.sh
**Purpose**: Creates monthly means for forecast surface parameters

---

#### monthly_means_of_daily_sums.sh
**Purpose**: Aggregates daily sums into monthly totals

---

#### monthly_minmax.sh
**Purpose**: Calculates monthly minimum and maximum values from daily extremes

---

#### master_submission_script.sh
**Purpose**: Orchestrates submission of all daily and monthly processing jobs

**Key Features**:
- Defines functions for submitting daily means, daily sums, and monthly aggregations
- Submits multiple SLURM jobs in parallel
- Splits daily sums by parameter to avoid timeout
- Configurable origin (default: no-ar-pa)

**Functions**:
- `submit_daily_means()`: Submits all daily mean calculations
- `submit_daily_sums()`: Submits daily sum calculations split by parameter
- `submit_daily()`: Calls both mean and sum functions
- `submit_monthly()`: Submits monthly aggregation jobs

---

#### run_new_period.py
**Purpose**: Python script that automatically detects when new periods are ready for processing

**Key Features**:
- Reads last processed periods from `last_archival_done.txt`
- Checks current data availability from `periods.txt`
- Triggers processing when data is 62+ days old
- Updates tracking file after successful submission
- Implements backup/restore mechanism for safety
- Calls ecfproj_start to create ECFlow suite

**Logic**:
- Compares last archived period with current timestamp
- If difference >= 62 days, processes next month
- Sets environment variables: CARRA_PERIOD, EXP
- Updates last_archival_done.txt on success

---

#### create_suite.py
**Purpose**: Python script for creating ECFlow suite definitions

---

#### correct_tp_values.sh / set_tp_to_zero.py
**Purpose**: Corrects erroneous total precipitation values in GRIB files

---

#### clean_scratch.sh
**Purpose**: Cleans up temporary scratch space

---

#### confirm_daily_means.sh
**Purpose**: Validates that daily mean calculations completed successfully

---

## Archive Submitters Directory (bash/archiving/archive_submitters/)

### submit_all_archival_scripts.sh
**Purpose**: Master script that submits all archival jobs for a given period

**Key Features**:
- Submits 6 different archival job types:
  1. daily_mean_an: Daily mean analysis
  2. daily_minmax_fc: Daily min/max forecasts
  3. daily_sum_fc: Daily sum forecasts
  4. monthly_mean_an: Monthly mean analysis
  5. monthly_minmax_fc: Monthly min/max forecasts
  6. monthly_daysum_fc: Monthly day sum forecasts
- Uses descriptive job names for tracking
- Configurable period and origin

---

### archive_daily_mean_an.sh
**Purpose**: Archives daily mean analysis data to MARS

**Key Features**:
- Modifies GRIB headers before archival
- Archives to marser database (or marsscratch)
- Processes all level types: sfc, hl, ml, pl
- Extracts and archives each parameter separately
- Handles special parameter mappings:
  - 173 (roughness) → 235244
  - 260649 → 263006
- Implements error logging
- Uses grib_filter for header modifications
- Sets MARS metadata: CLASS=RR, STREAM=DAME, TYPE=AN

**Workflow**:
1. Load environment and eccodes
2. For each level type and date:
   - Extract each parameter from daily mean file
   - Modify GRIB headers using rules file
   - Archive to MARS with correct metadata
   - Log any errors
3. Clean up temporary files

**Functions**:
- `extract_param()`: Filters specific parameter from GRIB file
- `archive_param()`: Archives parameter to MARS
- `error_log()`: Captures and logs MARS errors

---

### Other Archive Scripts
Similar structure for:
- `archive_daily_sums_fc.sh`: Archives daily forecast sums
- `archive_daily_minmax_fc.sh`: Archives daily extremes
- `archive_monthly_mean_an.sh`: Archives monthly means
- `archive_monthly_minmax_fc.sh`: Archives monthly extremes
- `archive_monthly_daysum_fc.sh`: Archives monthly day sums

---

## Retrieve and Archive Directory (bash/archiving/retrieve_and_archive/)

### submit_fetch.sh
**Purpose**: Submits data fetching jobs

---

### submit_archival_prep.sh
**Purpose**: Submits archival preparation jobs

---

## Missing Data Directory (bash/archiving/missing_data/)

### mars_checker.sh
**Purpose**: Comprehensive MARS data availability checker

**Key Features**:
- Checks data in both main MARS and marsscratch databases
- Supports year ranges (2000-2005), comma-separated years, or single years
- Supports month ranges (1-12) or specific months
- Generates detailed availability reports
- Calculates summary statistics

**Status Codes**:
- COMPLETE: Data in both MARS and marsscratch
- TRANSFER: Data only in marsscratch (needs transfer)
- SUBMIT: Data missing in both (needs processing)
- PARTIAL: Data in MARS but not marsscratch

**Functions**:
- `get_days_in_month()`: Handles leap years
- `check_mars_data()`: Queries MARS for data availability
- `get_status()`: Determines status from entry counts
- `parse_year_range()`: Parses year specifications
- `parse_month_range()`: Parses month specifications

**Output**: `availability_YYYYMMDD.txt` with detailed report


---


## Other scripts in the Job Submitters Directory (bash/job_submitters/)

Some of them were used to submit a point verification script
using harp, but have not been used in a while. USe at your own risk.

### run_verif_carra2.sh
**Purpose**: Runs HARP verification for CARRA2 data against observations

**Key Features**:
- SLURM: 64GB memory, QOS=nf
- Loads R/4.2.2 environment
- Runs point verification using HARP
- Processes multiple parameter sets:
  - Default parameters
  - T2m, S10m (temperature, wind)
  - RH2m, Pmsl (humidity, pressure)
  - CCtot, AccPcp12h (clouds, precipitation)
  - S, T (vertical profiles)
- Compares against ERA5 vertical profiles
- Transfers plots to visualization VM (136.156.128.148)

**Functions**:
- `run_verif_current()`: Runs verification for current periods
- `copy_plots()`: Transfers HARP verification plots
- `copy_vprof_plots()`: Transfers vertical profile plots

**Configuration**:
- Uses `config_carra2_prod.yml` for production
- Reads periods from `periods.txt`
- Verification plots stored in `/ec/res4/scratch/nhd/verification/plots/`

---

### vobs2sql.sh
**Purpose**: Converts observation data (vobs) to SQL database

---

### vfld2sql.sh
**Purpose**: Converts field data (vfld) to SQL database

---

### run_sql_conv.sh
**Purpose**: Orchestrates SQL conversion processes

---

### update_current_periods.sh
**Purpose**: Updates the periods.txt file with current processing status

---

## Configuration Directory (bash/archiving/config/)

### config_archive.sh
**Purpose**: Configuration file for archival parameters and paths

---

### config_archive_selected.sh
**Purpose**: Configuration for selected archival operations

---

### load_eccodes.sh
**Purpose**: Loads eccodes module for GRIB processing

---

## Python Scripts

### python/data_fetchers/fetch_data_yearly.py
**Purpose**: Fetches CARRA2 data from MARS for yearly processing

---

### bash/archiving/ecf_submitters/bin/calc_daily_minmax.py
**Purpose**: Python implementation for calculating daily minimum and maximum values

---

### bash/archiving/ecf_submitters/bin/calc_monthly_minmax.py
**Purpose**: Python implementation for calculating monthly minimum and maximum values

---

## Workflow Summary


1. **Data Availability Check**
   - `run_new_period.py` checks for new periods ready for processing
   - Compares last archived period with current data availability

2. **Suite Submission**
   - `submit_ecf_suite.sh` creates ECFlow suite
   - `ecfproj_start` initializes suite with configuration

3. **Daily Processing**
   - Daily means: `daily_mean_an_insta_*.sh` (sfc, ml, pl, hl)
   - Daily sums: `daily_sum_fc_accum_sfc.sh`
   - Daily extremes: `daily_minmax_fc_sfc.sh`

4. **Monthly Aggregation**
   - Monthly means: `monthly_means_an_insta.sh`, `monthly_means_fc_sfc.sh`
   - Monthly sums: `monthly_means_of_daily_sums.sh`
   - Monthly extremes: `monthly_minmax.sh`

5. **Data Retrieval and Preparation**
   - `fetch_and_prepare_for_archival_from_fac2.sh` fetches from marsscratch
   - Creates archival scripts for fac2

6. **Archival to MARS**
   - `submit_all_archival_scripts.sh` submits all archival jobs
   - Individual archive scripts modify headers and archive to MARS
   - Error logging and validation

7. **Error Correction**
   - `fix_mars_errors.sh` analyzes errors and generates fixes
   - Re-submission of corrected archive commands

8. **Monitoring**
   - `update_current_periods.sh` tracks processing status and submits new period

---

---

## Important Paths

- **Processing Output**: `$MEANS_OUTPUT/$origin/$YYYY/$MM/`
- **Archival Staging**: `$MEANS_OUTPUT_FAC2/$origin/$YYYY/$MM/`
- **Fetch Dump**: `/ec/res4/scratch/nhd/mars-pull/carra2/fetch_to_archive/`
- **Verification Plots**: `/ec/res4/scratch/nhd/verification/plots/CARRA2/`

---

## Parameter Sets

The pipeline processes multiple parameter sets defined in environment variables:
- `CARRA_PAR_AN_SFC`: Surface analysis parameters
- `CARRA_PAR_AN_ML`: Model level parameters
- `CARRA_PAR_AN_PL`: Pressure level parameters
- `CARRA_PAR_AN_HL`: Height level parameters
- `CARRA_PAR_FC_ACC`: Accumulated forecast parameters

---

