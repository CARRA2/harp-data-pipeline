import os
import yaml
from datetime import datetime, timedelta
from pathlib import Path
import sys

import subprocess
import shlex
from pathlib import Path

def get_dates(period:str) -> None:
    from datetime import datetime
    import calendar
    
    # Extract year and month
    year = int(period[:4])
    month = int(period[4:])
    
    # Create first day of month
    start_date = f"{year}-{month:02d}-01"

    # Get last day of month using calendar
    last_day = calendar.monthrange(year, month)[1]
    end_date = f"{year}-{month:02d}-{last_day}"
    return start_date, end_date

def get_sorted_levels(file_path):
    # First command: grib_ls -p level $FILE | sort -u | grep -v messages | grep -v grib2 | grep -v lev | sort -n
    cmd1 = f"grib_ls -p level {file_path}"
    cmd2 = "sort -u"
    cmd3 = "grep -v messages"
    cmd4 = "grep -v grib2"
    cmd5 = "grep -v lev"
    cmd6 = "sort -n"
  
    # Execute the first command
    p1 = subprocess.Popen(shlex.split(cmd1), stdout=subprocess.PIPE)
    p2 = subprocess.Popen(shlex.split(cmd2), stdin=p1.stdout, stdout=subprocess.PIPE)
    p3 = subprocess.Popen(shlex.split(cmd3), stdin=p2.stdout, stdout=subprocess.PIPE)
    p4 = subprocess.Popen(shlex.split(cmd4), stdin=p3.stdout, stdout=subprocess.PIPE)
    p5 = subprocess.Popen(shlex.split(cmd5), stdin=p4.stdout, stdout=subprocess.PIPE)
    p6 = subprocess.Popen(shlex.split(cmd6), stdin=p5.stdout, stdout=subprocess.PIPE)
  
    # Get the output
    out = p6.communicate()[0].decode('utf-8').strip()
    #levels = out.replace(' ', '/')
    levels = '/'.join(x.strip() for x in out.split('\n'))
  
    return levels

def get_grib_count(file_path):
    try:
        # Run grib_count command
        cmd = f"grib_count {file_path}"
        process = subprocess.run(shlex.split(cmd), 
                               capture_output=True, 
                               text=True, 
                               check=True)
        
        # Get the output (should be a number)
        count = process.stdout.strip()
        return count
        
    except subprocess.CalledProcessError as e:
        print(f"Error running grib_count: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

def load_configs(config_file="mars_config_archive.yaml"):
    """Load configurations from YAML file."""
    try:
        with open(config_file, "r") as f:
            configs = yaml.safe_load(f)
        return configs["archival_configs"]
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_file}' not found.")
        return []
    except yaml.YAMLError as e:
        print(f"Error parsing YAML configuration: {e}")
        return []
    except KeyError:
        print("Error: 'archival_configs' key not found in configuration file.")
        return []


def create_mars_statement(means_dict):
    """Create a MARS archival statement from dictionary of parameters."""
    statement = "archive,\n"
    for key, value in means_dict.items():
        if key != "means_type":
            # Remove any newlines and extra spaces from the values
            if isinstance(value, str):
                value = value.replace("\n", "").replace("  ", " ").strip()
            statement += f"{key}={value},\n"
    return statement.rstrip(",\n")


def generate_filename(type_val, date_val, levtype, stream, param):
    """Generate filename for MARS statement file."""
    # Clean date string (remove /to/ if present)
    if "/to/" in date_val:
        date_val = date_val.split("/to/")[0]

    # Create filename
    filename = f"mars_call_{type_val}_{date_val}_{levtype}_{stream}_{param}.grib2"
    return filename


def create_directory(type_val, stream, levtype,tmp_path_fetch):
    """Create and return directory path."""
    dir_name = os.path.join(tmp_path_fetch, f"{type_val}_{stream}_{levtype}")
    Path(dir_name).mkdir(parents=True, exist_ok=True)
    return dir_name


def get_files_in_directory(directory_path):
  # Convert string path to Path object
  path = Path(directory_path)
  
  # Get only files (not directories) from the specified path
  files = [f.name for f in path.iterdir() if f.is_file()]
  
  # Sort the files (optional, but usually helpful)
  files.sort()
  
  return files





def process_mars_statements(start_date, end_date, tmp_path_fetch, config_file="mars_config_archive.yaml"):
    """Process MARS statements for given date range.
    The files are to be found in the path
    starting with tmp_path_fetch and under these directories
    an_moda_sfc
    an_moda_pl
    an_moda_ml
    an_moda_hl
    an_dame_sfc
    an_dame_pl
    an_dame_ml
    an_dame_hl
    fc_moda_sfc_sums
    fc_dame_sfc_sums
    fc_moda_sfc_minmax
    fc_dame_sfc_minmax
    """
    # Load configurations from YAML file
    archival_configs = load_configs(config_file)
    scripts_path = os.path.join(tmp_path_fetch,"archival_scripts")
    if not os.path.isdir(scripts_path):
        os.makedirs(scripts_path)

    if not archival_configs:
        print("No configurations loaded. Exiting.")
        return []

    created_files = []
    for config in archival_configs:
        # the path for the data
        output_dir = os.path.join(tmp_path_fetch, config["data_path"])
        #get the files to archive
        if not output_dir:
            print(f"Not processing {output_dir}, since not available")
            continue
        else:
            files_to_archive = get_files_in_directory(output_dir)
        
        print(f"Processing files in {output_dir}")
        # Create a separate archive script for each file:
        for output_filename in files_to_archive:
             
            # Create a copy of the config for this parameter
            param_config = config.copy()
            del param_config["data_path"]
            if config["stream"] == "dame":
                param_config["date"] = f'{start_date}/to/{end_date}'
            elif config["stream"] == "moda":
                param_config["date"] = f'{start_date}'
            else:
                print(f"Unknown stream: {stream}")
                sys.exit(1)

            #output_filename = generate_filename(
            #config["type"], start_date, config["levtype"], config["stream"], param
            # )

            # Add full path to output filename
            full_output_path = str(Path(output_dir) / output_filename)
            param_config["source"] = f'"{full_output_path}"'
            print(f"Going through {full_output_path}")
            # extract param from filename 
            param =  output_filename.split("_")[-1].replace(".grib2","")
            param_config["param"] =  param
            param_config["levelist"] =  get_sorted_levels(full_output_path)
            param_config["expect"] =  get_grib_count(full_output_path)

            # Create MARS statement
            mars_statement = create_mars_statement(param_config)

            # Create script filename
            script_filename = f"archive_{config['type']}_{config['stream']}_{config['levtype']}_{param}.mars"
            script_path = os.path.join(scripts_path,script_filename)

            # Write statement to file
            with open(script_path, "w") as f:
                f.write(mars_statement)

            # Execute mars command
            # only fac2 can do this, so producing now
            # the list of the scripts from nhd and running later from fac2
            #os.system(f"mars {script_path}")
            created_files.append(script_path)

    return created_files


def create_slurm_script(script_list, output_slurm_file="run_mars_jobs_from_fac2.sh"):
    """
    Creates a SLURM script that executes MARS commands for all scripts in the list
    
    Args:
        script_list (list): List of paths to MARS scripts
        output_slurm_file (str): Name of the output SLURM script file
    """
    
    # SLURM header
    slurm_header = """#!/usr/bin/env bash
#SBATCH --error=log_means_archive.%j.err
#SBATCH --output=log_means_archive.%j.out
#SBATCH --job-name=means_archive
#SBATCH --qos=nf
#SBATCH --mem-per-cpu=16000
#SBATCH --account="c3srrp"

"""
    
    # Create the SLURM script content
    script_content = slurm_header
    
    # Add mars commands for each script
    for script_path in script_list:
        script_content += f"mars {script_path}\n"
    
    # Write the content to the SLURM script file
    with open(output_slurm_file, 'w') as f:
        f.write(script_content)
    
    # Make the script executable
    import os
    os.chmod(output_slurm_file, 0o755)
    
    print(f"Created SLURM script: {output_slurm_file}")
    


def main():
    period = sys.argv[1]
    tmp_path_fetch = sys.argv[2]
    start_date, end_date = get_dates(period)

    #tmp_path_fetch = "/ec/res4/scratch/nhd/mars-pull/carra2/fetch_to_archive"
    #start_date = sys.argv[1]
    #end_date = sys.argv[2]
    # Example usage
    #start_date = "1985-10-01"
    #end_date = "1985-10-31"
    #period=start_date[0:4]+start_date[5:7]
    print(f"Doing {period}")
    if not os.path.isdir(os.path.join(tmp_path_fetch,period)):
        os.makedirs(os.path.join(tmp_path_fetch,period))
    tmp_path_fetch = os.path.join(tmp_path_fetch,period)
    print("Creating MARS statements for archiving...")
    #for test
    #created_files = process_mars_statements(start_date, end_date, tmp_path_fetch,"mars_config_test.yaml")

    created_files = process_mars_statements(start_date, end_date, tmp_path_fetch)

    # Print created files
    print("\nCreated files:")
    for file in created_files:
        print(f"- {file}")
    slurm_scr = f"archive_{period}_from_fac2.sh"
    create_slurm_script(created_files,slurm_scr)

if __name__ == "__main__":
    main()

