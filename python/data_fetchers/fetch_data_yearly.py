import os
import subprocess
import yaml
import argparse
from datetime import datetime
import shutil

# Define a global variable to set paths
proj_lib_path = None

def init():
    global proj_lib_path
    proj_lib_path = os.environ.get('ECFPROJ_LIB')

class Stream:
    def __init__(self, data):
        self.BEG_DATE = data.get('BEG_DATE')
        self.END_DATE = data.get('END_DATE')
        self.USER = data.get('USER')
        self.ACTIVE = data.get('ACTIVE', False)
        self.PROGLOG = data.get('PROGLOG')

class OBSPath:
    def __init__(self, data):
        self.LOCALPATH = data.get('LOCALPATH')

class Config:
    def __init__(self):
        self.STREAMS = {}
        self.OBS = {}

def read_yaml(filename):
    with open(filename, 'r') as file:
        data = yaml.safe_load(file)
    config = Config()
    config.STREAMS = {k: Stream(v) for k, v in data['STREAMS'].items()}
    config.OBS = {k: OBSPath(v) for k, v in data['OBS'].items()}
    return config

def check_progress(stream_name, log_path):
    try:
        with open(log_path, 'r') as file:
            for line in file:
                if line.startswith('DTG='):
                    return line.split()[0][4:]  # Extract DTG value
    except FileNotFoundError:
        return None
    return None

def execute_ecp_command(data_type, year, year_dir):
    print(f"Executing ecp for {data_type} to {year_dir}")
    cmd = ['ecp', f'ec:/fac2/CARRA2/obs/{data_type}/{year}/*', year_dir]
    print(f"COMMAND {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

def run_ecfsdir(data_type, year, year_dir):
    script_path = "ecfsdir -o"
    arg1 = f"ec:/fac2/CARRA2/obs/OSISAF_v2_20240424/{year}"
    arg2 = f"{year_dir}/{year}"
    cmd = ['ksh', script_path, arg1, arg2]
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(f"Error: {result.stderr}")
    return result.stdout

def execute_osisaf_commands(data_type, year, destination_path):
    print(f"Executing ecfsdir and rsync for {data_type} to {year}")
    year_dir = os.path.join(destination_path, year)
    os.makedirs(year_dir, exist_ok=True)

    output = run_ecfsdir(data_type, year, year_dir)
    print(f"Script output:\n{output}")

    rsync_cmd = ['rsync', '-vaux', f'{year_dir}/{year}/??/', year_dir]
    cmd = " ".join(rsync_cmd)
    #print(f"rsync COMMAND {' '.join(rsync_cmd)}")
    print(f"rsync COMMAND: {cmd}")
    subprocess.run(cmd, check=True,shell=True)

    clean_dir = f"{year_dir}/{year}"
    print(f"Cleaning up directory {clean_dir}")
    shutil.rmtree(clean_dir)

def execute_actions(year, obs):
    data_types = ["S3SICE", "MODIS", "AVHRR", "OSISAF"]
    current_year = int(year)
    
    for data_type in data_types:
        destination_path = obs[data_type].LOCALPATH
        if not destination_path:
            raise ValueError(f"No destination path found for {data_type}")

        year_dir = os.path.join(destination_path, year)
        if data_type == "S3SICE" and current_year >= 2021:
            os.makedirs(year_dir, exist_ok=True)
            execute_ecp_command(data_type, year, year_dir)
        elif data_type == "AVHRR" and 1985 <= current_year <= 2000:
            os.makedirs(year_dir, exist_ok=True)
            execute_ecp_command(data_type, year, year_dir)
        elif data_type == "MODIS" and 2000 <= current_year <= 2019:
            os.makedirs(year_dir, exist_ok=True)
            execute_ecp_command(data_type, year, year_dir)
        elif data_type == "OSISAF":
            execute_osisaf_commands(data_type, year, destination_path)

def main():
    init()
    print(f"The path of the script is: {proj_lib_path}")

    parser = argparse.ArgumentParser()
    parser.add_argument('-config', default='streams.yml', help='Path to the YAML configuration file')
    args = parser.parse_args()

    try:
        config = read_yaml(args.config)
    except Exception as e:
        print(f"Failed to read YAML file: {e}")
        return

    for stream_name, stream in config.STREAMS.items():
        if not stream.ACTIVE:
            print(f"Stream {stream_name} is inactive, skipping.")
            continue

        current_dtg = check_progress(stream_name, stream.PROGLOG)
        if current_dtg is None:
            print(f"Failed to check progress for {stream_name}")
            continue

        try:
            current_time = datetime.strptime(current_dtg, "%Y%m%d%H")
        except ValueError:
            print(f"Failed to parse DTG {current_dtg}")
            continue

        if current_time.month == 12:
            next_year = str(current_time.year + 1)
            print(f"Executing actions for December, preparing for {next_year}...")
            try:
                execute_actions(next_year, config.OBS)
            except Exception as e:
                print(f"Failed to execute actions for {stream_name}: {e}")
        else:
            print(f"No action needed for stream {stream_name} (current DTG: {current_dtg})")

if __name__ == "__main__":
    main()
