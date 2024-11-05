import os
import yaml
from datetime import datetime, timedelta
from pathlib import Path
import sys

def load_configs(config_file="mars_config.yaml"):
    """Load configurations from YAML file."""
    try:
        with open(config_file, "r") as f:
            configs = yaml.safe_load(f)
        return configs["retrieval_configs"]
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_file}' not found.")
        return []
    except yaml.YAMLError as e:
        print(f"Error parsing YAML configuration: {e}")
        return []
    except KeyError:
        print("Error: 'retrieval_configs' key not found in configuration file.")
        return []


def create_mars_statement(params_dict):
    """Create a MARS retrieval statement from dictionary of parameters."""
    statement = "retrieve,\n"
    for key, value in params_dict.items():
        # Remove any newlines and extra spaces from the values
        if isinstance(value, str):
            value = value.replace("\n", "").replace("  ", " ").strip()
        statement += f"{key}={value},\n"
    return statement.rstrip(",\n")


def generate_filename(type_val, levtype, stream, param):
    """Generate filename for MARS statement file."""
    # Clean date string (remove /to/ if present)
    #if "/to/" in date_val:
    #    date_val = date_val.split("/to/")[0]

    # Create filename
    filename = f"{type_val}_{stream}_{levtype}_{param}.grib2"
    return filename


def create_directory(type_val, stream, levtype,tmp_path_fetch):
    """Create and return directory path."""

    #special names:
    # For parameters 201 and 202 of type_val fc 
    # and stream dame use minmax as directory suffix
    # For parameters other than this use 
    dir_name = os.path.join(tmp_path_fetch, f"{type_val}_{stream}_{levtype}")
    Path(dir_name).mkdir(parents=True, exist_ok=True)
    return dir_name

def create_directory_extra(type_val,stream, levtype, extra, tmp_path_fetch):
    """Create and return directory path for minmax or sums"""
    #special names:
    # For parameters 201 and 202 of type_val fc 
    # and stream dame use minmax as directory suffix
    # For parameters other than this use 
    dir_name = os.path.join(tmp_path_fetch, f"{type_val}_{stream}_{levtype}_{extra}")
    Path(dir_name).mkdir(parents=True, exist_ok=True)
    return dir_name

def process_mars_statements(start_date, end_date, tmp_path_fetch, config_file="mars_config.yaml"):
    """Process MARS statements for given date range."""
    mm_params = ['201','202']
    # Load configurations from YAML file
    retrieval_configs = load_configs(config_file)

    if not retrieval_configs:
        print("No configurations loaded. Exiting.")
        return []

    created_files = []

    for config in retrieval_configs:

        # Create directory for this configuration
        #if config["type"] != "fc":
        #    output_dir = create_directory( config["type"], config["stream"], config["levtype"],tmp_path_fetch)

        # Split parameters
        params = config["param"].replace("\n", "").replace(" ", "").split("/")

        # Create a separate file for each parameter
        for param in params:
            # Create a copy of the config for this parameter
            param_config = config.copy()
            param_config["param"] = param
            if config["stream"] == "dame":
                param_config["date"] = f'{start_date}/to/{end_date}'
            elif config["stream"] == "moda":
                param_config["date"] = f'{start_date}'
            else:
                stream = config["stream"]
                print(f"Unknown stream: {stream}")
                sys.exit(1)

            # Generate output filename
            output_filename = generate_filename(
                config["type"], config["levtype"], config["stream"], param
            )


            if config["type"] == "fc" and config["stream"] == "dame" and param in mm_params:
                output_dir = create_directory_extra( config["type"], config["stream"], config["levtype"],"minmax",tmp_path_fetch)
            elif config["type"] == "fc" and config["stream"] == "moda" and param in mm_params:
                output_dir = create_directory_extra( config["type"], config["stream"], config["levtype"],"minmax",tmp_path_fetch)
            elif config["type"] == "fc" and config["stream"] == "dame" and param not in mm_params:
                output_dir = create_directory_extra( config["type"], config["stream"], config["levtype"],"sums",tmp_path_fetch)
            elif config["type"] == "fc" and config["stream"] == "moda" and param not in mm_params:
                output_dir = create_directory_extra( config["type"], config["stream"], config["levtype"],"sums",tmp_path_fetch)
            else:
                output_dir = create_directory( config["type"], config["stream"], config["levtype"],tmp_path_fetch)
                #print("standard output path for this case")
            # Add full path to output filename
            full_output_path = str(Path(output_dir) / output_filename)
            param_config["target"] = f'"{full_output_path}"'

            # Create MARS statement
            mars_statement = create_mars_statement(param_config)
            # Create script filename
            script_filename = f"fetch_script_{config['type']}_{config['levtype']}_{param}.mars"
            script_path = os.path.join(tmp_path_fetch,"scr",script_filename)

            # Write statement to file
            with open(script_path, "w") as f:
                f.write(mars_statement)

            # Execute mars command
            os.system(f"mars {script_path}")

            created_files.append(script_path)

    return created_files


def main():
    # Example usage
    start_date = "1985-10-01"
    end_date = "1985-10-31"
    period=start_date[0:4]+start_date[5:7]
    print(f"Doing {period}")
    tmp_path_fetch = "/ec/res4/scratch/nhd/mars-pull/carra2/fetch_to_archive"
    if not os.path.isdir(os.path.join(tmp_path_fetch,period)):
        os.makedirs(os.path.join(tmp_path_fetch,period))
    tmp_path_fetch = os.path.join(tmp_path_fetch,period)
    if not os.path.isdir(os.path.join(tmp_path_fetch,"scr")):
        os.makedirs(os.path.join(tmp_path_fetch,"scr"))

    print("Processing MARS statements...")
    created_files = process_mars_statements(start_date, end_date, tmp_path_fetch)

    #print("\nCreated files and directories:")
    # Print created directories
    #print("\nCreated directories:")
    #for file in created_files:
    #    directory = os.path.dirname(file)
    #    if directory:
    #        print(f"- {directory}")

    # Print created files
    print("\nCreated files:")
    for file in created_files:
        print(f"- {file}")


if __name__ == "__main__":
    main()

## Created/Modified files during execution:
# print("\nCreated/Modified files during execution:")
# for config in load_configs():
#  dir_name = f"{config['type']}_{config['stream']}_{config['levtype']}"
#  print(f"- Directory: {dir_name}/")
#  params = config['param'].replace('\n', '').replace(' ', '').split('/')
#  for param in params:
#      script_file = f"{dir_name}/mars_script_{config['type']}_{config['levtype']}_{param}.mars"
#      output_file = f"{dir_name}/mars_call_{config['type']}_1985-09-01_{config['levtype']}_{config['stream']}_{param}.grib2"
#      print(f"  - {script_file}")
#      print(f"  - {output_file}")
