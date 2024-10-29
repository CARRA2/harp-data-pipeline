import os
import yaml
from datetime import datetime, timedelta


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


def generate_filename(type_val, date_val, levtype, stream):
    """Generate filename for MARS statement file."""
    # Clean date string (remove /to/ if present)
    if "/to/" in date_val:
        date_val = date_val.split("/to/")[0]

    # Create filename
    filename = f"mars_call_{type_val}_{date_val}_{levtype}_{stream}.grib2"
    return filename


def process_mars_statements(start_date, end_date, config_file="mars_config.yaml"):
    """Process MARS statements for given date range."""
    # Load configurations from YAML file
    retrieval_configs = load_configs(config_file)

    if not retrieval_configs:
        print("No configurations loaded. Exiting.")
        return []

    created_files = []

    for config in retrieval_configs:
        # Set date range
        config["date"] = f"{start_date}/to/{end_date}"

        # Generate output filename
        output_filename = generate_filename(
            config["type"], start_date, config["levtype"], config["stream"]
        )

        # Add target parameter with output filename
        config["target"] = f'"{output_filename}"'

        # Create MARS statement
        mars_statement = create_mars_statement(config)

        # Create script filename
        script_filename = f"mars_script_{config['type']}_{config['levtype']}.mars"

        # Write statement to file
        with open(script_filename, "w") as f:
            f.write(mars_statement)

        # Execute mars command
        os.system(f"mars {script_filename}")

        created_files.append(script_filename)

    return created_files


def main():
    # Example usage
    start_date = "1985-09-01"
    end_date = "1985-09-30"

    print("Processing MARS statements...")
    created_files = process_mars_statements(start_date, end_date)

    print("\nCreated files:")
    for file in created_files:
        print(f"- {file}")


if __name__ == "__main__":
    main()
    # Created/Modified files during execution:
    #print("Created files:")
    #for file in ["mars_configs.yaml"]:
    #    print(f"- {file}")
    #for file_type in ["an"]:
    #    for levtype in ["sfc", "hl"]:
    #        filename = f"mars_script_{file_type}_{levtype}.mars"
    #        print(f"- {filename}")
