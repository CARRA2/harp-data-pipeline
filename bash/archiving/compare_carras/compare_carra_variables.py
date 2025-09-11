#!/usr/bin/env python3

import os
import subprocess

def parse_variables(var_string):
    """Parse slash-separated variable string into a set of integers."""
    if not var_string:
        return set()
    return set(int(x) for x in var_string.split('/') if x.strip())

def load_env_from_file(config_file):
    """Load environment variables from a bash config file."""
    if not os.path.exists(config_file):
        print(f"Warning: Config file {config_file} not found")
        return {}
    
    try:
        # Source the file and capture environment variables
        cmd = f'source "{config_file}" && env'
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, executable='/bin/bash')
        
        if result.returncode != 0:
            print(f"Warning: Failed to source {config_file}")
            return {}
        
        env_vars = {}
        for line in result.stdout.splitlines():
            if '=' in line:
                key, value = line.split('=', 1)
                env_vars[key] = value
        
        return env_vars
    except Exception as e:
        print(f"Error loading config file {config_file}: {e}")
        return {}

def compare_variable_groups(name, carra2_vars, carratu_vars):
    """Compare two variable groups and print detailed analysis."""
    carra2_set = parse_variables(carra2_vars)
    carratu_set = parse_variables(carratu_vars)
    
    print(f"\n--- {name} ---")
    print(f"CARRA2 count: {len(carra2_set)}")
    print(f"CARRA-TU count: {len(carratu_set)}")
    
    only_carra2 = carra2_set - carratu_set
    only_carratu = carratu_set - carra2_set
    common = carra2_set & carratu_set
    
    print("Only in CARRA2:")
    if only_carra2:
        print("/".join(str(x) for x in sorted(only_carra2)))
    else:
        print("(none)")
    
    print("Only in CARRA-TU:")
    if only_carratu:
        print("/".join(str(x) for x in sorted(only_carratu)))
    else:
        print("(none)")
    
    print("Common:")
    if common:
        print("/".join(str(x) for x in sorted(common)))
    else:
        print("(none)")
    
    return carra2_set, carratu_set

def get_variables_from_env(env_vars, dataset_name):
    """Extract CARRA variables from environment variables."""
    var_mapping = {
        "AN_HL": "CARRA_PAR_AN_HL",
        "AN_PL": "CARRA_PAR_AN_PL", 
        "AN_SFC": "CARRA_PAR_AN_SFC",
        "AN_ML": "CARRA_PAR_AN_ML",
        "FC_SFC": "CARRA_PAR_FC_SFC",
        "FC_SFC_MM": "CARRA_PAR_FC_SFC_MM",
        "FC_SFC_IN": "CARRA_PAR_FC_SFC_IN",
        "FC_ACC": "CARRA_PAR_FC_ACC"
    }
    
    variables = {}
    for short_name, env_name in var_mapping.items():
        variables[short_name] = env_vars.get(env_name, "")
    
    return variables

def main():
    print("=== CARRA2 vs CARRA-TU Variable Comparison ===\n")
    
    # Default config file paths - can be overridden by command line args
    carra2_config = "/perm/nhd/CARRA2/harp-data-pipeline/bash/archiving/config/config_archive.sh"
    carratu_config = "/home/nhd/scripts/carra/carra_means/bashscripts/ecf_conf/bin/env.sh"  # None # Will use fallback hardcoded values if not provided
    
    # Check for command line arguments
    import sys
    if len(sys.argv) > 1:
        carra2_config = sys.argv[1]
    if len(sys.argv) > 2:
        carratu_config = sys.argv[2]
    
    # Load CARRA2 variables from config file
    print(f"Loading CARRA2 variables from: {carra2_config}")
    carra2_env = load_env_from_file(carra2_config)
    carra2_vars = get_variables_from_env(carra2_env, "CARRA2")
    
    # Load CARRA-TU variables from config file or use fallback
    if carratu_config:
        print(f"Loading CARRA-TU variables from: {carratu_config}")
        carratu_env = load_env_from_file(carratu_config)
        carratu_vars = get_variables_from_env(carratu_env, "CARRA-TU")
    else:
        print("Using hardcoded CARRA-TU variables (no config file provided)")
        # CARRA-TU variables (from commented lines in config)
        carratu_vars = {
            "AN_HL": "10/54/130/157/246/247/3031",
            "AN_PL": "60/75/76/129/130/131/132/157/246/247/3014/260028/260238/260257",
            "AN_SFC": "33/34/134/151/165/166/167/207/235/3020/3073/3074/3075/228141/228164/260057/260107/260108/260242/260260/260509/260289",
            "AN_ML": "75/76/130/131/132/133/246/247/260028/260155/260257",
            "FC_SFC": "49/201/202/260646/260647/260015",
            "FC_SFC_MM": "49/201/202/260646/260647",
            "FC_SFC_IN": "260015/78/79/260648",
            "FC_ACC": "228228/235015/260645/174008/260430/260259/235072/146/147/235019/235071/47/260264/176/169/210/177/175/211/178/179/235017/235018"
        }
    
    print("CARRA2 Variables:")
    for name, vars in carra2_vars.items():
        print(f"{name}: {vars}")
    
    print("\nCARRA-TU Variables:")
    for name, vars in carratu_vars.items():
        print(f"{name}: {vars}")
    
    print("\n=== Detailed Analysis ===")
    
    all_carra2 = set()
    all_carratu = set()
    
    # Compare each variable group
    for name in carra2_vars.keys():
        carra2_set, carratu_set = compare_variable_groups(
            name, carra2_vars[name], carratu_vars[name]
        )
        all_carra2.update(carra2_set)
        all_carratu.update(carratu_set)
    
    print("\n=== Overall Summary ===")
    print(f"Total unique variables in CARRA2: {len(all_carra2)}")
    print(f"Total unique variables in CARRA-TU: {len(all_carratu)}")
    
    overall_only_carra2 = all_carra2 - all_carratu
    overall_only_carratu = all_carratu - all_carra2
    overall_common = all_carra2 & all_carratu
    
    print(f"\nVariables only in CARRA2 ({len(overall_only_carra2)}):")
    if overall_only_carra2:
        print("/".join(str(x) for x in sorted(overall_only_carra2)))
    else:
        print("(none)")
    
    print(f"\nVariables only in CARRA-TU ({len(overall_only_carratu)}):")
    if overall_only_carratu:
        print("/".join(str(x) for x in sorted(overall_only_carratu)))
    else:
        print("(none)")
    
    print(f"\nCommon variables ({len(overall_common)}):")
    if overall_common:
        # Print in groups of 10 for readability
        common_sorted = sorted(overall_common)
        for i in range(0, len(common_sorted), 10):
            chunk = common_sorted[i:i+10]
            print("/".join(str(x) for x in chunk))
    else:
        print("(none)")

if __name__ == "__main__":
    main()
