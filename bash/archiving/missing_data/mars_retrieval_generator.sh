#!/bin/bash

# MARS Retrieval Command Generator
# Generates MARS retrieve commands for missing parameters based on batch analysis output
# Creates optimized commands that pack multiple missing dates per month into single calls

# Function to get days in month
get_days_in_month() {
    local year=$1
    local month=$2
    
    if [ $month -eq 2 ]; then
        if [ $((year % 4)) -eq 0 ] && [ $((year % 100)) -ne 0 ] || [ $((year % 400)) -eq 0 ]; then
            echo 29
        else
            echo 28
        fi
    elif [ $month -eq 4 ] || [ $month -eq 6 ] || [ $month -eq 9 ] || [ $month -eq 11 ]; then
        echo 30
    else
        echo 31
    fi
}

# Function to generate date range for a month
generate_date_range() {
    local year=$1
    local month=$2
    local days=$(get_days_in_month $year $month)
    
    local formatted_month=$(printf "%02d" $month)
    local formatted_last_day=$(printf "%02d" $days)
    
    echo "$year-$formatted_month-01/to/$year-$formatted_month-$formatted_last_day"
}

# Function to parse analysis file and generate retrieval commands
generate_retrieval_commands() {
    local analysis_file=$1
    local output_file="${analysis_file%.txt}_retrieval_commands.txt"
    local mars_script="${analysis_file%.txt}_retrieval_script.sh"
    
    echo "Generating MARS retrieval commands from: $analysis_file"
    echo "Output commands file: $output_file"
    echo "Output executable script: $mars_script"
    
    # Initialize output files
    cat > "$output_file" << EOF
# MARS Retrieval Commands
# Generated: $(date '+%Y-%m-%d %H:%M:%S')
# Source: $analysis_file
#
# These commands retrieve missing parameters identified in the batch analysis
# Commands are optimized to pack multiple missing parameters per month into single calls
#
EOF

    cat > "$mars_script" << EOF
#!/bin/bash
# MARS Retrieval Execution Script
# Generated: $(date '+%Y-%m-%d %H:%M:%S')
# Source: $analysis_file
#
# This script executes all MARS retrieve commands for missing parameters
# Run with: ./$mars_script

echo "========================================="
echo "MARS Missing Parameter Retrieval Script"
echo "========================================="
echo "Generated from: $analysis_file"
echo "Started: \$(date)"
echo ""

RETRIEVE_COUNT=0
ERROR_COUNT=0
SUCCESS_COUNT=0

EOF
    
    # Parse the analysis file to extract missing parameter information
    declare -A period_missing_params  # period_database -> "param1,param2,..."
    declare -A period_info           # period_database -> "year month database"
    
    local current_period=""
    local current_database=""
    local current_year=""
    local current_month=""
    
    echo "Parsing analysis file for missing parameters..."
    
    while IFS= read -r line; do
        # Look for period analysis headers
        if [[ "$line" =~ ^Analyzing:[[:space:]]+([0-9]{4}-[0-9]{2})[[:space:]]+\(([a-z]+)[[:space:]]+database\) ]]; then
            current_period="${BASH_REMATCH[1]}"
            current_database="${BASH_REMATCH[2]}"
            current_year="${current_period%-*}"
            current_month="${current_period#*-}"
            current_month=$((10#$current_month))  # Remove leading zero
            
            period_info["${current_period}_${current_database}"]="$current_year $current_month $current_database"
            continue
        fi
        
        # Look for completely missing parameters line
        if [[ "$line" =~ ^Completely[[:space:]]+missing[[:space:]]+parameters[[:space:]]+\([0-9]+\):[[:space:]]+(.+)$ ]]; then
            local missing_params="${BASH_REMATCH[1]}"
            if [[ -n "$current_period" && -n "$current_database" ]]; then
                period_missing_params["${current_period}_${current_database}"]="$missing_params"
            fi
            continue
        fi
        
        # Look for individual parameter lines that show complete absence (Days Found = 0)
        if [[ "$line" =~ ^([0-9]+)[[:space:]]+\|[[:space:]]+0[[:space:]]+\|[[:space:]]+[0-9]+[[:space:]]+\|[[:space:]]+[0-9]+[[:space:]]*$ ]]; then
            local param="${BASH_REMATCH[1]}"
            if [[ -n "$current_period" && -n "$current_database" ]]; then
                local key="${current_period}_${current_database}"
                if [[ -n "${period_missing_params[$key]}" ]]; then
                    period_missing_params["$key"]="${period_missing_params[$key]} $param"
                else
                    period_missing_params["$key"]="$param"
                fi
            fi
        fi
        
    done < "$analysis_file"
    
    echo "Found missing parameters for ${#period_missing_params[@]} period/database combinations"
    
    # Generate MARS commands for each period with missing parameters
    local command_count=0
    
    for key in "${!period_missing_params[@]}"; do
        local missing_params="${period_missing_params[$key]}"
        local info="${period_info[$key]}"
        
        # Skip if no missing parameters
        [[ -z "$missing_params" ]] && continue
        
        read year month database <<< "$info"
        local period="$year-$(printf "%02d" $month)"
        
        # Clean up and format parameter list
        local param_list=$(echo "$missing_params" | tr ' ' '\n' | sort -n | uniq | tr '\n' '/' | sed 's/\/$//')
        
        # Skip empty parameter lists
        [[ -z "$param_list" || "$param_list" == "/" ]] && continue
        
        command_count=$((command_count + 1))
        
        # Generate date range
        local date_range=$(generate_date_range $year $month)
        
        # Generate retrieve command for text file (static filename)
        local retrieve_command="retrieve,
class=rr,
date=$date_range,
expver=prod,
levtype=sfc,
origin=no-ar-pa,
param=$param_list,
stream=oper,
time=0000/0300/0600/0900/1200/1500/1800/2100,
type=an,
target=\"missing_${period}.grib\""
        
        # Write to commands file
        echo "# Command $command_count: Retrieve missing parameters for $period ($database database)" >> "$output_file"
        echo "# Missing parameters: $param_list" >> "$output_file"
        echo "$retrieve_command" >> "$output_file"
        echo "" >> "$output_file"
        
        # Add to executable script with proper date handling
        cat >> "$mars_script" << EOF
# Command $command_count: $period ($database database)
echo "Retrieving $period ($database database) - Parameters: $param_list"
TIMESTAMP=\$(date '+%Y%m%d_%H%M%S')
TARGET_FILE="missing_${period}_\${TIMESTAMP}.grib"
echo "Target file: \$TARGET_FILE"

if mars << EOFMARS
retrieve,
class=rr,
date=$date_range,
expver=prod,
levtype=sfc,
origin=no-ar-pa,
param=$param_list,
stream=oper,
time=0000/0300/0600/0900/1200/1500/1800/2100,
type=an,
target="\$TARGET_FILE"
EOFMARS
then
    echo "SUCCESS: Retrieved $period ($database database) to \$TARGET_FILE"
    SUCCESS_COUNT=\$((SUCCESS_COUNT + 1))
else
    echo "ERROR: Failed to retrieve $period ($database database)"
    ERROR_COUNT=\$((ERROR_COUNT + 1))
fi
RETRIEVE_COUNT=\$((RETRIEVE_COUNT + 1))
echo ""

EOF
    done
    
    # Add summary to commands file
    cat >> "$output_file" << EOF
# Summary:
# ========
# Total retrieve commands generated: $command_count
# Generated: $(date '+%Y-%m-%d %H:%M:%S')
#
# To execute all commands:
# chmod +x $mars_script
# ./$mars_script
EOF

    # Add summary and completion to executable script
    cat >> "$mars_script" << EOF

echo "========================================="
echo "MARS Retrieval Summary"
echo "========================================="
echo "Total commands executed: \$RETRIEVE_COUNT"
echo "Successful retrievals: \$SUCCESS_COUNT"
echo "Failed retrievals: \$ERROR_COUNT"
echo "Completed: \$(date)"

if [ \$ERROR_COUNT -eq 0 ]; then
    echo ""
    echo "All retrievals completed successfully!"
    exit 0
else
    echo ""
    echo "WARNING: \$ERROR_COUNT retrievals failed. Check output above for details."
    exit 1
fi
EOF

    # Make the script executable
    chmod +x "$mars_script"
    
    echo ""
    echo "Generated $command_count MARS retrieve commands"
    echo ""
    echo "Files created:"
    echo "  Commands file: $output_file"
    echo "  Executable script: $mars_script"
    echo ""
    echo "To execute all retrievals:"
    echo "  ./$mars_script"
    echo ""
    echo "Or execute individual commands from: $output_file"
}

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 <batch_analysis_file>

This script generates MARS retrieve commands for missing parameters identified
in a batch analysis file.

The input should be the output from mars_batch_analyzer.sh:
  *_batch_analysis.txt

FEATURES:
- Identifies completely missing parameters from the analysis
- Groups missing parameters by period/database for efficient retrieval  
- Generates optimized MARS retrieve commands
- Creates both a commands file and an executable script
- Handles both main and marssc database retrievals

EXAMPLE:
  $0 output_from_1991_1993_mars_commands_batch_analysis.txt

OUTPUT FILES:
  <input>_retrieval_commands.txt - Text file with all MARS commands
  <input>_retrieval_script.sh    - Executable script to run all retrievals

USAGE:
  # Generate the commands
  $0 analysis_file.txt
  
  # Execute all retrievals
  ./analysis_file_retrieval_script.sh
  
  # Or run individual commands from the commands file
EOF
}

# Main execution
if [ $# -ne 1 ]; then
    show_usage
    exit 1
fi

analysis_file="$1"

if [ ! -f "$analysis_file" ]; then
    echo "Error: File '$analysis_file' not found."
    exit 1
fi

# Basic validation of input file
if ! grep -q "MARS Batch Parameter Analysis" "$analysis_file"; then
    echo "Error: '$analysis_file' does not appear to be a MARS batch analysis file."
    echo "Expected file generated by mars_batch_analyzer.sh"
    exit 1
fi

echo "========================================="
echo "MARS Retrieval Command Generator"
echo "========================================="
echo "Analysis file: $analysis_file"
echo ""

generate_retrieval_commands "$analysis_file"