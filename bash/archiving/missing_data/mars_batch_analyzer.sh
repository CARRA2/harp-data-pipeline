#!/bin/bash

# MARS Batch Output Analyzer
# Analyzes "dirty" MARS output containing multiple list commands with headers and data
# Handles output from running mars with availability_1991_1993_mars_commands.txt

# Expected parameters (30 total based on mars_checker.sh)
EXPECTED_PARAMS=(31 34 78 79 134 151 165 166 167 172 207 235 3066 3073 3074 3075 174096 174098 228002 228141 228164 231063 260001 260038 260057 260107 260108 260242 260260 260650)

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

# Function to extract period from date range
extract_period_from_date() {
    local date_line=$1
    # Extract YYYY-MM from date=1992-05-01/to/1992-05-31 format
    if [[ "$date_line" =~ date=([0-9]{4})-([0-9]{2})-[0-9]{2}/to/([0-9]{4})-([0-9]{2})-[0-9]{2} ]]; then
        local start_year="${BASH_REMATCH[1]}"
        local start_month="${BASH_REMATCH[2]}"
        echo "$start_year-$start_month"
    fi
}

# Function to extract database type from command
extract_database_type() {
    local command_text=$1
    if [[ "$command_text" =~ database=marssc ]]; then
        echo "marssc"
    else
        echo "main"
    fi
}

# Function to analyze batch MARS output
analyze_batch_mars_output() {
    local input_file=$1
    local output_file="${input_file}_batch_analysis.txt"
    
    echo "Analyzing batch MARS output from: $input_file"
    echo "Creating comprehensive analysis: $output_file"
    
    # Initialize analysis file
    cat > "$output_file" << EOF
# MARS Batch Parameter Analysis
# Generated: $(date '+%Y-%m-%d %H:%M:%S')
# Source: $input_file
#
# Analysis of multiple MARS list commands from batch execution
# Expected parameters: ${EXPECTED_PARAMS[@]}
#
EOF
    
    local temp_dir="/tmp/mars_batch_$$"
    mkdir -p "$temp_dir"
    
    # Parse the input file and separate different MARS list results
    echo "Extracting individual MARS list results..."
    
    local current_period=""
    local current_database=""
    local current_year=""
    local current_month=""
    local in_data_section=false
    local section_counter=0
    
    while IFS= read -r line; do
        # Skip MARS info/log lines
        [[ "$line" =~ ^mars[[:space:]]*-[[:space:]]*INFO ]] && continue
        
        # Check for investigation comment to identify new section
        if [[ "$line" =~ ^#.*Investigation.*period[[:space:]]+([0-9]{4}-[0-9]{2}) ]]; then
            current_period="${BASH_REMATCH[1]}"
            in_data_section=false
            continue
        fi
        
        # Extract database type from command
        if [[ "$line" =~ database=marssc ]]; then
            current_database="marssc"
        elif [[ "$line" =~ ^list, ]]; then
            current_database="main"
        fi
        
        # Look for date parameter to confirm period
        if [[ "$line" =~ date=([0-9]{4})-([0-9]{2})-[0-9]{2}/to/([0-9]{4})-([0-9]{2})-[0-9]{2} ]]; then
            local extracted_period="${BASH_REMATCH[1]}-${BASH_REMATCH[2]}"
            if [[ -n "$extracted_period" ]]; then
                current_period="$extracted_period"
                current_year="${BASH_REMATCH[1]}"
                current_month="${BASH_REMATCH[2]}"
            fi
        fi
        
        # Check for data section start (headers like "month = 199205")
        if [[ "$line" =~ ^month[[:space:]]*=[[:space:]]*([0-9]{6}) ]]; then
            local yyyymm="${BASH_REMATCH[1]}"
            current_year="${yyyymm:0:4}"
            current_month="${yyyymm:4:2}"
            current_period="$current_year-$current_month"
            in_data_section=true
            
            # Create new section file
            section_counter=$((section_counter + 1))
            local section_file="$temp_dir/section_${section_counter}_${current_period}_${current_database}.txt"
            echo "# Period: $current_period, Database: $current_database" > "$section_file"
            echo "Found data section for $current_period ($current_database database)"
            continue
        fi
        
        # Check for data header line
        if [[ "$line" =~ ^date[[:space:]]+file[[:space:]]+length.*param[[:space:]]*$ ]]; then
            in_data_section=true
            continue
        fi
        
        # Process data lines if we're in a data section
        if [ "$in_data_section" = true ] && [[ "$line" =~ ^([0-9]{4}-[0-9]{2}-[0-9]{2})[[:space:]]+[0-9]+[[:space:]]+[0-9]+[[:space:]]+[^[:space:]]+[[:space:]]+[0-9]+[[:space:]]+([0-9]+)[[:space:]]*$ ]]; then
            local date="${BASH_REMATCH[1]}"
            local param="${BASH_REMATCH[2]}"
            
            # Write to current section file
            if [[ -n "$current_period" && -n "$current_database" ]]; then
                local section_file="$temp_dir/section_${section_counter}_${current_period}_${current_database}.txt"
                echo "$date $param" >> "$section_file"
            fi
        fi
        
        # Reset data section flag on empty lines or comment lines
        if [[ -z "$line" || "$line" =~ ^# ]]; then
            in_data_section=false
        fi
        
    done < "$input_file"
    
    echo "Found $section_counter data sections"
    
    # Analyze each section
    echo "Analyzing individual sections..." >> "$output_file"
    echo "=================================" >> "$output_file"
    echo "" >> "$output_file"
    
    for section_file in "$temp_dir"/section_*.txt; do
        if [[ -f "$section_file" ]]; then
            analyze_section "$section_file" "$output_file"
        fi
    done
    
    # Create summary comparison
    create_batch_summary "$temp_dir" "$output_file"
    
    # Clean up
    rm -rf "$temp_dir"
    
    echo "Batch analysis complete: $output_file"
}

# Function to analyze individual section
analyze_section() {
    local section_file=$1
    local output_file=$2
    
    # Extract section info from filename and header
    local section_name=$(basename "$section_file" .txt)
    local header_line=$(head -1 "$section_file")
    
    if [[ "$header_line" =~ Period:[[:space:]]+([0-9]{4}-[0-9]{2}).*Database:[[:space:]]+([a-z]+) ]]; then
        local period="${BASH_REMATCH[1]}"
        local database="${BASH_REMATCH[2]}"
        local year="${period%-*}"
        local month="${period#*-}"
        local month_num=$((10#$month))
        
        echo "Analyzing: $period ($database database)" >> "$output_file"
        echo "----------------------------------------" >> "$output_file"
        
        local days_in_month=$(get_days_in_month $year $month_num)
        local expected_total=$((${#EXPECTED_PARAMS[@]} * days_in_month))
        
        echo "Expected entries: $expected_total (${#EXPECTED_PARAMS[@]} params × $days_in_month days)" >> "$output_file"
        
        # Count entries by parameter and date
        declare -A param_dates
        declare -A all_dates
        declare -A param_counts
        local total_entries=0
        
        # Skip header line and process data
        tail -n +2 "$section_file" | while read date param; do
            [[ -z "$date" || -z "$param" ]] && continue
            param_dates["$param,$date"]=1
            all_dates["$date"]=1
            param_counts["$param"]=$((${param_counts[$param]:-0} + 1))
            total_entries=$((total_entries + 1))
        done
        
        # Re-read for analysis (since while loop runs in subshell)
        unset param_dates all_dates param_counts
        declare -A param_dates all_dates param_counts
        total_entries=0
        
        while read date param; do
            [[ -z "$date" || -z "$param" ]] && continue
            param_dates["$param,$date"]=1
            all_dates["$date"]=1
            param_counts["$param"]=$((${param_counts[$param]:-0} + 1))
            total_entries=$((total_entries + 1))
        done < <(tail -n +2 "$section_file")
        
        echo "Actual entries: $total_entries" >> "$output_file"
        echo "Missing entries: $((expected_total - total_entries))" >> "$output_file"
        echo "" >> "$output_file"
        
        # Parameter analysis
        echo "Parameter breakdown:" >> "$output_file"
        echo "Param    | Days Found | Expected | Missing" >> "$output_file"
        echo "---------|------------|----------|--------" >> "$output_file"
        
        local completely_missing=()
        local partially_missing=()
        
        for param in "${EXPECTED_PARAMS[@]}"; do
            local count=${param_counts[$param]:-0}
            local missing=$((days_in_month - count))
            
            printf "%-8s | %-10s | %-8s | %-7s\n" "$param" "$count" "$days_in_month" "$missing" >> "$output_file"
            
            if [ $count -eq 0 ]; then
                completely_missing+=("$param")
            elif [ $count -lt $days_in_month ]; then
                partially_missing+=("$param")
            fi
        done
        
        echo "" >> "$output_file"
        
        if [ ${#completely_missing[@]} -gt 0 ]; then
            echo "Completely missing parameters (${#completely_missing[@]}): ${completely_missing[*]}" >> "$output_file"
        fi
        
        if [ ${#partially_missing[@]} -gt 0 ]; then
            echo "Partially missing parameters (${#partially_missing[@]}): ${partially_missing[*]}" >> "$output_file"
        fi
        
        echo "" >> "$output_file"
        echo "=================================================" >> "$output_file"
        echo "" >> "$output_file"
    fi
}

# Function to create batch summary
create_batch_summary() {
    local temp_dir=$1
    local output_file=$2
    
    echo "" >> "$output_file"
    echo "BATCH ANALYSIS SUMMARY" >> "$output_file"
    echo "=====================" >> "$output_file"
    echo "" >> "$output_file"
    
    # Count sections by database
    local main_count=$(ls "$temp_dir"/section_*_main.txt 2>/dev/null | wc -l)
    local marssc_count=$(ls "$temp_dir"/section_*_marssc.txt 2>/dev/null | wc -l)
    
    echo "Analyzed sections:" >> "$output_file"
    echo "- Main database: $main_count periods" >> "$output_file"
    echo "- MARSCRATCH database: $marssc_count periods" >> "$output_file"
    echo "" >> "$output_file"
    
    # Find the most common missing parameters across all sections
    declare -A global_missing_count
    
    for section_file in "$temp_dir"/section_*.txt; do
        [[ -f "$section_file" ]] || continue
        
        # Quick analysis to find missing params
        declare -A section_params
        while read date param; do
            [[ -z "$date" || -z "$param" ]] && continue
            section_params["$param"]=1
        done < <(tail -n +2 "$section_file")
        
        # Check which expected params are missing
        for expected_param in "${EXPECTED_PARAMS[@]}"; do
            if [[ -z "${section_params[$expected_param]}" ]]; then
                global_missing_count["$expected_param"]=$((${global_missing_count[$expected_param]:-0} + 1))
            fi
        done
        
        unset section_params
    done
    
    echo "Most frequently missing parameters across all sections:" >> "$output_file"
    # Sort by frequency
    for param in "${!global_missing_count[@]}"; do
        echo "${global_missing_count[$param]} $param"
    done | sort -nr | head -10 | while read count param; do
        echo "- Parameter $param: missing from $count sections" >> "$output_file"
    done
    
    echo "" >> "$output_file"
}

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 <mars_batch_output_file>

This script analyzes "dirty" MARS output containing multiple list command results
with headers, log messages, and data sections.

The input should be output from running:
  mars availability_1991_1993_mars_commands.txt > output_file

This script will:
- Parse multiple MARS list results from the batch output
- Separate main database vs MARSCRATCH database results  
- Analyze parameter completeness for each period/database combination
- Identify missing parameters and patterns across periods

EXAMPLE:
  $0 output_from_1991_1993_mars_commands

OUTPUT:
  <input>_batch_analysis.txt - Comprehensive analysis of all periods and databases

The output will show:
- Parameter-by-parameter analysis for each period
- Missing vs present parameter counts  
- Comparison between main and MARSCRATCH databases
- Summary of most commonly missing parameters
EOF
}

# Main execution
if [ $# -ne 1 ]; then
    show_usage
    exit 1
fi

input_file="$1"

if [ ! -f "$input_file" ]; then
    echo "Error: File '$input_file' not found."
    exit 1
fi

echo "========================================="
echo "MARS Batch Output Analyzer" 
echo "========================================="
echo "Input file: $input_file"
echo ""

analyze_batch_mars_output "$input_file"