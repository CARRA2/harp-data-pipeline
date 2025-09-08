#!/bin/bash

# MARS Data Discrepancy Analyzer
# Analyzes availability reports and identifies data discrepancies

# Function to get the number of days in a month
get_days_in_month() {
    local year=$1
    local month=$2
    
    # Handle leap years for February
    if [ $month -eq 2 ]; then
        if [ $((year % 4)) -eq 0 ] && [ $((year % 100)) -ne 0 ] || [ $((year % 400)) -eq 0 ]; then
            echo 29  # Leap year
        else
            echo 28  # Non-leap year
        fi
    elif [ $month -eq 4 ] || [ $month -eq 6 ] || [ $month -eq 9 ] || [ $month -eq 11 ]; then
        echo 30
    else
        echo 31
    fi
}

# Function to calculate expected entries (30 parameters × days in month)
calculate_expected_entries() {
    local year=$1
    local month=$2
    local days=$(get_days_in_month $year $month)
    local parameters=30  # Based on the parameter list in your template
    
    echo $((parameters * days))
}

# Function to generate MARS list command for discrepancy analysis
generate_mars_command() {
    local year=$1
    local month=$2
    local database=$3
    
    local formatted_month=$(printf "%02d" $month)
    local last_day=$(get_days_in_month $year $month)
    local formatted_last_day=$(printf "%02d" $last_day)
    
    local db_param=""
    if [ "$database" == "marssc" ]; then
        db_param=", database=marssc"
    fi
    
    cat << EOF
# MARS command to investigate $year-$formatted_month in $database database:
list,
class=rr,
date=$year-$formatted_month-01/to/$year-$formatted_month-$formatted_last_day,
expver=prod,
levtype=sfc,
origin=no-ar-pa,
param=31/34/78/79/134/151/165/166/167/172/207/235/3066/3073/3074/3075/174096/174098/228002/228141/228164/231063/260001/260038/260057/260107/260108/260242/260260/260650,
stream=dame,
type=an$db_param

EOF
}

# Function to analyze a single availability report
analyze_report() {
    local report_file=$1
    local output_file="${report_file%.txt}_analysis.txt"
    
    echo "Analyzing: $report_file"
    echo "Output: $output_file"
    
    # Initialize output file
    cat > "$output_file" << EOF
# MARS Data Discrepancy Analysis
# Generated: $(date '+%Y-%m-%d %H:%M:%S')
# Source: $report_file
#
# Analysis of data completeness and discrepancies
# Expected entries = 30 parameters × days in month
#
EOF

    local max_mars=0
    local max_marssc=0
    local max_period=""
    local discrepancies_found=0
    local mars_commands_file="${report_file%.txt}_mars_commands.txt"
    
    # Clear mars commands file
    > "$mars_commands_file"
    
    echo "period expected mars marssc mars_diff marssc_diff status analysis" >> "$output_file"
    echo "=================================================================" >> "$output_file"
    
    # Read the availability report and analyze each period
    while IFS=' ' read -r period mars marssc status; do
        # Skip comment lines and header
        if [[ "$period" =~ ^#.*$ ]] || [[ "$period" == "period" ]]; then
            continue
        fi
        
        # Skip empty lines
        if [[ -z "$period" ]]; then
            continue
        fi
        
        # Parse year and month from period (format: YYYY-MM)
        if [[ "$period" =~ ^([0-9]{4})-([0-9]{2})$ ]]; then
            local year=${BASH_REMATCH[1]}
            local month=$((10#${BASH_REMATCH[2]}))  # Remove leading zero
            
            # Calculate expected entries
            local expected=$(calculate_expected_entries $year $month)
            
            # Calculate differences
            local mars_diff=$((expected - mars))
            local marssc_diff=$((expected - marssc))
            
            # Track maximum values for reference
            if [ $mars -gt $max_mars ]; then
                max_mars=$mars
                max_period="$period"
            fi
            if [ $marssc -gt $max_marssc ]; then
                max_marssc=$marssc
            fi
            
            # Determine analysis status
            local analysis=""
            local needs_investigation=0
            
            if [ $mars -eq $expected ] && [ $marssc -eq $expected ]; then
                analysis="COMPLETE_CORRECT"
            elif [ $mars -eq 0 ] && [ $marssc -eq $expected ]; then
                analysis="TRANSFER_NEEDED"
                needs_investigation=1
            elif [ $mars -eq 0 ] && [ $marssc -eq 0 ]; then
                analysis="SUBMIT_REQUIRED"
                needs_investigation=1
            elif [ $mars_diff -ne 0 ] || [ $marssc_diff -ne 0 ]; then
                analysis="INCOMPLETE_DATA"
                needs_investigation=1
                discrepancies_found=$((discrepancies_found + 1))
            else
                analysis="UNKNOWN"
                needs_investigation=1
            fi
            
            # Write analysis line
            printf "%-8s %-8s %-4s %-8s %-9s %-11s %-15s %s\n" \
                "$period" "$expected" "$mars" "$marssc" "$mars_diff" "$marssc_diff" "$status" "$analysis" >> "$output_file"
            
            # Generate MARS commands for investigation if needed
            if [ $needs_investigation -eq 1 ]; then
                echo "# Investigation needed for period $period ($analysis)" >> "$mars_commands_file"
                
                if [ $mars -ne $expected ]; then
                    echo "# MARS database missing $mars_diff entries" >> "$mars_commands_file"
                    generate_mars_command $year $month "main" >> "$mars_commands_file"
                fi
                
                if [ $marssc -ne $expected ]; then
                    echo "# MARSCRATCH database missing $marssc_diff entries" >> "$mars_commands_file"
                    generate_mars_command $year $month "marssc" >> "$mars_commands_file"
                fi
                
                echo "" >> "$mars_commands_file"
            fi
        fi
    done < <(grep -v "^#.*Statistics:" "$report_file")
    
    # Add summary to analysis file
    cat >> "$output_file" << EOF

# Analysis Summary:
# ================
# Maximum MARS entries found: $max_mars (period: $max_period)
# Maximum MARSCRATCH entries found: $max_marssc
# INCOMPLETE_DATA periods requiring investigation: $discrepancies_found
# 
# Investigation commands saved to: $mars_commands_file
#
# Analysis codes:
# COMPLETE_CORRECT - All expected entries present in both databases
# TRANSFER_NEEDED  - Data missing from MARS but complete in MARSCRATCH (no investigation needed)
# SUBMIT_REQUIRED  - Data missing from both databases (no investigation needed)
# INCOMPLETE_DATA  - Partial data missing from one or both databases (INVESTIGATION REQUIRED)
EOF
    
    echo ""
    echo "Analysis complete!"
    echo "Summary:"
    echo "  Maximum MARS entries: $max_mars (reference period: $max_period)"
    echo "  Maximum MARSCRATCH entries: $max_marssc"
    echo "  INCOMPLETE_DATA periods requiring investigation: $discrepancies_found"
    
    if [ $discrepancies_found -gt 0 ]; then
        echo ""
        echo "Files created:"
        echo "  Analysis: $output_file"
        echo "  MARS commands: $mars_commands_file"
    else
        echo ""
        echo "No INCOMPLETE_DATA cases found - no MARS commands generated."
        echo "Analysis file created: $output_file"
        # Remove empty mars commands file
        rm -f "$mars_commands_file"
    fi
}

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 <availability_report_file>

This script analyzes MARS availability reports and identifies data discrepancies.

For each period, it:
- Calculates expected entries (30 parameters × days in month)
- Compares actual vs expected entries
- Identifies incomplete data
- Generates MARS commands for investigation

Example:
  $0 availability_1991_1993.txt

Output files:
- {input}_analysis.txt     - Detailed analysis report
- {input}_mars_commands.txt - MARS commands to investigate discrepancies
EOF
}

# Main execution
if [ $# -eq 1 ]; then
    report_file="$1"
    
    # Check if file exists
    if [ ! -f "$report_file" ]; then
        echo "Error: File '$report_file' not found."
        exit 1
    fi
    
    # Check if file appears to be an availability report
    if ! grep -q "^period mars marscratch status" "$report_file"; then
        echo "Error: '$report_file' does not appear to be a valid availability report."
        echo "Expected format with header: 'period mars marscratch status'"
        exit 1
    fi
    
    echo "========================================="
    echo "MARS Data Discrepancy Analyzer"
    echo "========================================="
    echo "Input file: $report_file"
    echo ""
    
    analyze_report "$report_file"
    
else
    show_usage
    exit 1
fi
