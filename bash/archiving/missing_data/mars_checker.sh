#!/bin/bash
source env.sh

# MARS Data Availability Checker - Multi-Period
# Checks data availability in both main database and scratch database (marssc)
# Creates a comprehensive availability report

# Global variables
OUTPUT_FILE="availability_$(date '+%Y%m%d').txt"
TEMP_DIR="/tmp/mars_check_$$"
mkdir -p "$TEMP_DIR"

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

# Function to check MARS data availability
check_mars_data() {
    local year=$1
    local month=$2
    local database=$3
    
    # Format month with leading zero
    local formatted_month=$(printf "%02d" $month)
    
    # Get last day of month
    local last_day=$(get_days_in_month $year $month)
    local formatted_last_day=$(printf "%02d" $last_day)
    
    # Set database parameter
    local db_param=""
    if [ "$database" == "marssc" ]; then
        db_param=", database=marssc"
    fi
    
    # Create MARS request
    local mars_request="list,
class=rr,
date=$year-$formatted_month-01/to/$year-$formatted_month-$formatted_last_day,
expver=prod,
levtype=sfc,
origin=no-ar-pa,
param=$CARRA_PAR_AN_SFC,
stream=dame,
type=an$db_param"

    # Execute MARS request and capture output
    local output=$(mars << EOF
$mars_request
EOF
)
    
    # Extract entries count
    local entries=$(echo "$output" | grep "Entries" | awk -F: '{print $2}' | tr -d ' ')
    
    # Return entries count (default to 0 if not found)
    echo ${entries:-0}
}

# Function to determine status based on availability
get_status() {
    local mars_entries=$1
    local marssc_entries=$2
    
    if [ $mars_entries -gt 0 ] && [ $marssc_entries -gt 0 ]; then
        echo "COMPLETE"
    elif [ $mars_entries -eq 0 ] && [ $marssc_entries -gt 0 ]; then
        echo "TRANSFER"
    elif [ $mars_entries -eq 0 ] && [ $marssc_entries -eq 0 ]; then
        echo "SUBMIT"
    else
        echo "PARTIAL"  # Data in mars but not in marssc
    fi
}

# Function to check single period and append to results
check_period() {
    local year=$1
    local month=$2
    
    local formatted_month=$(printf "%02d" $month)
    local period="${year}-${formatted_month}"
    
    echo "Checking period: $period..."
    
    # Check both databases
    local mars_entries=$(check_mars_data $year $month "main")
    local marssc_entries=$(check_mars_data $year $month "marssc")
    
    # Determine status
    local status=$(get_status $mars_entries $marssc_entries)
    
    # Write to temporary file
    echo "$period $mars_entries $marssc_entries $status" >> "$TEMP_DIR/results.txt"
    
    echo "  MARS: $mars_entries entries, MARSCRATCH: $marssc_entries entries, Status: $status"
}

# Function to initialize output file
init_output_file() {
    cat > "$OUTPUT_FILE" << EOF
# MARS Data Availability Report
# Generated: $(date '+%Y-%m-%d %H:%M:%S')
# Parameters: 31/34/78/79/134/151/165/166/167/172/207/235/3066/3073/3074/3075/174096/174098/228002/228141/228164/231063/260001/260038/260057/260107/260108/260242/260260/260650
# 
# Status codes:
# COMPLETE - Data available in both MARS and MARSCRATCH
# TRANSFER - Data missing in MARS but available in MARSCRATCH
# SUBMIT   - Data missing in both databases
# PARTIAL  - Data in MARS but not in MARSCRATCH
#
period mars marscratch status
EOF
}

# Function to finalize output file
finalize_output_file() {
    if [ -f "$TEMP_DIR/results.txt" ]; then
        # Sort results by period and append to output file
        sort "$TEMP_DIR/results.txt" >> "$OUTPUT_FILE"
        
        # Add summary statistics
        echo "" >> "$OUTPUT_FILE"
        echo "# Summary Statistics:" >> "$OUTPUT_FILE"
        echo "# Total periods checked: $(wc -l < "$TEMP_DIR/results.txt")" >> "$OUTPUT_FILE"
        echo "# COMPLETE: $(grep -c "COMPLETE" "$TEMP_DIR/results.txt")" >> "$OUTPUT_FILE"
        echo "# TRANSFER: $(grep -c "TRANSFER" "$TEMP_DIR/results.txt")" >> "$OUTPUT_FILE"
        echo "# SUBMIT: $(grep -c "SUBMIT" "$TEMP_DIR/results.txt")" >> "$OUTPUT_FILE"
        echo "# PARTIAL: $(grep -c "PARTIAL" "$TEMP_DIR/results.txt")" >> "$OUTPUT_FILE"
    fi
    
    # Clean up
    rm -rf "$TEMP_DIR"
}

# Function to parse year range
parse_year_range() {
    local year_input=$1
    
    if [[ "$year_input" =~ ^([0-9]{4})-([0-9]{4})$ ]]; then
        # Year range format: 2000-2005
        local start_year=${BASH_REMATCH[1]}
        local end_year=${BASH_REMATCH[2]}
        
        if [ $start_year -le $end_year ]; then
            seq $start_year $end_year
        else
            echo "Error: Start year must be <= end year" >&2
            exit 1
        fi
    elif [[ "$year_input" =~ ^[0-9]{4}(,[0-9]{4})*$ ]]; then
        # Comma-separated years: 2000,2003,2005
        echo "$year_input" | tr ',' '\n'
    elif [[ "$year_input" =~ ^[0-9]{4}$ ]]; then
        # Single year
        echo "$year_input"
    else
        echo "Error: Invalid year format" >&2
        exit 1
    fi
}

# Function to parse month range
parse_month_range() {
    local month_input=$1
    
    if [[ "$month_input" =~ ^([0-9]{1,2})-([0-9]{1,2})$ ]]; then
        # Month range format: 1-12 or 03-06
        local start_month=${BASH_REMATCH[1]}
        local end_month=${BASH_REMATCH[2]}
        
        # Remove leading zeros for arithmetic
        start_month=$((10#$start_month))
        end_month=$((10#$end_month))
        
        if [ $start_month -ge 1 ] && [ $start_month -le 12 ] && 
           [ $end_month -ge 1 ] && [ $end_month -le 12 ] && 
           [ $start_month -le $end_month ]; then
            seq $start_month $end_month
        else
            echo "Error: Invalid month range" >&2
            exit 1
        fi
    elif [[ "$month_input" =~ ^[0-9]{1,2}(,[0-9]{1,2})*$ ]]; then
        # Comma-separated months: 1,3,6,12
        echo "$month_input" | tr ',' '\n' | while read month; do
            month=$((10#$month))  # Remove leading zeros
            if [ $month -ge 1 ] && [ $month -le 12 ]; then
                echo $month
            else
                echo "Error: Invalid month $month" >&2
                exit 1
            fi
        done
    elif [[ "$month_input" =~ ^[0-9]{1,2}$ ]]; then
        # Single month
        local month=$((10#$month_input))
        if [ $month -ge 1 ] && [ $month -le 12 ]; then
            echo $month
        else
            echo "Error: Invalid month" >&2
            exit 1
        fi
    else
        echo "Error: Invalid month format" >&2
        exit 1
    fi
}

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 <years> <months>

Years can be:
  - Single year: 2007
  - Year range: 2000-2005
  - Comma-separated: 2000,2003,2007

Months can be:
  - Single month: 1 or 01
  - Month range: 1-12 or 03-06
  - Comma-separated: 1,3,6,12

Examples:
  $0 2007 1                    # Check January 2007
  $0 2000-2005 1-12           # Check all months from 2000 to 2005
  $0 2007,2008 1,6,12         # Check Jan, Jun, Dec for 2007 and 2008
  $0 2020 3-5                 # Check March to May 2020

Output: Creates availability_YYYYMMDD.txt with results
EOF
}

# Main execution
if [ $# -eq 2 ] || [ $# -eq 3 ]; then
    years_input=$1
    months_input=$2
    
    # Set output filename
    if [ $# -eq 3 ]; then
        OUTPUT_FILE="$3"
        # Add .txt extension if not present
        if [[ ! "$OUTPUT_FILE" =~ \.txt$ ]]; then
            OUTPUT_FILE="${OUTPUT_FILE}.txt"
        fi
    else
        OUTPUT_FILE="availability_$(date '+%Y%m%d').txt"
    fi
    
    # Parse years and months
    years=$(parse_year_range "$years_input")
    if [ $? -ne 0 ]; then exit 1; fi
    
    months=$(parse_month_range "$months_input")
    if [ $? -ne 0 ]; then exit 1; fi
    
    # Convert to arrays for counting
    year_array=($years)
    month_array=($months)
    total_periods=$((${#year_array[@]} * ${#month_array[@]}))
    
    echo "========================================="
    echo "MARS Multi-Period Data Availability Check"
    echo "========================================="
    echo "Years: $(echo $years | tr '\n' ',' | sed 's/,$//')"
    echo "Months: $(echo $months | tr '\n' ',' | sed 's/,$//')"
    echo "Total periods to check: $total_periods"
    echo "Output file: $OUTPUT_FILE"
    echo "========================================="
    echo ""
    
    # Initialize output file
    init_output_file
    
    # Check all combinations
    current_period=0
    for year in $years; do
        for month in $months; do
            current_period=$((current_period + 1))
            echo "Progress: $current_period/$total_periods"
            check_period $year $month
            echo ""
        done
    done
    
    # Finalize output file
    finalize_output_file
    
    echo "========================================="
    echo "CHECK COMPLETED"
    echo "========================================="
    echo "Results saved to: $OUTPUT_FILE"
    echo ""
    echo "Summary:"
    tail -5 "$OUTPUT_FILE" | grep "^#" | sed 's/^# //'
    
else
    show_usage
    exit 1
fi
