#!/bin/bash

# Script to fix MARS archive parameter errors
# Usage: ./fix_mars_errors.sh log_file.out

if [ $# -ne 1 ]; then
    echo "Usage: $0 <log_file>"
    exit 1
fi

LOG_FILE="$1"

# Check if log file exists
if [ ! -f "$LOG_FILE" ]; then
    echo "Error: Log file '$LOG_FILE' not found"
    exit 1
fi

echo "Analyzing MARS archive errors in: $LOG_FILE"
echo "=================================================="

# Extract sections with errors
awk '
BEGIN { 
    in_archive_block = 0
    in_error_block = 0
    archive_request = ""
    error_info = ""
}

# Start of archive request block
/^ARCHIVE,/ { 
    in_archive_block = 1
    archive_request = $0 "\n"
    next
}

# Continue archive block until we hit mars - INFO scanning line
in_archive_block && /mars - INFO.*Scanning and analysing GRIB file/ {
    grib_file = $0
    gsub(/.*GRIB file /, "", grib_file)
    archive_request = archive_request "GRIB_FILE: " grib_file "\n"
    in_archive_block = 0
    next
}

# Collect archive block lines
in_archive_block {
    archive_request = archive_request $0 "\n"
    next
}

# Detect error start
/mars - ERROR.*Field [0-9]+ is unknown/ {
    in_error_block = 1
    error_info = archive_request
    next
}

# Get the actual parameter from error message
in_error_block && /mars - ERROR.*Could not match PARAM/ {
    match($0, /PARAM \(([0-9]+)\)/, param_match)
    if (param_match[1]) {
        actual_param = param_match[1]
        error_info = error_info "ACTUAL_PARAM: " actual_param "\n"
    }
    next
}

# End of error block - print the error info
in_error_block && /mars - ERROR.*Error occured for field/ {
    print "ERROR FOUND:"
    print "============"
    print error_info
    print "---"
    in_error_block = 0
    archive_request = ""
    error_info = ""
    actual_param = ""
}

# Reset if we hit memory usage (end of successful archive)
/Memory used:/ {
    in_archive_block = 0
    in_error_block = 0
    archive_request = ""
    error_info = ""
}
' "$LOG_FILE" > temp_errors.txt

# Process the extracted errors and create fixed commands
echo "Generating corrected MARS archive commands..."
echo "=============================================="

cat temp_errors.txt | awk '
BEGIN {
    RS = "ERROR FOUND:\n============\n"
    FS = "\n"
}

NF > 0 {
    grib_file = ""
    actual_param = ""
    archive_lines = ""
    
    for (i = 1; i <= NF; i++) {
        if ($i ~ /^GRIB_FILE:/) {
            gsub(/^GRIB_FILE: /, "", $i)
            grib_file = $i
        }
        else if ($i ~ /^ACTUAL_PARAM:/) {
            gsub(/^ACTUAL_PARAM: /, "", $i)
            actual_param = $i
        }
        else if ($i ~ /^ARCHIVE,/ || $i ~ /^[[:space:]]+/) {
            if ($i !~ /^GRIB_FILE:/ && $i !~ /^ACTUAL_PARAM:/ && $i != "---") {
                archive_lines = archive_lines $i "\n"
            }
        }
    }
    
    if (grib_file != "" && actual_param != "") {
        print "# Error in file: " grib_file
        print "# Actual parameter found: " actual_param
        print "# Corrected archive command:"
        
        # Replace the PARAM line with actual parameter
        gsub(/PARAM[[:space:]]*=[[:space:]]*[0-9]+,/, "PARAM      = " actual_param ",", archive_lines)
        
        print "archive,"
        print archive_lines
        print "# Use grib_ls to verify parameter:"
        print "grib_ls -p paramId " grib_file
        print ""
        print "---"
        print ""
    }
}
' 

# Also create a summary
echo ""
echo "SUMMARY:"
echo "========"
error_count=$(grep -c "ERROR FOUND:" temp_errors.txt)
echo "Total MARS archive errors found: $error_count"

if [ $error_count -gt 0 ]; then
    echo ""
    echo "Files with parameter mismatches:"
    grep "GRIB_FILE:" temp_errors.txt | sed 's/GRIB_FILE: /- /'
    
    echo ""
    echo "Parameter mismatches found:"
    paste <(grep "GRIB_FILE:" temp_errors.txt | sed 's/.*an_dame_sfc_//' | sed 's/.grib2//') \
          <(grep "ACTUAL_PARAM:" temp_errors.txt | sed 's/ACTUAL_PARAM: //') | \
    awk '{printf "- File suffix %s: actual param = %s\n", $1, $2}'
fi

# Generate executable MARS commands
echo ""
echo "EXECUTABLE MARS COMMANDS:"
echo "========================="

cat temp_errors.txt | awk '
BEGIN {
    RS = "ERROR FOUND:\n============\n"
    FS = "\n"
    print "#!/bin/bash"
    print "# Generated MARS archive commands with corrected parameters"
    print "# Generated on: " strftime("%Y-%m-%d %H:%M:%S")
    print ""
}

NF > 0 {
    grib_file = ""
    actual_param = ""
    archive_lines = ""
    original_param = ""
    
    for (i = 1; i <= NF; i++) {
        if ($i ~ /^GRIB_FILE:/) {
            gsub(/^GRIB_FILE: /, "", $i)
            grib_file = $i
        }
        else if ($i ~ /^ACTUAL_PARAM:/) {
            gsub(/^ACTUAL_PARAM: /, "", $i)
            actual_param = $i
        }
        else if ($i ~ /^ARCHIVE,/ || $i ~ /^[[:space:]]+/) {
            if ($i !~ /^GRIB_FILE:/ && $i !~ /^ACTUAL_PARAM:/ && $i != "---") {
                # Extract original param for reference
                if ($i ~ /PARAM/) {
                    match($i, /PARAM[[:space:]]*=[[:space:]]*([0-9]+)/, param_match)
                    if (param_match[1]) original_param = param_match[1]
                }
                archive_lines = archive_lines $i "\n"
            }
        }
    }
    
    if (grib_file != "" && actual_param != "") {
        print "echo \"Archiving file with corrected parameter: " grib_file "\""
        print "echo \"Original param: " original_param " -> Corrected param: " actual_param "\""
        
        # Replace the PARAM line with actual parameter
        gsub(/PARAM[[:space:]]*=[[:space:]]*[0-9]+,/, "PARAM      = " actual_param ",", archive_lines)
        
        print "mars << EOF"
        print "archive,"
        # Clean up the archive lines and remove trailing newlines
        gsub(/\n+$/, "", archive_lines)
        print archive_lines
        print "EOF"
        print ""
        print "if [ $? -eq 0 ]; then"
        print "    echo \"SUCCESS: Archived " grib_file "\""
        print "else"
        print "    echo \"ERROR: Failed to archive " grib_file "\""
        print "    exit 1"
        print "fi"
        print ""
    }
}

END {
    print "echo \"All MARS archive commands completed successfully!\""
}
' > corrected_mars_commands.sh

chmod +x corrected_mars_commands.sh

echo "Generated executable script: corrected_mars_commands.sh"
echo ""

# Also generate a simple list format
echo "SIMPLE COMMAND LIST:"
echo "==================="

cat temp_errors.txt | awk '
BEGIN {
    RS = "ERROR FOUND:\n============\n"
    FS = "\n"
}

NF > 0 {
    grib_file = ""
    actual_param = ""
    archive_lines = ""
    
    for (i = 1; i <= NF; i++) {
        if ($i ~ /^GRIB_FILE:/) {
            gsub(/^GRIB_FILE: /, "", $i)
            grib_file = $i
        }
        else if ($i ~ /^ACTUAL_PARAM:/) {
            gsub(/^ACTUAL_PARAM: /, "", $i)
            actual_param = $i
        }
        else if ($i ~ /^ARCHIVE,/ || $i ~ /^[[:space:]]+/) {
            if ($i !~ /^GRIB_FILE:/ && $i !~ /^ACTUAL_PARAM:/ && $i != "---") {
                archive_lines = archive_lines $i "\n"
            }
        }
    }
    
    if (grib_file != "" && actual_param != "") {
        # Replace the PARAM line with actual parameter
        gsub(/PARAM[[:space:]]*=[[:space:]]*[0-9]+,/, "PARAM=" actual_param ",", archive_lines)
        
        # Create a single line command
        single_line = "mars -c \"archive," 
        gsub(/\n/, "", archive_lines)
        gsub(/[[:space:]]+/, " ", archive_lines)
        gsub(/,/, ", ", archive_lines)
        single_line = single_line archive_lines "\""
        
        print single_line
    }
}
' > corrected_mars_list.txt

echo "Generated command list: corrected_mars_list.txt"

# Clean up
rm -f temp_errors.txt

echo ""
echo "GENERATED FILES:"
echo "==============="
echo "1. corrected_mars_commands.sh  - Executable bash script with error handling"
echo "2. corrected_mars_list.txt     - Simple list of MARS commands"
echo ""
echo "To run the corrections:"
echo "./corrected_mars_commands.sh"
echo ""
echo "To fix these errors, you should:"
echo "1. Review the generated commands above"
echo "2. Run: ./corrected_mars_commands.sh"
echo "3. Verify parameters with: grib_ls -p paramId <grib_file>"
echo "4. Update your archiving scripts to use correct parameter values"
