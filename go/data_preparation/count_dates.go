// Go script to count the files tar ball files for each stream in CARRA
// It creates a file periods.txt as well as prints the information on the screen.
// This is to be used to run the sqlite conversion

package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func main() {
	var previousPeriods map[string]string

	// Check if periods_prev.txt exists
	if _, err := os.Stat("periods_prev.txt"); err == nil {
		// periods_prev.txt exists, so read its contents
		previousPeriods = make(map[string]string)
		file, err := os.Open("periods_prev.txt")
		if err != nil {
			log.Fatalf("Error opening periods_prev.txt: %v", err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			fields := strings.Fields(line)
			if len(fields) == 3 {
				stream := fields[0]
				endDate := fields[2] // Assuming the third field is the previous end date
				previousPeriods[stream] = endDate
			}
		}
		if err := scanner.Err(); err != nil {
			log.Fatalf("Error reading periods_prev.txt: %v", err)
		}
		fmt.Println("Loaded previous periods from periods_prev.txt")
	} else if os.IsNotExist(err) {
		// periods_prev.txt doesn't exist
		fmt.Println("No periods_prev.txt found, creating a new periods.txt without previous data")
	} else {
		// Some other error occurred while checking for the file
		log.Fatalf("Error checking periods_prev.txt: %v", err)
	}

	streams := []string{
		"carra2_198409", "carra2_198909", "carra2_199409",
		"carra2_199909", "carra2_200409", "carra2_200909",
		"carra2_201409", "carra2_201909",
	}

	// Create or open a file to write the new periods to
	file, err := os.Create("periods.txt")
	if err != nil {
		log.Fatalf("Error creating file: %v", err)
	}
	defer file.Close() // Ensure file is closed at the end
	writer := bufio.NewWriter(file) // Create a buffered writer

	for _, stream := range streams {
		fmt.Println("Processing stream:", stream)
		var dates []string

		// Get list of files using filepath.Glob
		files, err := filepath.Glob(fmt.Sprintf("/ec/res4/scratch/fac2/hm_home/%s/archive/extract/*.tar.gz", stream))
		if err != nil {
			log.Println("Error executing command:", err)
			continue
		}

		// Extract dates from filenames
		for _, file := range files {
			base := filepath.Base(file)
			filename := strings.TrimSuffix(base, ".tar.gz")
			if len(filename) >= 18 {
				date := filename[10:18]
				dates = append(dates, date)
			}
		}

		// Sort and get unique dates
		sort.Strings(dates)
		if len(dates) > 0 {
			startDate := dates[0] + "00"
			endDate := dates[len(dates)-1] + "23"

			// Initialize the line that will be written to periods.txt
			var outputLine string

			if previousPeriods != nil {
				// Check if we have a previous period for this stream
				if prevEndDate, exists := previousPeriods[stream]; exists {
					// Use the previous end date as the second column
					outputLine = fmt.Sprintf("%s %s %s", stream, prevEndDate, endDate)
				} else {
					// No previous period available, leave it blank or set to default
					outputLine = fmt.Sprintf("%s %s %s", stream, startDate, endDate)
				}
			} else {
				// No previous periods, just write start and end dates
				outputLine = fmt.Sprintf("%s %s %s", stream, startDate, endDate)
			}

			// Write the period to the new file
			_, err := writer.WriteString(outputLine + "\n")
			if err != nil {
				log.Fatalf("Error writing to file: %v", err)
			}
		}
	}

	// Flush any remaining data in the buffer to the file
	writer.Flush()

	fmt.Println("New periods have been written to periods.txt")
}

