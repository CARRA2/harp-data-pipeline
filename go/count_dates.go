package main

// Go script to count the files tar ball files for each stream in CARRA
// It creates a file periods.txt as well as prints the information on the screen.
// This is to be used to run the sqlite conversion

import (
	"fmt"
         "log"
	// "os/exec" //needed by the bash command
	"path/filepath"
	"sort"
	"strings"
        "os"
        "bufio" //need to write a file
)

func main() {
	streams := []string{
		"carra2_198409", "carra2_198909", "carra2_199409",
		"carra2_199909", "carra2_200409", "carra2_200909",
		"carra2_201409", "carra2_201909",
	}

        // Create or open a file to write the periods to
	file, err := os.Create("periods.txt")
	if err != nil {
		log.Fatalf("Error creating file: %v", err)
	}
	defer file.Close() // Ensure file is closed at the end
	writer := bufio.NewWriter(file) // Create a buffered writer


	for _, stream := range streams {
		fmt.Println("Going through ",stream)
		var dates []string

		// Get list of files using 'ls' command
		// cmd := exec.Command("ls", "-al", fmt.Sprintf("/ec/res4/scratch/fac2/hm_home/%s/archive/extract/*.tar.gz", stream))
                // the part above failed because it cannot handle the wildcards. Using bash instead works:
                //cmd := exec.Command("bash", "-c", fmt.Sprintf("ls -al /ec/res4/scratch/fac2/hm_home/%s/archive/extract/*.tar.gz", stream))
		//output, err := cmd.Output()
                
                // Using filepath.Glob instead? This is faster than using bash!
                files, err := filepath.Glob(fmt.Sprintf("/ec/res4/scratch/fac2/hm_home/%s/archive/extract/*.tar.gz", stream))
		if err != nil {
			log.Println("Error executing command:", err)
			continue
		}

		// Split output by lines
		// lines := strings.Split(string(output), "\n")
		// var files []string

		// Extract file names from the output (assuming 9th field is the filename)
                // use this with the bash command above
		// for _, line := range lines {
		// 	fields := strings.Fields(line)
		// 	if len(fields) > 8 {
		// 		files = append(files, fields[8])
		// 	}
		// }

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
			period := fmt.Sprintf("%s %s", startDate, endDate)
			fmt.Println(period)
                        // Write the period to the file
			_, err := writer.WriteString(fmt.Sprintf("%s %s\n", stream, period))
			if err != nil {
				log.Fatalf("Error writing to file: %v", err)
			}
		}

		// break // Remove this if you want to process all streams
	}

// Flush any remaining data in the buffer to the file
	writer.Flush()

	fmt.Println("Periods have been written to periods.txt")
}

