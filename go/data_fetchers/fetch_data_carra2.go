package main

import (
        "flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
        "strconv" 
	"gopkg.in/yaml.v2"
)

// Define a few global variables to set paths
var projLibPath string

func init() {
    // Initialize the global variable with the value from the environment variable
    projLibPath = os.Getenv("ECFPROJ_LIB")
}

// Stream represents the structure of each stream in the YAML file.
type Stream struct {
	BEG_DATE string `yaml:"BEG_DATE"`
	END_DATE string `yaml:"END_DATE"`
	USER     string `yaml:"USER"`
	ACTIVE   bool   `yaml:"ACTIVE"`
	PROGLOG  string   `yaml:"PROGLOG"`
}

// OBSPath represents the destination paths for the data types.
type OBSPath struct {
	LOCALPATH string `yaml:"LOCALPATH"`
}

// Config represents the structure of the YAML file.
type Config struct {
	STREAMS map[string]Stream           `yaml:"STREAMS"`
	OBS     map[string]OBSPath          `yaml:"OBS"`
}

// ReadYAML reads and parses the YAML file.
func ReadYAML(filename string) (*Config, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

// CheckProgress reads the progress.log file for the given stream and returns the current DTG.
func CheckProgress(streamName, user string) (string, error) {
	logPath := fmt.Sprintf("/home/%s/hm_home/%s/progress.log", user, streamName)
	data, err := ioutil.ReadFile(logPath)
	if err != nil {
		return "", err
	}
	// Find the DTG in the log file (assuming it appears as "DTG=YYYYMMDDHH")
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "DTG=") {
                      // Split the line by space and take the first part, which is DTG=YYYYMMDDHH
			parts := strings.Split(line, " ")
			// Extract the actual DTG value by removing the "DTG=" prefix
			if len(parts) > 0 {
				dtg := strings.TrimPrefix(parts[0], "DTG=")
				return dtg, nil
			}
		}
	}
	return "", fmt.Errorf("DTG not found in %s", logPath)
}
// Test version of thre function above
func CheckProgressTest(streamName, logPath string) (string, error) {
	// logPath := fmt.Sprintf("/home/%s/hm_home/%s/progress.log", user, streamName)
	data, err := ioutil.ReadFile(logPath)
	if err != nil {
		return "", err
	}
	// Find the DTG in the log file (assuming it appears as "DTG=YYYYMMDDHH")
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "DTG=") {
                      // Split the line by space and take the first part, which is DTG=YYYYMMDDHH
			parts := strings.Split(line, " ")
			// Extract the actual DTG value by removing the "DTG=" prefix
			if len(parts) > 0 {
				dtg := strings.TrimPrefix(parts[0], "DTG=")
				return dtg, nil
			}
		}
	}
	return "", fmt.Errorf("DTG not found in %s", logPath)
}

// ExecuteActions performs the actions for each data type in December.
func ExecuteActions(year string, obs map[string]OBSPath) error {
    dataTypes := []string{"S3SICE", "MODIS", "AVHRR", "OSISAF"}
    currentYear, _ := strconv.Atoi(year)

    for _, dataType := range dataTypes {
        // Get the destination path from the OBS section
        destinationPath := obs[dataType].LOCALPATH
        if destinationPath == "" {
            return fmt.Errorf("no destination path found for %s", dataType)
        }

        yearDir := filepath.Join(destinationPath, year)


        // Execute commands specific to each data type
        switch dataType {
        case "S3SICE":
            if currentYear >= 2021 {
              // Create the directory for the next year if it doesn't exist
              if _, err := os.Stat(yearDir); os.IsNotExist(err) {
                  fmt.Printf("Creating directory %s\n", yearDir)
                  if err := os.MkdirAll(yearDir, 0755); err != nil {
                      return err
                  }
              }
                executeEcpCommand(dataType, year, yearDir)
            }
        case "AVHRR":
            if currentYear >= 1985 && currentYear <= 2000 {
              // Create the directory for the next year if it doesn't exist
              if _, err := os.Stat(yearDir); os.IsNotExist(err) {
                  fmt.Printf("Creating directory %s\n", yearDir)
                  if err := os.MkdirAll(yearDir, 0755); err != nil {
                      return err
                  }
              }
                executeEcpCommand(dataType, year, yearDir)
            }
        case "MODIS":
            if currentYear >= 2000 && currentYear <= 2019 {
              // Create the directory for the next year if it doesn't exist
              if _, err := os.Stat(yearDir); os.IsNotExist(err) {
                  fmt.Printf("Creating directory %s\n", yearDir)
                  if err := os.MkdirAll(yearDir, 0755); err != nil {
                      return err
                  }
              }
                executeEcpCommand(dataType, year, yearDir)
            }
        case "OSISAF":
            executeOSISAFCommands(dataType, year, destinationPath) //, yearDir)
        }
    }
    return nil
}

func executeEcpCommand(dataType, year, yearDir string) error {
    fmt.Printf("Executing ecp for %s to %s\n", dataType, yearDir)
    ecpCmd := exec.Command("ecp", fmt.Sprintf("ec:/fac2/CARRA2/obs/%s/%s/*", dataType, year), yearDir)
    fmt.Printf("COMMAND %s\n", ecpCmd)
    return ecpCmd.Run()
}

func executeOSISAFCommands(dataType, year, destinationPath string) error {
    fmt.Printf("Executing ecfsdir and rsync for %s to %s\n", dataType, year)
    //ecfsCmd := exec.Command("ecfsdir", fmt.Sprintf("ec:/fac2/CARRA2/obs/OSISAF_v2_20240424/%s %s", year), year)
    yearDir := filepath.Join(destinationPath, year)

    // Create the directory for the next year if it doesn't exist
    if _, err := os.Stat(yearDir); os.IsNotExist(err) {
          fmt.Printf("Creating directory %s\n", yearDir)
          if err := os.MkdirAll(yearDir, 0755); err != nil {
              return err
           }
     }

    // Using a bash wrapper for ecfsdir. For some reason it fails!
    ecfsCmd := exec.Command("/perm/nhd/CARRA2/harp-data-pipeline/go/data_fetchers/call_ecfs.sh",fmt.Sprintf("ec:/fac2/CARRA2/obs/OSISAF_v2_20240424/%s %s/%s", year, yearDir,year))
    //output, err_long := ecfsCmd.CombinedOutput()
    
    //fmt.Printf("ecfsdir COMMAND %s\n", ecfsCmd)
    //ecfsCmd.Dir = destinationPath
    //if err := ecfsCmd.Run(); err != nil {
    //    fmt.Printf("ERROR before rsync COMMAND ")
    //    fmt.Println(err)
    //    fmt.Printf("Command failed with error: %v\n", err_long)
    //    fmt.Printf("Output: %s\n", output)
    //    //return err
    //}
    
    fmt.Printf("before rsync COMMAND ")
    //	result := fmt.Sprintf("%s/%s??", yearDir, "file")

    rsyncCmd := exec.Command("rsync","-vaux", fmt.Sprintf("%s/%s/%s/",yearDir,year,"??"), yearDir)
    //rsyncCmd := exec.Command("/perm/nhd/CARRA2/harp-data-pipeline/go/data_fetchers/call_rsync.sh", fmt.Sprintf("%s/%s/%s/",yearDir,year,"??"), yearDir)
    //rsyncCmd := exec.Command("rsync", "-qaux", fmt.Sprintf("%s/%s/%s/",yearDir,year,"??"), yearDir)
    //rsyncCmd := exec.Command("mv", fmt.Sprintf("%s/%s/%s",yearDir,year,"??/*"), yearDir)
    //rsync -vaux /ec/res4/scratch/nhd/CARRA2/OSISAF/1991/??/ /ec/res4/scratch/nhd/CARRA2/OSISAF/1991
    fmt.Printf("rsync COMMAND %s\n", rsyncCmd)
    rsyncCmd.Dir = destinationPath
    if err := rsyncCmd.Run(); err != nil {
        return err
    }
    // Cleanup
    cleanDir := fmt.Sprintf("%s/%s",yearDir,year)
    fmt.Printf("Cleaning up directory %s\n", cleanDir)
    return os.RemoveAll(cleanDir)
}

func main() {

	// Command-line flag for specifying the YAML file
	yamlFile := flag.String("config", "streams.yml", "Path to the YAML configuration file")
	flag.Parse()
        init()
        fmt.Printf("The path of the script is: %s\n", projLibPath)

	// Load the YAML configuration
     	config, err := ReadYAML(*yamlFile)

	if err != nil {
		log.Fatalf("Failed to read YAML file: %v", err)
	}

	// Iterate through each stream and check the progress
	for streamName, stream := range config.STREAMS {
		if !stream.ACTIVE {
			fmt.Printf("Stream %s is inactive, skipping.\n", streamName)
			continue // Skip inactive streams
		}

		// Check the progress of the current stream
		// currentDTG, err := CheckProgress(streamName, stream.USER)
                // this one is for testing:
		 currentDTG, err := CheckProgressTest(streamName, stream.PROGLOG)
		log.Printf("Test user %s: %v", streamName, err)
		if err != nil {
			log.Printf("Failed to check progress for %s: %v", streamName, err)
			continue
		}

		// Parse the current DTG to extract the month
		currentTime, err := time.Parse("2006010215", currentDTG)
		if err != nil {
			log.Printf("Failed to parse DTG %s: %v", currentDTG, err)
			continue
		}

		// If the current month is December, execute actions
		if currentTime.Month() == time.December {
			nextYear := currentTime.AddDate(1, 0, 0).Format("2006")
			fmt.Printf("Executing actions for December, preparing for %s...\n", nextYear)
			if err := ExecuteActions(nextYear, config.OBS); err != nil {
				log.Printf("Failed to execute actions for %s: %v", streamName, err)
			}
		} else {
			fmt.Printf("No action needed for stream %s (current DTG: %s)\n", streamName, currentDTG)
		}
	}
}

