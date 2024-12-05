import datetime

def parse_timestamp(timestamp_str):
    # Convert timestamp string (YYYYMMDDHH) to datetime
    return datetime.datetime.strptime(timestamp_str[:10], '%Y%m%d%H')

def get_period_from_timestamp(timestamp):
    # Convert timestamp to YYYYMM format
    return timestamp.strftime('%Y%m')

def read_last_archival():
    # Read and parse last_archival_done.txt
    streams = {}
    with open('last_archival_done.txt', 'r') as f:
        for line in f:
            stream, period = line.strip().split()
            streams[stream] = period
    return streams

def read_periods():
    # Read and parse periods.txt
    current_states = {}
    with open('../../../../bash/job_submitters/periods.txt', 'r') as f:
        for line in f:
            parts = line.strip().split()
            stream = parts[0]
            timestamp = parts[2]  # Using the second column
            current_states[stream] = timestamp
    return current_states

def check_and_process():
    # Get the current state of all streams
    last_archived = read_last_archival()
    current_states = read_periods()
    
    # Check each stream
    for stream in last_archived.keys():
        if stream not in current_states:
            continue
            
        # Get the last archived period and current timestamp
        last_period = last_archived[stream]
        current_timestamp = current_states[stream]
        
        # Convert current timestamp to datetime
        current_dt = parse_timestamp(current_timestamp)
        current_period = get_period_from_timestamp(current_dt)
        #print(f"current and last periods: {current_period} {last_period}")
        
        # Compare periods
        #if current_period > last_period:
        last_dt = datetime.datetime.strptime(last_period, '%Y%m')
        if (current_dt - last_dt).days >= 62:
            next_month = last_dt.replace(day=1) + datetime.timedelta(days=32)
            next_period = next_month.strftime('%Y%m')
            # If current timestamp is in a later period, trigger processing
            print(f"Processing {stream} for {next_period}, since last processed was {last_period} (currently on {current_timestamp})")
            # calling the script
            import subprocess
            subprocess.run(['./submit_ecf_suite.sh', next_period])
        else:
            print(f"Doing nothing for {stream}. Last processed was {last_period} (currently on {current_timestamp})")

if __name__ == "__main__":
    check_and_process()
