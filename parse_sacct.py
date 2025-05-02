#!/usr/bin/env python
import subprocess
import sys
def main():

    job_id = sys.argv[1]
    jobs = parse_sacct(job_id)
    for job in jobs:
        print(job)
        print_seff_output(job)

# Function to convert human-readable memory sizes (e.g., '320K', '4G') to bytes
def convert_to_bytes(mem_str: str) -> int:
    mem_str = mem_str.strip().upper()
    if mem_str.endswith('K'):
        return int(float(mem_str[:-1]) * 1024)
    elif mem_str.endswith('M'):
        return int(float(mem_str[:-1]) * 1024 ** 2)
    elif mem_str.endswith('G'):
        return int(float(mem_str[:-1]) * 1024 ** 3)
    elif mem_str.endswith('T'):
        return int(float(mem_str[:-1]) * 1024 ** 4)
    else:
        # Assume it's already in bytes if there's no suffix
        return int(float(mem_str))

def parse_sacct(job_id: str):
    # Run the sacct command
    cmd = [
        'sacct',
        '-P', '-n', '-a',
        '--format', 'JobID,User,Group,State,Cluster,AllocCPUS,REQMEM,TotalCPU,Elapsed,MaxRSS,ExitCode,NNodes,NTasks',
        '-j', job_id
    ]
    
    # Capture the output
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    # Split the output into lines and process
    lines = result.stdout.strip().split('\n')
    
    # List to hold the parsed job information
    jobs = []
    
    # Parse each line
    for line in lines:
        fields = line.split('|')
        
        # Map each field to a dictionary key
        job_data = {
            'JobID': fields[0],
            'User': fields[1],
            'Group': fields[2],
            'State': fields[3],
            'Cluster': fields[4],
            'AllocCPUS': fields[5],
            'REQMEM': fields[6],
            'TotalCPU': fields[7],
            'Elapsed': fields[8],
            'MaxRSS': fields[9],
            'ExitCode': fields[10],
            'NNodes': fields[11],
            'NTasks': fields[12]
        }
        
        jobs.append(job_data)
    
    return jobs

def calculate_efficiencies(job_data):
    # Convert memory and CPU times
    requested_mem = convert_to_bytes(job_data['REQMEM'])
    max_rss = convert_to_bytes(job_data['MaxRSS'])
    total_cpu = convert_to_seconds(job_data['TotalCPU'])  # We'll implement convert_to_seconds next
    
    # Example data for utilized memory (from TRESData):
    max_rss_utilized = 3462881280  # This would be retrieved based on your logic

    # Memory Efficiency
    memory_efficiency = (max_rss_utilized / requested_mem) * 100 if requested_mem else 0
    
    # Total CPU in seconds (convert to seconds)
    total_cpu_time = convert_to_seconds(job_data['TotalCPU'])
    
    # CPU Efficiency
    cpu_efficiency = (total_cpu_time / (int(job_data['AllocCPUS']) * 3600)) * 100 if total_cpu_time else 0
    
    return {
        'JobID': job_data['JobID'],
        'User': job_data['User'],
        'MaxRSS': job_data['MaxRSS'],
        'MaxRSS Utilized': max_rss_utilized,
        'Total CPU': total_cpu_time,
        'CPU Efficiency': cpu_efficiency,
        'Memory Efficiency': memory_efficiency
    }

# Function to convert time strings like "00:20:00" into seconds
def convert_to_seconds(time_str: str) -> int:
    if time_str:
        parts = time_str.split(':')
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = int(parts[2])
        return hours * 3600 + minutes * 60 + seconds
    return 0

def print_seff_output(job_data):
    efficiencies = calculate_efficiencies(job_data)
    
    print(f"Job ID: {efficiencies['JobID']}")
    print(f"User: {efficiencies['User']}")
    print(f"MaxRSS: {efficiencies['MaxRSS']}")
    print(f"MaxRSS Utilized: {efficiencies['MaxRSS Utilized']} bytes")
    print(f"Total CPU: {efficiencies['Total CPU']} seconds")
    print(f"CPU Efficiency: {efficiencies['CPU Efficiency']:.2f}%")
    print(f"Memory Efficiency: {efficiencies['Memory Efficiency']:.2f}%")

if __name__ == "__main__":
    main()