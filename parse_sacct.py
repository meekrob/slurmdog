#!/usr/bin/env python
import subprocess
import sys
from typing import Optional, List

from numbers import Number
from math import nan

from typing import List, Dict, Any
from collections import defaultdict

def aggregate_sacct_rows(steps: List[Dict[str, Any]]) -> Dict[str, Any]:
    summary:dict = defaultdict(lambda: None)

    # Find top-level job (no "." in JobID)
    top_level = next((step for step in steps if '.' not in step['JobID']), None)
    
    # motivated by edge case which has both:
    # 1) a single step AND
    # 2) a '...' at the end of the JobID like jid_[r1-r2,r3-4,...]
    if len(steps) == 1:
        top_level = steps[0]


    if top_level:
        # Take directly from top-level step
        for field in ["JobID", 
                      "AllocCPUS", 
                      "REQMEM", 
                      "Elapsed", 
                      "NTasks",
                        "NNodes", 
                        "User", 
                        "Group", 
                        "State",
                        "Cluster", 
                        "ExitCode",
                        "Submit",
                        "Start",
                        "End",
                        "Account"]:
            summary[field] = top_level.get(field)

    # Aggregated fields
    total_cpu = 0.0
    elapsed = 0.0
    max_rss = 0
    jobnames = []

    for step in steps[1:]:
        if step.get("TotalCPU"):
            total_cpu += parse_time(step["TotalCPU"])
        if step.get("Elapsed"):
            elapsed =  max(elapsed, parse_time(step["Elapsed"]))
        if step.get("MaxRSS"):
            mem = convert_to_bytes(step["MaxRSS"])
            if mem is not None:
                max_rss = max(max_rss, mem)
        if step.get("JobName"):
            jobnames.append(step["JobName"])
        if 'REQMEM' not in summary and step.get('REQMEM'):
            summary["REQMEM"] = step.get("REQMEM")
            

    summary["TotalCPU"] = total_cpu
    summary["Elapsed"] = seconds_to_timeformat(int(elapsed))
    summary["MaxRSS"] = max_rss

    if top_level and top_level.get('JobName'):
        jobnames.insert(0, top_level['JobName'])
    summary["JobNames"] = ",".join(jobnames)

    return dict(summary)


def format_size(bytes:int) -> str:
    units:list[str] = ["B",  "KB", "MB", "GB", "TB", "PB"]
    value:float = float(bytes)
    while int(value / 1024) > 0:
        if len(units) == 1: 
            break

        value /= 1024
        units = units[1:]

    return f"{value:.2f} {units[0]}"


def main():

    print_seff_output_tsv_header()

    for jid, jobs in parse_from_stdin():
    
        steps_aggregated  = aggregate_sacct_rows(jobs) # yields an aggregation of (usually) 3 lines of input
        print_seff_output(steps_aggregated)

        #for job in jobs:
        #   print(job)
        #  print_seff_output(job)

# Function to convert human-readable memory sizes (e.g., '320K', '4G') to bytes
def convert_to_bytes(mem_str: str) -> int:

    if mem_str == '': return 0

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

# THIS IS NOW DEFUNCT IN ORDER TO READ INPUT FROM STDIN
def parse_sacct(job_id: str):

    if job_id != "":
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

    else:
        alpine_lines = """
12078642_6|naly@colostate.edu|nalypgrp@colostate.edu|COMPLETED|alpine|10|37.50G|20:18.048|00:05:26||0:0|1|
12078642_6.batch|||COMPLETED|alpine|10||20:18.047|00:05:26|3381720K|0:0|1|1
12078642_6.extern|||COMPLETED|alpine|10||00:00.001|00:05:26|0|0:0|1|1
""".strip().split('\n')
        
        # from riviera - Elapsed has the "days format"
        riviera_lines = """
52791|dking|dking|TIMEOUT|slurm|128|491554M|00:29.686|2-00:00:02||0:0|1|
52791.batch|||CANCELLED|slurm|128||00:29.686|2-00:00:03|36404576K|0:15|1|1
""".strip().split('\n')
    
    lines = alpine_lines + riviera_lines

    # List to hold the parsed job information
    for jid, jobs in parse_sacct_lines(lines):
        yield jid, jobs

def parse_from_stdin():
    lines = sys.stdin.readlines()
    for jid, jobs in parse_sacct_lines(lines):
        yield jid, jobs

def get_job_id_prefix(job_id_str):
    if job_id_str.find('.') > 0:
        parts = job_id_str.split('.')
        return parts[0]
    
    return job_id_str

def parse_sacct_lines(lines):
    jobs = []
    last_job_id = None
    # Parse each line
    for line in lines:
        if line.find('JobID') >= 0:
            # this is a header
            continue
        fields = line.strip().split('|')
        job_id_prefix = get_job_id_prefix(fields[0])

        if last_job_id is not None and job_id_prefix != last_job_id:
            yield last_job_id, jobs
            jobs = []

        last_job_id = job_id_prefix


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
            'NTasks': fields[12],
            'JobName': fields[13],
            'Submit': fields[14],
            'Start': fields[15],
            'End': fields[16],
            'Account': fields[17]
        }
            
        jobs.append(job_data)

    
    yield last_job_id, jobs

def calculate_efficiencies(job_data):
    # Convert memory and CPU times
    requested_mem = convert_to_bytes(job_data['REQMEM'])
    if type(job_data['MaxRSS']) == type(str()):
        max_rss = convert_to_bytes(job_data['MaxRSS'])
    else:
        max_rss = job_data['MaxRSS']
    
    
    # Example data for utilized memory (from TRESData):
    max_rss_utilized = max_rss  # This would be retrieved based on your logic

    # Memory Efficiency
    memory_efficiency = (max_rss_utilized / requested_mem) * 100 if requested_mem else 0
    
    # Total CPU in seconds (convert to seconds)
    if isinstance(job_data['TotalCPU'], Number):
        total_cpu_time = job_data['TotalCPU']
    else:
        total_cpu_time = parse_total_cpu_time(job_data['TotalCPU'])
    
    cpu_wall_time = (parse_time(job_data['Elapsed']) * int(job_data['AllocCPUS']))

    # CPU Efficiency
    try:
        cpu_efficiency = (total_cpu_time / cpu_wall_time) * 100 if total_cpu_time else 0
    except ZeroDivisionError:
        print(f"Warning 0 Elapsed time {job_data['JobID']}", file=sys.stderr)
        cpu_efficiency = nan
    
    return {
        'JobID': job_data['JobID'],
        'User': job_data['User'],
        'MaxRSS': job_data['MaxRSS'],
        'MaxRSS Utilized': max_rss_utilized,
        'Total CPU': total_cpu_time,
        'CPU Efficiency': cpu_efficiency,
        'CPU Wall-time': cpu_wall_time,
        'Memory Utilized': max_rss_utilized,
        'Memory Efficiency': memory_efficiency,
        'REQMEM': job_data['REQMEM'],
        'Submit': job_data['Submit'],
        'Start': job_data['Start'],
        'End': job_data['End']
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

def seconds_to_timeformat(seconds: int) -> str:
    days = int(seconds/(3600*24))
    seconds_remaining = seconds % (3600*24)
    hours = int(seconds_remaining/3600)
    seconds_remaining = seconds_remaining % 3600
    minutes = int(seconds_remaining/60)
    seconds = int(seconds_remaining % 60)

    day_str = ''
    if days > 0: day_str = f"{days}-"
    return f"{day_str}{hours:02d}:{minutes:02d}:{seconds:02d}"


def parse_time(s: Optional[str]) -> float:
    """
    Parse Slurm-style time strings into seconds.
    Returns 0.0 if input is missing or invalid.
    """
    if not s or s.strip() in {"", "Unknown"}:
        return 0.0

    try:
        parts = s.strip().split(":")
        if len(parts) == 3:
            hh, m, sec = parts
            if hh.find('-') > 0:
                days,hours = hh.split('-')
                h = int(days) * 24 + int(hours)
            else:
                h = int(hh)
            return h * 3600 + int(m) * 60 + float(sec)
        elif len(parts) == 2:
            m, sec = parts
            return int(m) * 60 + float(sec)
        elif len(parts) == 1:
            return float(parts[0])
    except (ValueError, TypeError):
        return 0.0
    
    return 0


def parse_total_cpu_time(time_str: str) -> float:
    # Format: HH:MM:SS.sss or MM:SS.sss
    
    time_str = time_str.strip()
    parts = time_str.split(':')
    
    if len(parts) == 3:
        hours, minutes, seconds = parts
        return int(hours) * 3600 + int(minutes) * 60 + float(seconds)
    elif len(parts) == 2:
        minutes, seconds = parts
        return int(minutes) * 60 + float(seconds)
    else:
        # Fallback for just seconds
        return float(parts[0])

def print_seff_output(job_data):
    efficiencies = calculate_efficiencies(job_data)
    #print_seff_output_description(efficiencies, job_data)
    print_seff_output_tsv(efficiencies, job_data)

def print_seff_output_tsv_header():
    print_seff_output_tsv(None, None, True)

def print_seff_output_tsv(efficiencies, job_data, print_header=False): 
    if print_header:
        print('JobID',
              'User',
              'Group',
              'State',
              'ExitCode',
            'NNodes', 
            'AllocCPUS',
            'CPU_Utilized',
            'CPU_Efficiency',
            'core_walltime',
            'Elapsed',
            'Elapsed_raw',
            'MaxRSS_Utilized',
            'MaxRSS_Utilized_raw',
            'REQMEM',
            'memory_efficiency',
            'JobNames',
            'Submit',
            'Start',
            'End',
            'Account',
            sep="\t")
        
        return

    print(efficiencies['JobID'], 
          efficiencies['User'],
          job_data['Group'],
          job_data['State'],
          job_data['ExitCode'],
          job_data['NNodes'],
          job_data['AllocCPUS'],
          seconds_to_timeformat(efficiencies['Total CPU']),
          efficiencies['CPU Efficiency'],
          seconds_to_timeformat(efficiencies['CPU Wall-time']),
          job_data['Elapsed'],
          parse_time(job_data['Elapsed']),
          format_size(efficiencies['MaxRSS Utilized']),
          efficiencies['MaxRSS Utilized'],
          convert_to_bytes(efficiencies['REQMEM']),
          efficiencies['Memory Efficiency'],
          job_data['JobNames'],
          efficiencies['Submit'],
          efficiencies['Start'],
          efficiencies['End'],
          job_data['Account'],
          sep="\t")

def print_seff_output_description(efficiencies, job_data): 
    print(f"Job ID: {efficiencies['JobID']}")
    print(f"User/Group: {efficiencies['User']}/{job_data['Group']}")
    print(f"State: {job_data['State']} (exit code {job_data['ExitCode']})")
    print(f"Nodes: {job_data['NNodes']}")
    print(f"Cores per Node: {job_data['AllocCPUS']}")
    
    print(f"CPU Utilized: {seconds_to_timeformat(efficiencies['Total CPU'])}")
    print(f"CPU Efficiency: {efficiencies['CPU Efficiency']:.2f}% of {seconds_to_timeformat(efficiencies['CPU Wall-time'])} core-walltime")
    print(f"Job Wall-clock time: {job_data['Elapsed']}")
    print(f"Memory Utilized: {format_size(efficiencies['MaxRSS Utilized'])}")  

    req_mem_bytes = convert_to_bytes(efficiencies['REQMEM'])

    print(f"Memory Efficiency: {efficiencies['Memory Efficiency']:.2f}% of {format_size(req_mem_bytes)}")
    print()
    sys.stdout.flush()

if __name__ == "__main__":
    main()

