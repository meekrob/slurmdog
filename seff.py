from SlurmJob import SlurmJob
from SlurmTres import TRESData, TRESItem
import json
import sys

def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <job_data.json>")
        sys.exit(1)

    json_file = sys.argv[1]
    with open(json_file) as f:
        data = json.load(f)

    meta = data.get("meta")
    command = meta.get("command")
    print(command)
    Slurm = meta.get("Slurm")
    print(Slurm)

    for job_data in data.get('jobs'):

        # Create a SlurmJob from the JSON dictionary
        job = SlurmJob.from_json(job_data)

        # Get SEFF-style info
        seff_info = job.seff_stats()

        # Print the SEFF fields
        for key, value in seff_info.items():
            print(f"{key}: {value}")

if __name__ == "__main__":
    main()

