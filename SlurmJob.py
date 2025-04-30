from SlurmTres import TRESData, TRESItem
from SlurmTime import TimeInfo
from JobStep import JobStep
from typing import Optional, List, Dict
import json

class SlurmJob:
    def __init__(self, job_id, name, nodes, partition, qos, required_cpus, required_memory_per_cpu, time, steps:List[JobStep]):
        self.job_id = job_id
        self.name = name
        self.nodes = nodes
        self.partition = partition
        self.qos = qos
        self.required_cpus = required_cpus
        self.required_memory_per_cpu = required_memory_per_cpu  # in MiB
        self.time = time  # SlurmTime instance
        self.tres = tres  # TRESData instance

    @classmethod
    def from_json(cls, data):
        job_id = data.get("job_id")
        name = data.get("name")
        nodes = data.get("nodes")
        partition = data.get("partition")
        qos = data.get("qos")

        required = data.get("required", {})
        required_cpus = required.get("CPUs", 0)
        required_memory_per_cpu_info = required.get("memory_per_cpu", {})
        required_memory_per_cpu = required_memory_per_cpu_info.get("number", 0)

        time_data = data.get("time", {})
        time = SlurmTime.from_json(time_data) if time_data else None

        tres_data = data.get("tres", {})
        tres = TRESData.from_json(tres_data) if tres_data else None

        return cls(
            job_id=job_id,
            name=name,
            nodes=nodes,
            partition=partition,
            qos=qos,
            required_cpus=required_cpus,
            required_memory_per_cpu=required_memory_per_cpu,
            time=time,
            tres=tres
        )


    def seff_stats(self) -> Dict:
        """Compute seff-like statistics and return as a dictionary."""
        elapsed = self.time.elapsed if self.time else 0
        cpus_requested = self.required.cpus if self.required else 0

        # CPU time is (user + system)
        cpu_seconds = 0
        if self.time:
            if self.time.user:
                cpu_seconds += self.time.user.total_seconds()
            if self.time.system:
                cpu_seconds += self.time.system.total_seconds()

        # CPU efficiency: CPU time used divided by (CPUs * elapsed time)
        cpu_efficiency = None
        if elapsed and cpus_requested:
            cpu_efficiency = (cpu_seconds / (elapsed * cpus_requested)) * 100

        # Memory efficiency: Unknown without memory used info
        # We can report requested memory per CPU
        mem_per_cpu_mb = None
        if self.required and self.required.memory_per_cpu:
            mem_per_cpu_mb = self.required.memory_per_cpu.number  # Assuming MB

        return {
            "Job ID": self.job_id,
            "Elapsed Time (seconds)": elapsed,
            "CPU Time (seconds)": round(cpu_seconds, 3),
            "CPU Efficiency (%)": round(cpu_efficiency, 2) if cpu_efficiency is not None else None,
            "Memory Requested (per CPU)": f"{mem_per_cpu_mb} MB" if mem_per_cpu_mb is not None else None,
            "Memory Efficiency (%)": "Unknown (no usage data)"  # we would need 'MaxRSS' or similar field
        }

    def __repr__(self) -> str:
        return (f"<Job id={self.job_id} name={self.name} partition={self.partition} "
                f"nodes={self.nodes}>")

class Priority:
    def __init__(self, data: Dict):
        self.set: bool = data.get("set", False)
        self.infinite: bool = data.get("infinite", False)
        self.number: Optional[int] = data.get("number")

    def __repr__(self) -> str:
        return f"<Priority set={self.set} number={self.number}>"

class MemoryRequirement:
    def __init__(self, data: Dict):
        self.set: bool = data.get("set", False)
        self.infinite: bool = data.get("infinite", False)
        self.number: Optional[int] = data.get("number")

    def __repr__(self) -> str:
        return f"<MemoryRequirement set={self.set} number={self.number}>"

class RequiredResources:
    def __init__(self, data: Dict):
        self.cpus: Optional[int] = data.get("CPUs")
        self.memory_per_cpu: Optional[MemoryRequirement] = (
            MemoryRequirement(data["memory_per_cpu"]) if "memory_per_cpu" in data else None
        )
        self.memory_per_node: Optional[MemoryRequirement] = (
            MemoryRequirement(data["memory_per_node"]) if "memory_per_node" in data else None
        )
        self.memory: Optional[int] = data.get("memory")

    def __repr__(self) -> str:
        return (f"<RequiredResources CPUs={self.cpus} "
                f"memory_per_cpu={self.memory_per_cpu} "
                f"memory_per_node={self.memory_per_node} "
                f"memory={self.memory}>")

if __name__  == "__main__":
    with open("sacct.json") as fh:
        json_txt = fh.read()

    job = SlurmJob.from_json(json.loads(json_txt))
    print(job)
