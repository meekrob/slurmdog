from datetime import datetime
from SlurmTres import TRESData
from SlurmTime import TimeInfo  # assuming job steps also include time-related data
from dataclasses import dataclass
from typing import Optional, Dict, Any
from enum import Enum
import json


class ExitCodeStatus(Enum):
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"
    NODE_FAIL = "NODE_FAIL"
    PREEMPTED = "PREEMPTED"
    UNKNOWN = "UNKNOWN"

@dataclass
class ExitCode:
    status: ExitCodeStatus
    return_code: int

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "ExitCode":
        status_str = data.get("status", "UNKNOWN")
        return cls(
            status = ExitCodeStatus(status_str) if status_str in ExitCodeStatus._value2member_map_ else ExitCodeStatus.UNKNOWN, # check for valid enum
            return_code=data.get("return_code", 0)
        )

class JobStep:
    def __init__(self, pid: str,
        CPU: dict,
        kill_request_user: str,
        reservation: dict,
        script: dict,
        statistics: dict,
        step: dict,
        state: Optional[str] = None,
        nodes: Optional[dict] = None,
        time: Optional[TimeInfo] = None,
        tres: Optional[TRESData] = None,
        exit_code: Optional[ExitCode] = None,
        raw: Optional[dict] = None
    ):

      self.pid = pid
      self.state = state
      self.nodes = nodes
      self.CPU = CPU
      self.time = time
      self.tres = tres
      self.exit_code = exit_code
      self.kill_request_user = kill_request_user
      self.reservation = reservation
      self.script = script
      self.statistics = statistics
      self.step = step or {}
      self.raw = raw or {}

    @property
    def name(self) -> str:
        return self.step.get("name", None)
    
    @property
    def step_id(self) -> dict:
        return self.step.get("id", None)

    @classmethod
    def from_json(cls, data: Dict[str, Any]):
        time_data = data['time']
        time_info = TimeInfo.from_json(time_data) if any(
            k in time_data for k in ("start_time", "end_time", "elapsed")
        ) else None

        tres_data = TRESData.from_json(data["tres"]) if "tres" in data else None

        return cls(
            state=data.get("state", ""),
            time=time_info,
            tres=tres_data,
            exit_code=data.get("exit_code", ""),
            pid=data.get("pid", ""),
            kill_request_user=data.get("kill_request_user", ""),
            reservation=data.get("reservation", {}),
            script=data.get("script", {}),
            statistics=data.get("statistics", {}),
            step=data.get("step", {}),
            nodes=data.get("nodes", {}),
            CPU=data.get("CPU", {}),
            raw=data

        )

    def __repr__(self):
        parts = [f"<JobStep name={self.name!r}, state={self.state!r}>"]

        if self.tres:
            parts.append("  TRES Info:")
            if self.tres.allocated:
                parts.append("    Allocated:")
                for item in self.tres.allocated:
                    parts.append(f"      - {item}")
            if self.tres.requested:
                parts.append("    Requested:")
                for category, items in self.tres.requested.items():
                    parts.append(f"      {category.capitalize()}:")
                    for item in items:
                        parts.append(f"        - {item}")
            if self.tres.consumed:
                parts.append("    Consumed:")
                for category, items in self.tres.consumed.items():
                    parts.append(f"      {category.capitalize()}:")
                    for item in items:
                        parts.append(f"        - {item}")

        return "\n".join(parts)


if __name__ == "__main__":

    TEST_JSON = """
{
       "steps": [
         {
           "time": {
             "elapsed": 326,
             "end": 1741668146,
             "start": 1741667820,
             "suspended": 0,
             "system": {
               "seconds": 17,
               "microseconds": 997649
             },
             "total": {
               "seconds": 1217,
               "microseconds": 1047267
             },
             "user": {
               "seconds": 1200,
               "microseconds": 49618
             }
           },
           "exit_code": {
             "status": "SUCCESS",
             "return_code": 0
           },
           "nodes": {
             "count": 1,
             "range": "c3cpu-a2-u1-1",
             "list": [
               "c3cpu-a2-u1-1"
             ]
           },
           "tasks": {
             "count": 1
           },
           "pid": "",
           "CPU": {
             "requested_frequency": {
               "min": {
                 "set": true,
                 "infinite": false,
                 "number": 0
               },
               "max": {
                 "set": true,
                 "infinite": false,
                 "number": 0
               }
             },
             "governor": "0"
           },
           "kill_request_user": "",
           "state": "COMPLETED",
           "statistics": {
             "CPU": {
               "actual_frequency": 4640326097307697152
             },
             "energy": {
               "consumed": {
                 "set": true,
                 "infinite": false,
                 "number": 0
               }
             }
           },
           "step": {
             "id": {
               "job_id": 12079670,
               "step_id": "batch"
             },
             "name": "batch"
           },
           "task": {
             "distribution": "Unknown"
           },
           "tres": {
             "requested": {
               "max": [
                 {
                   "type": "cpu",
                   "name": "",
                   "id": 1,
                   "count": 1217620,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "mem",
                   "name": "",
                   "id": 2,
                   "count": 3462881280,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "energy",
                   "name": "",
                   "id": 3,
                   "count": 0,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "fs",
                   "name": "disk",
                   "id": 6,
                   "count": 5007810406,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "vmem",
                   "name": "",
                   "id": 7,
                   "count": 4352557056,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "pages",
                   "name": "",
                   "id": 8,
                   "count": 3267,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 }
               ],
               "min": [
                 {
                   "type": "cpu",
                   "name": "",
                   "id": 1,
                   "count": 1217620,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "mem",
                   "name": "",
                   "id": 2,
                   "count": 3462881280,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "energy",
                   "name": "",
                   "id": 3,
                   "count": 0,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "fs",
                   "name": "disk",
                   "id": 6,
                   "count": 5007810406,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "vmem",
                   "name": "",
                   "id": 7,
                   "count": 4352557056,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "pages",
                   "name": "",
                   "id": 8,
                   "count": 3267,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 }
               ],
               "average": [
                 {
                   "type": "cpu",
                   "name": "",
                   "id": 1,
                   "count": 1217620
                 },
                 {
                   "type": "mem",
                   "name": "",
                   "id": 2,
                   "count": 3462881280
                 },
                 {
                   "type": "energy",
                   "name": "",
                   "id": 3,
                   "count": 0
                 },
                 {
                   "type": "fs",
                   "name": "disk",
                   "id": 6,
                   "count": 5007810406
                 },
                 {
                   "type": "vmem",
                   "name": "",
                   "id": 7,
                   "count": 4352557056
                 },
                 {
                   "type": "pages",
                   "name": "",
                   "id": 8,
                   "count": 3267
                 }
               ],
               "total": [
                 {
                   "type": "cpu",
                   "name": "",
                   "id": 1,
                   "count": 1217620
                 },
                 {
                   "type": "mem",
                   "name": "",
                   "id": 2,
                   "count": 3462881280
                 },
                 {
                   "type": "energy",
                   "name": "",
                   "id": 3,
                   "count": 0
                 },
                 {
                   "type": "fs",
                   "name": "disk",
                   "id": 6,
                   "count": 5007810406
                 },
                 {
                   "type": "vmem",
                   "name": "",
                   "id": 7,
                   "count": 4352557056
                 },
                 {
                   "type": "pages",
                   "name": "",
                   "id": 8,
                   "count": 3267
                 }
               ]
             },
             "consumed": {
               "max": [
                 {
                   "type": "energy",
                   "name": "",
                   "id": 3,
                   "count": 0,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "fs",
                   "name": "disk",
                   "id": 6,
                   "count": 3185345,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 }
               ],
               "min": [
                 {
                   "type": "energy",
                   "name": "",
                   "id": 3,
                   "count": 0,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "fs",
                   "name": "disk",
                   "id": 6,
                   "count": 3185345,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 }
               ],
               "average": [
                 {
                   "type": "energy",
                   "name": "",
                   "id": 3,
                   "count": 0
                 },
                 {
                   "type": "fs",
                   "name": "disk",
                   "id": 6,
                   "count": 3185345
                 }
               ],
               "total": [
                 {
                   "type": "energy",
                   "name": "",
                   "id": 3,
                   "count": 0
                 },
                 {
                   "type": "fs",
                   "name": "disk",
                   "id": 6,
                   "count": 3185345
                 }
               ]
             },
             "allocated": [
               {
                 "type": "cpu",
                 "name": "",
                 "id": 1,
                 "count": 10
               },
               {
                 "type": "mem",
                 "name": "",
                 "id": 2,
                 "count": 38400
               },
               {
                 "type": "node",
                 "name": "",
                 "id": 4,
                 "count": 1
               }
             ]
           }
         },
         {
           "time": {
             "elapsed": 326,
             "end": 1741668146,
             "start": 1741667820,
             "suspended": 0,
             "system": {
               "seconds": 0,
               "microseconds": 679
             },
             "total": {
               "seconds": 0,
               "microseconds": 1365
             },
             "user": {
               "seconds": 0,
               "microseconds": 686
             }
           },
           "exit_code": {
             "status": "SUCCESS",
             "return_code": 0
           },
           "nodes": {
             "count": 1,
             "range": "c3cpu-a2-u1-1",
             "list": [
               "c3cpu-a2-u1-1"
             ]
           },
           "tasks": {
             "count": 1
           },
           "pid": "",
           "CPU": {
             "requested_frequency": {
               "min": {
                 "set": true,
                 "infinite": false,
                 "number": 0
               },
               "max": {
                 "set": true,
                 "infinite": false,
                 "number": 0
               }
             },
             "governor": "0"
           },
           "kill_request_user": "",
           "state": "COMPLETED",
           "statistics": {
             "CPU": {
               "actual_frequency": 4660073326142554112
             },
             "energy": {
               "consumed": {
                 "set": true,
                 "infinite": false,
                 "number": 0
               }
             }
           },
           "step": {
             "id": {
               "job_id": 12079670,
               "step_id": "extern"
             },
             "name": "extern"
           },
           "task": {
             "distribution": "Unknown"
           },
           "tres": {
             "requested": {
               "max": [
                 {
                   "type": "cpu",
                   "name": "",
                   "id": 1,
                   "count": 0,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "mem",
                   "name": "",
                   "id": 2,
                   "count": 0,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "energy",
                   "name": "",
                   "id": 3,
                   "count": 0,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "fs",
                   "name": "disk",
                   "id": 6,
                   "count": 5273,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "vmem",
                   "name": "",
                   "id": 7,
                   "count": 0,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "pages",
                   "name": "",
                   "id": 8,
                   "count": 0,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 }
               ],
               "min": [
                 {
                   "type": "cpu",
                   "name": "",
                   "id": 1,
                   "count": 0,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "mem",
                   "name": "",
                   "id": 2,
                   "count": 0,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "energy",
                   "name": "",
                   "id": 3,
                   "count": 0,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "fs",
                   "name": "disk",
                   "id": 6,
                   "count": 5273,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "vmem",
                   "name": "",
                   "id": 7,
                   "count": 0,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "pages",
                   "name": "",
                   "id": 8,
                   "count": 0,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 }
               ],
               "average": [
                 {
                   "type": "cpu",
                   "name": "",
                   "id": 1,
                   "count": 0
                 },
                 {
                   "type": "mem",
                   "name": "",
                   "id": 2,
                   "count": 0
                 },
                 {
                   "type": "energy",
                   "name": "",
                   "id": 3,
                   "count": 0
                 },
                 {
                   "type": "fs",
                   "name": "disk",
                   "id": 6,
                   "count": 5273
                 },
                 {
                   "type": "vmem",
                   "name": "",
                   "id": 7,
                   "count": 0
                 },
                 {
                   "type": "pages",
                   "name": "",
                   "id": 8,
                   "count": 0
                 }
               ],
               "total": [
                 {
                   "type": "cpu",
                   "name": "",
                   "id": 1,
                   "count": 0
                 },
                 {
                   "type": "mem",
                   "name": "",
                   "id": 2,
                   "count": 0
                 },
                 {
                   "type": "energy",
                   "name": "",
                   "id": 3,
                   "count": 0
                 },
                 {
                   "type": "fs",
                   "name": "disk",
                   "id": 6,
                   "count": 5273
                 },
                 {
                   "type": "vmem",
                   "name": "",
                   "id": 7,
                   "count": 0
                 },
                 {
                   "type": "pages",
                   "name": "",
                   "id": 8,
                   "count": 0
                 }
               ]
             },
             "consumed": {
               "max": [
                 {
                   "type": "energy",
                   "name": "",
                   "id": 3,
                   "count": 0,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "fs",
                   "name": "disk",
                   "id": 6,
                   "count": 1,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 }
               ],
               "min": [
                 {
                   "type": "energy",
                   "name": "",
                   "id": 3,
                   "count": 0,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 },
                 {
                   "type": "fs",
                   "name": "disk",
                   "id": 6,
                   "count": 1,
                   "task": 0,
                   "node": "c3cpu-a2-u1-1"
                 }
               ],
               "average": [
                 {
                   "type": "energy",
                   "name": "",
                   "id": 3,
                   "count": 0
                 },
                 {
                   "type": "fs",
                   "name": "disk",
                   "id": 6,
                   "count": 1
                 }
               ],
               "total": [
                 {
                   "type": "energy",
                   "name": "",
                   "id": 3,
                   "count": 0
                 },
                 {
                   "type": "fs",
                   "name": "disk",
                   "id": 6,
                   "count": 1
                 }
               ]
             },
             "allocated": [
               {
                 "type": "cpu",
                 "name": "",
                 "id": 1,
                 "count": 10
               },
               {
                 "type": "mem",
                 "name": "",
                 "id": 2,
                 "count": 38400
               },
               {
                 "type": "energy",
                 "name": "",
                 "id": 3,
                 "count": -2
               },
               {
                 "type": "node",
                 "name": "",
                 "id": 4,
                 "count": 1
               },
               {
                 "type": "billing",
                 "name": "",
                 "id": 5,
                 "count": 9
               }
             ]
           }
         }
       ]
}
"""

    data = json.loads(TEST_JSON)
    for step_data in data['steps']:
        step = JobStep.from_json(step_data)
        print(step)

