from typing import Optional, Any, Dict

class TimeLimit:
    def __init__(self, data: Dict):
        self.set: bool = data.get("set", False)
        self.infinite: bool = data.get("infinite", False)
        self.number: Optional[int] = data.get("number")

    def __repr__(self) -> str:
        return f"<TimeLimit set={self.set} infinite={self.infinite} number={self.number}>"

class TimeComponent:
    def __init__(self, data: Dict):
        self.seconds: int = data.get("seconds", 0)
        self.microseconds: int = data.get("microseconds", 0)

    def total_seconds(self) -> float:
        return self.seconds + self.microseconds / 1_000_000

    def __repr__(self) -> str:
        return f"<TimeComponent {self.total_seconds():.6f} sec>"

class TimeInfo:
    def __init__(self, 
                    elapsed:int, 
                    start, 
                    end, 
                    suspended, 
                    system: Optional[TimeComponent],
                    user: Optional[TimeComponent],
                    total: Optional[TimeComponent]
                    ):
        self.elapsed = elapsed
        self.start = start
        self.end = end
        self.suspended = suspended
        self.system = system
        self.total = total
        self.user = user

    def __repr__(self) -> str:
        return (f"<TimeInfo elapsed={self.elapsed} start={self.start} end={self.end} "
                f"system={self.system} user={self.user}>")

    @classmethod
    def from_json(cls, data: Dict[str, Any]):

        return cls(
            elapsed = int(data.get("elapsed", -1)),
            end = int(data.get("end", -1)),
            start = int(data.get("start", -1)),
            suspended = int(data.get("suspended", -1)),
            system =  TimeComponent(data["system"]),
            total =  TimeComponent(data["total"]),
            user = TimeComponent(data["user"])
        )