from typing import Optional, List, Dict

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
    def __init__(self, data: Dict):
        self.elapsed: Optional[int] = data.get("elapsed")
        self.eligible: Optional[int] = data.get("eligible")
        self.end: Optional[int] = data.get("end")
        self.start: Optional[int] = data.get("start")
        self.submission: Optional[int] = data.get("submission")
        self.suspended: Optional[int] = data.get("suspended")

        self.system: Optional[TimeComponent] = (
            TimeComponent(data["system"]) if "system" in data else None
        )
        self.limit: Optional[TimeLimit] = (
            TimeLimit(data["limit"]) if "limit" in data else None
        )
        self.total: Optional[TimeComponent] = (
            TimeComponent(data["total"]) if "total" in data else None
        )
        self.user: Optional[TimeComponent] = (
            TimeComponent(data["user"]) if "user" in data else None
        )

    def __repr__(self) -> str:
        return (f"<TimeInfo elapsed={self.elapsed} start={self.start} end={self.end} "
                f"system={self.system} user={self.user}>")

