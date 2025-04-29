from typing import List, Dict, Optional
from dataclasses import dataclass

@dataclass
class TRESItem:
    type: str
    name: str = ""
    id: Optional[int] = None
    count: Optional[int] = None
    task: Optional[int] = None
    node: Optional[str] = None

    @classmethod
    def from_json(cls, data):
        return cls(
            type=data.get("type", ""),
            name=data.get("name", ""),
            id=data.get("id"),
            count=data.get("count"),
            task=data.get("task"),
            node=data.get("node"),
        )

    @classmethod
    def from_dict(cls, d: dict) -> "TRESItem":
        return cls(
            type_=d['type'],
            name=d.get('name', ''),
            id_=d['id'],
            count=d['count'],
            task=d.get('task'),
            node=d.get('node')
        )

    def __repr__(self):
        return f"TRESItem(type={self.type}, name={self.name}, id={self.id}, count={self.count})"


class TRESData:
    def __init__(self, 
                requested: Dict[str, List[TRESItem]], # has lists keyed on min, max, average 
                consumed:  Dict[str, List[TRESItem]], # has lists keyed on min, max, average 
                allocated: List[TRESItem]): # just lists
        self.requested = requested
        self.consumed = consumed
        self.allocated = allocated

    @classmethod
    def from_dict(cls, d: dict) -> "TRESData":
        requested = {
            key: [TRESItem.from_dict(item) for item in d['requested'].get(key, [])]
            for key in ['min', 'max', 'average', 'total']
        }
        consumed = {
            key: [TRESItem.from_dict(item) for item in d['consumed'].get(key, [])]
            for key in ['min', 'max', 'average', 'total']
        }
        allocated = [TRESItem.from_dict(item) for item in d.get('allocated', [])]
        return cls(requested=requested, consumed=consumed, allocated=allocated)

    def find_allocated(self, resource_type: str) -> Optional[TRESItem]:
        """Find an allocated resource by type (e.g., 'cpu', 'mem', 'node')."""
        for item in self.allocated:
            if item.type == resource_type:
                return item
        return None

    def find_requested_max(self, resource_type: str) -> Optional[TRESItem]:
        """Find requested max resource by type."""
        for item in self.requested.get('max', []):
            if item.type == resource_type:
                return item
        return None

    def find_consumed_total(self, resource_type: str) -> Optional[TRESItem]:
        """Find consumed total resource by type."""
        for item in self.consumed.get('total', []):
            if item.type == resource_type:
                return item
        return None

    def __repr__(self):
        return f"TRESData(allocated={self.allocated})"

    @classmethod
    def from_json(cls, data):
        def parse_items(item_list):
            return [TRESItem.from_json(item) for item in item_list]

        return cls(
            requested={k: parse_items(v) for k, v in data.get("requested", {}).items()},
            consumed={k: parse_items(v) for k, v in data.get("consumed", {}).items()},
            allocated=parse_items(data.get("allocated", [])),
        )


