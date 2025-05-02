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
            type=d['type'],
            name=d.get('name', ''),
            id=d['id'],
            count=d['count'],
            task=d.get('task'),
            node=d.get('node')
        )

    def __repr__(self):
        return f"TRESItem(type={self.type}, name={self.name}, id={self.id}, count={self.count})"


class TRESData:
    def __init__(self, 
                requested: Dict[str, List[TRESItem]],  # has lists keyed on min, max, average, total 
                consumed:  Dict[str, List[TRESItem]],  # has lists keyed on min, max, average, total 
                allocated: Dict[str, List[TRESItem]]): # only has total but keeping the same data format
        self.requested = requested
        self.consumed = consumed
        self.allocated = allocated

    def find_allocated(self, resource_type: str, summary_type = "total") -> Optional[TRESItem]:
        """Find an allocated resource by type (e.g., 'cpu', 'mem', 'node')."""
        for item in self.allocated[summary_type]:
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

        
        allocated = None
        requested = None
        consumed = None

        # initialize
        tres_data = {'requested': None, 'consumed': None, 'allocated': None}

        # walk the nested structure
        for category in tres_data.keys():
            if category in data:
                cat_dict = {}
                for summ in ['max', 'min', 'average', 'total']: 
                    if summ in data[category]: 
                        tresitemlist = data[category][summ]
                        tres_items = [ TRESItem.from_json(j) for j in tresitemlist ]
                        cat_dict[summ] = tres_items
                    elif category == 'allocated' and type(data[category] == type([])):                
                        cat_dict['total'] = [ TRESItem.from_json(j) for j in data[category] ]

                tres_data[category] = cat_dict
                
            else:
                Warning(f"data missing expected TRES category: {category}")

        return cls(
            requested=tres_data['requested'],
            consumed=tres_data['consumed'],
            allocated=tres_data['allocated']
        )


