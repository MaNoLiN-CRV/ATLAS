import json
from datetime import datetime
from typing import TypedDict, List, Union, Optional
from decimal import Decimal

class PerformanceDataDict(TypedDict):
    """Type definition for performance data JSON structure."""
    total_elapsed_time_ms: int
    total_cpu_time_ms: int
    total_logical_reads: int
    total_physical_reads: int
    execution_count: int
    avg_elapsed_time_ms: Decimal
    avg_cpu_time_ms: Decimal
    creation_time: datetime
    last_execution_time: datetime
    query_text: str
    query_plan: str



class RawPerformanceData:
    def __init__(self, data: List[PerformanceDataDict]):
        self.data = data

    def get_data(self) -> List[PerformanceDataDict]:
        """Returns the raw performance data."""
        return self.data
    
    def to_json(self, indent: Optional[int] = None) -> str:
        """Converts the raw performance data to JSON format."""
        return json.dumps(self.data, default=self._datetime_serializer, indent=indent)
    
    def _datetime_serializer(self, obj: Union[datetime, object]) -> str:
        """Custom serializer for datetime objects."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
    @classmethod
    def from_json(cls, json_string: str) -> 'RawPerformanceData':
        """Creates a RawPerformanceData instance from JSON string."""
        data = json.loads(json_string)
        # Convert ISO format strings back to datetime objects
        for item in data:
            if 'creation_time' in item:
                item['creation_time'] = datetime.fromisoformat(item['creation_time'])
            if 'last_execution_time' in item:
                item['last_execution_time'] = datetime.fromisoformat(item['last_execution_time'])
        return cls(data)