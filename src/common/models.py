import json
from datetime import datetime
from typing import TypedDict, List, Union, Optional
from decimal import Decimal


# Performance data models used in the collector module.

class PerformanceDataDict(TypedDict):
    """Type definition for performance data JSON structure."""
    total_elapsed_time_ms: int
    total_cpu_time_ms: int
    total_logical_reads: int
    total_physical_reads: int
    total_logical_writes: int
    execution_count: int
    avg_elapsed_time_ms: Decimal
    avg_cpu_time_ms: Decimal
    avg_logical_reads: Decimal
    avg_physical_reads: Decimal
    avg_logical_writes: Decimal
    creation_time: datetime
    last_execution_time: datetime
    query_text: str
    query_plan: str
    min_elapsed_time_ms: int
    max_elapsed_time_ms: int
    min_cpu_time_ms: int
    max_cpu_time_ms: int
    plan_generation_num: int
    total_rows: int
    avg_rows_returned: Decimal
    total_dop: int
    avg_dop: Decimal
    total_grant_kb: int
    avg_grant_kb: Decimal
    total_used_grant_kb: int
    avg_used_grant_kb: Decimal
    total_ideal_grant_kb: int
    avg_ideal_grant_kb: Decimal
    total_reserved_threads: int
    total_used_threads: int
    total_clr_time_ms: int
    avg_clr_time_ms: Decimal
    total_spills: int
    avg_spills: Decimal
    buffer_hit_ratio: Decimal
    cpu_efficiency_ratio: Decimal
    query_hash: str
    query_plan_hash: str
    collection_timestamp: datetime



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

# ///////////////////////////////////////////////

# Data analyzer models used in the analyzer module.

class CustomMetrics(TypedDict):
    """Type definition for custom metrics."""
    value = RawPerformanceData
    timestamp: datetime
