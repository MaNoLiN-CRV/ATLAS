"""
Performance thresholds configuration for filtering problematic queries.
"""

from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class PerformanceThresholds:
    """Configuration class for performance filtering thresholds."""
    
    # Time-based thresholds (in milliseconds)
    max_avg_elapsed_time_ms: float = 1000.0          # Queries with avg > 1 second
    max_total_elapsed_time_ms: float = 30000.0       # Queries with total > 30 seconds
    max_avg_cpu_time_ms: float = 5000.0              # Queries with avg CPU > 5 seconds
    
    # I/O thresholds
    max_avg_logical_reads: float = 10000.0           # Queries with avg logical reads > 10K
    max_avg_physical_reads: float = 1000.0           # Queries with avg physical reads > 1K
    max_total_logical_reads: int = 1000000           # Queries with total logical reads > 1M
    
    # Memory and spill thresholds
    min_spills_to_capture: int = 1                   # Any query with spills
    max_avg_grant_kb: float = 50000.0               # Queries with avg memory grant > 50MB
    
    # Execution frequency thresholds
    min_execution_count: int = 1                     # Minimum executions to consider
    max_execution_count_for_slow_queries: int = 10   # If slow and frequent, definitely capture
    
    # Buffer hit ratio threshold (lower is worse)
    min_buffer_hit_ratio: float = 80.0              # Buffer hit ratio < 80%
    
    # CPU efficiency threshold (lower is worse)
    min_cpu_efficiency_ratio: float = 50.0          # CPU efficiency < 50%
    
    # Database size considerations
    max_stored_queries_per_collection: int = 50      # Limit per collection cycle
    
    @classmethod
    def get_conservative_thresholds(cls) -> 'PerformanceThresholds':
        """Get more conservative thresholds for smaller databases."""
        return cls(
            max_avg_elapsed_time_ms=500.0,
            max_total_elapsed_time_ms=10000.0,
            max_avg_cpu_time_ms=2000.0,
            max_avg_logical_reads=5000.0,
            max_avg_physical_reads=500.0,
            max_stored_queries_per_collection=25
        )
    
    @classmethod
    def get_aggressive_thresholds(cls) -> 'PerformanceThresholds':
        """Get more aggressive thresholds for larger databases with many queries."""
        return cls(
            max_avg_elapsed_time_ms=2000.0,
            max_total_elapsed_time_ms=60000.0,
            max_avg_cpu_time_ms=10000.0,
            max_avg_logical_reads=20000.0,
            max_avg_physical_reads=2000.0,
            max_stored_queries_per_collection=100
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert thresholds to dictionary for serialization."""
        return {
            'max_avg_elapsed_time_ms': self.max_avg_elapsed_time_ms,
            'max_total_elapsed_time_ms': self.max_total_elapsed_time_ms,
            'max_avg_cpu_time_ms': self.max_avg_cpu_time_ms,
            'max_avg_logical_reads': self.max_avg_logical_reads,
            'max_avg_physical_reads': self.max_avg_physical_reads,
            'max_total_logical_reads': self.max_total_logical_reads,
            'min_spills_to_capture': self.min_spills_to_capture,
            'max_avg_grant_kb': self.max_avg_grant_kb,
            'min_execution_count': self.min_execution_count,
            'max_execution_count_for_slow_queries': self.max_execution_count_for_slow_queries,
            'min_buffer_hit_ratio': self.min_buffer_hit_ratio,
            'min_cpu_efficiency_ratio': self.min_cpu_efficiency_ratio,
            'max_stored_queries_per_collection': self.max_stored_queries_per_collection
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PerformanceThresholds':
        """Create thresholds from dictionary."""
        return cls(**data)
