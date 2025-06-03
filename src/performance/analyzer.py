
from datetime import datetime
import logging
from typing import List, Dict, Any
from src.common import CustomMetrics, RawPerformanceData
from src.database import SQLiteRepository
from .performance_thresholds import PerformanceThresholds


class PerformanceAnalyzer:
    """A class to analyze performance data and filter problematic queries."""

    def __init__(self, sqlite_repository: SQLiteRepository, 
                 thresholds: PerformanceThresholds = None):
        self.logger = logging.getLogger(__name__)
        self.sqlite_repository = sqlite_repository
        self.thresholds = thresholds or PerformanceThresholds()
        self._query_cache = {}  # Cache to avoid duplicates

    def process_data(self, raw_data: RawPerformanceData) -> None:
        """Process the raw performance data and sends filtered metrics to the sqlite repository."""
        self.logger.info("Processing raw performance data with intelligent filtering")
        
        # Filter and analyze data
        filtered_metrics = self._filter_problematic_queries(raw_data)
        
        if filtered_metrics:
            self.logger.info(f"Filtered {len(filtered_metrics)} problematic queries from {len(raw_data.get_data())} total queries")
            self.sqlite_repository.save_metrics(filtered_metrics)
        else:
            self.logger.info("No problematic queries found in this collection cycle")
        
        self.logger.info("Data processing completed")
        
    def get_all_metrics(self) -> List[CustomMetrics]:
        """Retrieve all metrics from the SQLite repository."""
        self.logger.info("Retrieving all metrics from repository")
        return self.sqlite_repository.get_all_metrics()

    def _filter_problematic_queries(self, raw_data: RawPerformanceData) -> List[CustomMetrics]:
        """Filter queries to only include problematic ones based on performance thresholds."""
        problematic_queries = []
        processed_hashes = set()
        
        for item in raw_data.get_data():
            # Skip if we've already processed this query hash to avoid duplicates
            query_hash = item.get('query_hash')
            if query_hash and query_hash in processed_hashes:
                continue
            
            if self._is_problematic_query(item):
                metric = {
                    'value': item,
                    'timestamp': datetime.now()
                }
                problematic_queries.append(metric)
                
                if query_hash:
                    processed_hashes.add(query_hash)
                
                # Log why this query was flagged
                self._log_problematic_query_reason(item)
        
        # Limit the number of queries per collection to prevent database bloat
        if len(problematic_queries) > self.thresholds.max_stored_queries_per_collection:
            # Sort by severity and take the worst ones
            problematic_queries.sort(key=self._calculate_severity_score, reverse=True)
            problematic_queries = problematic_queries[:self.thresholds.max_stored_queries_per_collection]
            self.logger.info(f"Limited to {self.thresholds.max_stored_queries_per_collection} worst queries")
        
        return problematic_queries

    def _is_problematic_query(self, query_data: Dict[str, Any]) -> bool:
        """Determine if a query is problematic based on multiple criteria."""
        
        # Extract metrics with safe defaults
        avg_elapsed_time = float(query_data.get('avg_elapsed_time_ms', 0))
        total_elapsed_time = float(query_data.get('total_elapsed_time_ms', 0))
        avg_cpu_time = float(query_data.get('avg_cpu_time_ms', 0))
        avg_logical_reads = float(query_data.get('avg_logical_reads', 0))
        avg_physical_reads = float(query_data.get('avg_physical_reads', 0))
        total_logical_reads = int(query_data.get('total_logical_reads', 0))
        total_spills = int(query_data.get('total_spills', 0))
        avg_grant_kb = float(query_data.get('avg_grant_kb', 0))
        execution_count = int(query_data.get('execution_count', 0))
        buffer_hit_ratio = float(query_data.get('buffer_hit_ratio', 100))
        cpu_efficiency_ratio = float(query_data.get('cpu_efficiency_ratio', 100))
        
        # Check against thresholds
        criteria_met = []
        
        # Time-based criteria
        if avg_elapsed_time > self.thresholds.max_avg_elapsed_time_ms:
            criteria_met.append(f"High avg elapsed time: {avg_elapsed_time:.2f}ms")
        
        if total_elapsed_time > self.thresholds.max_total_elapsed_time_ms:
            criteria_met.append(f"High total elapsed time: {total_elapsed_time:.2f}ms")
        
        if avg_cpu_time > self.thresholds.max_avg_cpu_time_ms:
            criteria_met.append(f"High avg CPU time: {avg_cpu_time:.2f}ms")
        
        # I/O criteria
        if avg_logical_reads > self.thresholds.max_avg_logical_reads:
            criteria_met.append(f"High avg logical reads: {avg_logical_reads:.0f}")
        
        if avg_physical_reads > self.thresholds.max_avg_physical_reads:
            criteria_met.append(f"High avg physical reads: {avg_physical_reads:.0f}")
        
        if total_logical_reads > self.thresholds.max_total_logical_reads:
            criteria_met.append(f"High total logical reads: {total_logical_reads}")
        
        # Memory and spill criteria
        if total_spills >= self.thresholds.min_spills_to_capture:
            criteria_met.append(f"Memory spills detected: {total_spills}")
        
        if avg_grant_kb > self.thresholds.max_avg_grant_kb:
            criteria_met.append(f"High memory grant: {avg_grant_kb:.0f}KB")
        
        # Efficiency criteria
        if buffer_hit_ratio < self.thresholds.min_buffer_hit_ratio:
            criteria_met.append(f"Low buffer hit ratio: {buffer_hit_ratio:.2f}%")
        
        if cpu_efficiency_ratio < self.thresholds.min_cpu_efficiency_ratio:
            criteria_met.append(f"Low CPU efficiency: {cpu_efficiency_ratio:.2f}%")
        
        # Special case: slow queries that execute frequently are especially problematic
        if (avg_elapsed_time > self.thresholds.max_avg_elapsed_time_ms / 2 and 
            execution_count > self.thresholds.max_execution_count_for_slow_queries):
            criteria_met.append(f"Frequent slow query: {execution_count} executions")
        
        # Store criteria for logging
        if criteria_met:
            query_data['_problematic_criteria'] = criteria_met
            return True
        
        return False

    def _calculate_severity_score(self, metric: Dict[str, Any]) -> float:
        """Calculate a severity score for prioritizing which queries to store."""
        query_data = metric['value']
        
        score = 0.0
        
        # Weight factors for different metrics
        avg_elapsed_time = float(query_data.get('avg_elapsed_time_ms', 0))
        total_elapsed_time = float(query_data.get('total_elapsed_time_ms', 0))
        avg_logical_reads = float(query_data.get('avg_logical_reads', 0))
        total_spills = int(query_data.get('total_spills', 0))
        execution_count = int(query_data.get('execution_count', 1))
        
        # Higher score = more problematic
        score += avg_elapsed_time * 0.3  # 30% weight on average time
        score += (total_elapsed_time / 1000) * 0.2  # 20% weight on total time
        score += (avg_logical_reads / 1000) * 0.2  # 20% weight on I/O
        score += total_spills * 1000  # High penalty for spills
        score += (execution_count * avg_elapsed_time / 1000) * 0.3  # Impact factor
        
        return score

    def _log_problematic_query_reason(self, query_data: Dict[str, Any]) -> None:
        """Log why a query was flagged as problematic."""
        criteria = query_data.get('_problematic_criteria', [])
        query_preview = query_data.get('query_text', 'Unknown')[:50] + "..."
        
        if criteria:
            self.logger.debug(f"Flagged query '{query_preview}' - Criteria: {', '.join(criteria)}")

    def analyze(self, raw_data: RawPerformanceData) -> List[CustomMetrics]:
        """Analyze the raw performance data and return custom metrics (now with filtering)."""
        self.logger.info("Performance analysis with intelligent filtering started")
        return self._filter_problematic_queries(raw_data)

    def update_thresholds(self, new_thresholds: PerformanceThresholds) -> None:
        """Update the performance thresholds."""
        self.thresholds = new_thresholds
        self.logger.info("Performance thresholds updated")

    def get_current_thresholds(self) -> PerformanceThresholds:
        """Get the current performance thresholds."""
        return self.thresholds