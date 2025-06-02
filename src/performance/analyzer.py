
from datetime import datetime
import logging
from typing import List
from src.common.models import CustomMetrics, RawPerformanceData
from src.database.sqlite_repository import SQLiteRepository


class PerformanceAnalyzer:
    """A class to analyze performance data."""

    def __init__(self , sqlite_repository : SQLiteRepository):
        self.logger = logging.getLogger(__name__)
        self.sqlite_repository = sqlite_repository

    def process_data(self, raw_data: RawPerformanceData) -> None:
        """Process the raw performance data and sends it to the sqlite repository."""
        self.logger.info("Processing raw performance data")
        customMetrics = self.analyze(raw_data)
        self.sqlite_repository.save_metrics(customMetrics)
        self.logger.info("Data processing completed")

    def analyze(self, raw_data: RawPerformanceData) -> List[CustomMetrics]:
        """Analyze the raw performance data and return custom metrics."""
        metrics = []
        for item in raw_data.get_data():
            metric = CustomMetrics(
                value=RawPerformanceData(
                    total_elapsed_time_ms=item['total_elapsed_time_ms'],
                    total_cpu_time_ms=item['total_cpu_time_ms'],
                    total_logical_reads=item['total_logical_reads'],
                    total_physical_reads=item['total_physical_reads'],
                    execution_count=item['execution_count'],
                    avg_elapsed_time_ms=item['avg_elapsed_time_ms'],
                    avg_cpu_time_ms=item['avg_cpu_time_ms'],
                    creation_time=item['creation_time'],
                    last_execution_time=item['last_execution_time'],
                    query_text=item['query_text'],
                    query_plan=item['query_plan']
                ),
                timestamp=datetime.now()
            )
            metrics.append(metric)
        self.logger.info("Performance analysis completed")
        return metrics