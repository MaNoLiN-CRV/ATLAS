
from datetime import datetime
import logging
from typing import List
from src.common import CustomMetrics, RawPerformanceData
from src.database import SQLiteRepository


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
        
    def get_all_metrics(self) -> List[CustomMetrics]:
        """Retrieve all metrics from the SQLite repository."""
        self.logger.info("Retrieving all metrics from repository")
        return self.sqlite_repository.get_all_metrics()

    def analyze(self, raw_data: RawPerformanceData) -> List[CustomMetrics]:
        """Analyze the raw performance data and return custom metrics."""
        metrics = []
        for item in raw_data.get_data():
            # Create a CustomMetrics instance for each performance data item
            metric = {
                'value': item,  # Pass the original item as the value
                'timestamp': datetime.now()
            }
            metrics.append(metric)
        self.logger.info("Performance analysis completed")
        return metrics