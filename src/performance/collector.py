import logging
from typing import List, Optional
from src.common import PerformanceDataDict, RawPerformanceData
from src.database import MSSQLConnector

class PerformanceCollector:
    """A performance collector that gathers SQL Server performance metrics."""
    
    PERFORMANCE_QUERY = """
    SELECT TOP 10
        total_elapsed_time / 1000 AS total_elapsed_time_ms,
        total_worker_time / 1000 AS total_cpu_time_ms,
        total_logical_reads AS total_logical_reads,
        total_physical_reads AS total_physical_reads,
        execution_count,
        CAST((total_elapsed_time / 1000.0) / execution_count AS DECIMAL(18,2)) AS avg_elapsed_time_ms,
        CAST((total_worker_time / 1000.0) / execution_count AS DECIMAL(18,2)) AS avg_cpu_time_ms,
        creation_time,
        last_execution_time,
        st.text AS query_text,
        qp.query_plan
    FROM sys.dm_exec_query_stats AS qs
    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
    CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
    WHERE st.text NOT LIKE '%sys.dm_exec_query_stats%'
      AND st.text NOT LIKE '%SELECT @@VERSION%'
    ORDER BY total_elapsed_time DESC;
    """

    def __init__(self, connector: MSSQLConnector):
        self.connector = connector
        self.logger = logging.getLogger(__name__)
        self._is_started = False

    def start(self):
        """Initialize the collector and verify database connection."""
        try:
            # Test connection
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
            self._is_started = True
            self.logger.info("Performance collector started successfully")
        except Exception as e:
            self.logger.error(f"Failed to start collector: {e}")
            raise

    def collect(self) -> Optional[RawPerformanceData]:
        """Collect performance data from the database."""
        if not self._is_started:
            raise RuntimeError("Collector not started. Call start() first.")
        
        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(self.PERFORMANCE_QUERY)
                
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                # Convert rows to list of dictionaries
                performance_data = []
                for row in rows:
                    row_dict = dict(zip(columns, row))
                    performance_data.append(row_dict)
                
                self.logger.info(f"Collected {len(performance_data)} performance records")
                return RawPerformanceData(performance_data)
                
        except Exception as e:
            self.logger.error(f"Failed to collect performance data: {e}")
            return None

    def stop(self):
        """Stop the collector and cleanup resources."""
        self._is_started = False
        self.logger.info("Performance collector stopped")

    def is_started(self) -> bool:
        """Check if the collector is started."""
        return self._is_started