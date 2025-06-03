import logging
from typing import List, Optional
from src.common import PerformanceDataDict, RawPerformanceData
from src.database import MSSQLConnector

class PerformanceCollector:
    """A performance collector that gathers SQL Server performance metrics."""
    
    PERFORMANCE_QUERY = """
    SELECT TOP 50
        -- Basic timing metrics
        total_elapsed_time / 1000 AS total_elapsed_time_ms,
        total_worker_time / 1000 AS total_cpu_time_ms,
        
        -- I/O metrics
        total_logical_reads AS total_logical_reads,
        total_physical_reads AS total_physical_reads,
        total_logical_writes AS total_logical_writes,
        
        -- Execution metrics
        execution_count,
        
        -- Average calculations
        CAST((total_elapsed_time / 1000.0) / execution_count AS DECIMAL(18,2)) AS avg_elapsed_time_ms,
        CAST((total_worker_time / 1000.0) / execution_count AS DECIMAL(18,2)) AS avg_cpu_time_ms,
        CAST(total_logical_reads / CAST(execution_count AS DECIMAL(18,2)) AS DECIMAL(18,2)) AS avg_logical_reads,
        CAST(total_physical_reads / CAST(execution_count AS DECIMAL(18,2)) AS DECIMAL(18,2)) AS avg_physical_reads,
        CAST(total_logical_writes / CAST(execution_count AS DECIMAL(18,2)) AS DECIMAL(18,2)) AS avg_logical_writes,
        
        -- Time-based data
        creation_time,
        last_execution_time,
        
        -- Query information
        st.text AS query_text,
        CAST(ISNULL(qp.query_plan, '') AS NVARCHAR(MAX)) AS query_plan,
        
        -- Min/Max metrics
        min_elapsed_time / 1000 AS min_elapsed_time_ms,
        max_elapsed_time / 1000 AS max_elapsed_time_ms,
        min_worker_time / 1000 AS min_cpu_time_ms,
        max_worker_time / 1000 AS max_cpu_time_ms,
        
        -- Plan information
        plan_generation_num,
        
        -- Row metrics
        total_rows,
        CAST(total_rows / CAST(execution_count AS DECIMAL(18,2)) AS DECIMAL(18,2)) AS avg_rows_returned,
        
        -- Parallelism metrics
        total_dop,
        CAST(total_dop / CAST(execution_count AS DECIMAL(18,2)) AS DECIMAL(18,2)) AS avg_dop,
        
        -- Memory grant metrics
        ISNULL(total_grant_kb, 0) AS total_grant_kb,
        CAST(ISNULL(total_grant_kb, 0) / CAST(execution_count AS DECIMAL(18,2)) AS DECIMAL(18,2)) AS avg_grant_kb,
        ISNULL(total_used_grant_kb, 0) AS total_used_grant_kb,
        CAST(ISNULL(total_used_grant_kb, 0) / CAST(execution_count AS DECIMAL(18,2)) AS DECIMAL(18,2)) AS avg_used_grant_kb,
        ISNULL(total_ideal_grant_kb, 0) AS total_ideal_grant_kb,
        CAST(ISNULL(total_ideal_grant_kb, 0) / CAST(execution_count AS DECIMAL(18,2)) AS DECIMAL(18,2)) AS avg_ideal_grant_kb,
        
        -- Threading metrics
        ISNULL(total_reserved_threads, 0) AS total_reserved_threads,
        ISNULL(total_used_threads, 0) AS total_used_threads,
        
        -- Wait statistics (new)
        ISNULL(total_clr_time / 1000, 0) AS total_clr_time_ms,
        CAST(ISNULL(total_clr_time / 1000, 0) / CAST(execution_count AS DECIMAL(18,2)) AS DECIMAL(18,2)) AS avg_clr_time_ms,
        
        -- Spill metrics (new)
        ISNULL(total_spills, 0) AS total_spills,
        CAST(ISNULL(total_spills, 0) / CAST(execution_count AS DECIMAL(18,2)) AS DECIMAL(18,2)) AS avg_spills,
        
        -- Performance ratios (calculated)
        CASE 
            WHEN total_logical_reads > 0 
            THEN CAST((total_physical_reads * 100.0) / total_logical_reads AS DECIMAL(5,2))
            ELSE 0 
        END AS buffer_hit_ratio,
        
        CASE 
            WHEN total_elapsed_time > 0 
            THEN CAST((total_worker_time * 100.0) / total_elapsed_time AS DECIMAL(5,2))
            ELSE 0 
        END AS cpu_efficiency_ratio,
        
        -- Collection timestamp
        GETDATE() AS collection_timestamp,
        
        -- Query hash for grouping similar queries
        qs.query_hash,
        qs.query_plan_hash
        
    FROM sys.dm_exec_query_stats AS qs
    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
    OUTER APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
    WHERE st.text NOT LIKE '%sys.dm_exec_query_stats%'
      AND st.text NOT LIKE '%SELECT @@VERSION%'
      AND st.text NOT LIKE '%PERFORMANCE_QUERY%'
      AND st.text NOT LIKE '%sp_reset_connection%'
      AND st.text NOT LIKE '%BACKUP%'
      AND st.text NOT LIKE '%RESTORE%'
      AND st.text NOT LIKE '%CHECKPOINT%'
      AND st.text NOT LIKE '%DBCC%'
      AND execution_count > 0
      AND total_elapsed_time > 0
      AND LEN(LTRIM(RTRIM(st.text))) > 10
      -- Filter for problematic queries only:
      AND (
          -- High average execution time (> 1000ms)
          (total_elapsed_time / 1000.0) / execution_count > 1000
          -- OR high total execution time (> 30 seconds total)
          OR total_elapsed_time / 1000 > 30000
          -- OR high logical reads per execution (> 10000)
          OR total_logical_reads / CAST(execution_count AS DECIMAL(18,2)) > 10000
          -- OR high physical reads per execution (> 1000)
          OR total_physical_reads / CAST(execution_count AS DECIMAL(18,2)) > 1000
          -- OR queries with spills (indicates memory pressure)
          OR ISNULL(total_spills, 0) > 0
          -- OR queries with high CPU usage (> 5000ms average)
          OR (total_worker_time / 1000.0) / execution_count > 5000
      )
    ORDER BY 
        -- Prioritize by multiple criteria for worst queries
        (total_elapsed_time / 1000.0) / execution_count DESC,
        total_elapsed_time DESC;
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
            self.logger.warning("Collector will be started in limited mode")
            # Allow starting even with connection issues
            self._is_started = True

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
            # Return empty data set instead of None to avoid null reference issues
            return RawPerformanceData([])

    def stop(self):
        """Stop the collector and cleanup resources."""
        self._is_started = False
        self.logger.info("Performance collector stopped")

    def is_started(self) -> bool:
        """Check if the collector is started."""
        return self._is_started