import sqlite3
import json
import logging
from datetime import datetime
from typing import List, Optional, Callable
from pathlib import Path

from src.utils.data_compressor import compress_data, decompress_data
from ..common.models import CustomMetrics, RawPerformanceData

# Resume of the methods:
# - `__init__`: Initializes the SQLite repository and creates necessary tables.
# - `_initialize_database`: Creates the database tables if they don't exist.
# - `add_observer`: Adds an observer to be notified when new data is saved.
# - `remove_observer`: Removes an observer.
# - `_notify_observers`: Notifies all observers about new data.
# - `save_metrics`: Saves custom metrics to the database and notifies observers.
# - `get_all_metrics`: Retrieves all custom metrics from the database.
# - `get_metrics_by_timerange`: Retrieves custom metrics within a specific time range.
# - `get_latest_metrics`: Retrieves the most recent custom metrics.
# - `clear_all_metrics`: Clears all metrics from the database.

class SQLiteRepository:
    """SQLite repository for storing custom metrics and performance snapshots."""
    
    def __init__(self, db_path: str = "atlas_data.db"):
        """Initialize the SQLite repository.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = Path(db_path)
        self.logger = logging.getLogger(__name__)
        self._observers: List[Callable[[List[CustomMetrics]], None]] = []
        self._initialize_database()
    
    def _initialize_database(self) -> None:
        """Create the database tables if they don't exist."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create the custom_metrics table with all fields from PerformanceDataDict
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS custom_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        total_elapsed_time_ms INTEGER NOT NULL,
                        total_cpu_time_ms INTEGER NOT NULL,
                        total_logical_reads INTEGER NOT NULL,
                        total_physical_reads INTEGER NOT NULL,
                        total_logical_writes INTEGER NOT NULL,
                        execution_count INTEGER NOT NULL,
                        avg_elapsed_time_ms REAL NOT NULL,
                        avg_cpu_time_ms REAL NOT NULL,
                        avg_logical_reads REAL NOT NULL,
                        avg_physical_reads REAL NOT NULL,
                        avg_logical_writes REAL NOT NULL,
                        creation_time TEXT NOT NULL,
                        last_execution_time TEXT NOT NULL,
                        query_text BLOB NOT NULL,
                        query_plan BLOB,
                        min_elapsed_time_ms INTEGER NOT NULL,
                        max_elapsed_time_ms INTEGER NOT NULL,
                        min_cpu_time_ms INTEGER NOT NULL,
                        max_cpu_time_ms INTEGER NOT NULL,
                        plan_generation_num INTEGER NOT NULL,
                        total_rows INTEGER NOT NULL,
                        avg_rows_returned REAL NOT NULL,
                        total_dop INTEGER NOT NULL,
                        avg_dop REAL NOT NULL,
                        total_grant_kb INTEGER NOT NULL,
                        avg_grant_kb REAL NOT NULL,
                        total_used_grant_kb INTEGER NOT NULL,
                        avg_used_grant_kb REAL NOT NULL,
                        total_ideal_grant_kb INTEGER NOT NULL,
                        avg_ideal_grant_kb REAL NOT NULL,
                        total_reserved_threads INTEGER NOT NULL,
                        total_used_threads INTEGER NOT NULL,
                        total_clr_time_ms INTEGER NOT NULL,
                        avg_clr_time_ms REAL NOT NULL,
                        total_spills INTEGER NOT NULL,
                        avg_spills REAL NOT NULL,
                        buffer_hit_ratio REAL NOT NULL,
                        cpu_efficiency_ratio REAL NOT NULL,
                        query_hash TEXT NOT NULL,
                        query_plan_hash TEXT NOT NULL,
                        collection_timestamp TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Create index for faster queries
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_timestamp 
                    ON custom_metrics(timestamp)
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_created_at 
                    ON custom_metrics(created_at)
                """)
                
                conn.commit()
                self.logger.info(f"Database initialized at {self.db_path}")
                
        except sqlite3.Error as e:
            self.logger.error(f"Failed to initialize database: {e}")
            raise
    
    def add_observer(self, observer: Callable[[List[CustomMetrics]], None]) -> None:
        """Add an observer to be notified when new data is saved.
        
        Args:
            observer: Callback function that receives new custom metrics
        """
        if observer not in self._observers:
            self._observers.append(observer)
            self.logger.info("Observer added to SQLite repository")
    
    def remove_observer(self, observer: Callable[[List[CustomMetrics]], None]) -> None:
        """Remove an observer.
        
        Args:
            observer: Callback function to remove
        """
        if observer in self._observers:
            self._observers.remove(observer)
            self.logger.info("Observer removed from SQLite repository")
    
    def _notify_observers(self, new_metrics: List[CustomMetrics]) -> None:
        """Notify all observers about new data.
        
        Args:
            new_metrics: List of new custom metrics that were saved
        """
        for observer in self._observers:
            try:
                observer(new_metrics)
            except Exception as e:
                self.logger.error(f"Error notifying observer: {e}")
    
    def save_metrics(self, metrics: List[CustomMetrics]) -> bool:
        """Save custom metrics to the database and notify observers.
        
        Args:
            metrics: List of custom metrics to save
            
        Returns:
            True if successful, False otherwise
        """
        if not metrics:
            self.logger.warning("No metrics to save")
            return False
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                for metric in metrics:
                    # Extract data from the CustomMetrics
                    raw_data = metric['value']  # This is now a dict containing a single performance data item
                    timestamp = metric['timestamp']
                    
                    # Process the raw_data item directly
                    item = raw_data  # raw_data is already the item dictionary
                    cursor.execute("""
                            INSERT INTO custom_metrics (
                                timestamp, total_elapsed_time_ms, total_cpu_time_ms,
                                total_logical_reads, total_physical_reads, total_logical_writes,
                                execution_count, avg_elapsed_time_ms, avg_cpu_time_ms,
                                avg_logical_reads, avg_physical_reads, avg_logical_writes,
                                creation_time, last_execution_time, query_text, query_plan,
                                min_elapsed_time_ms, max_elapsed_time_ms, min_cpu_time_ms,
                                max_cpu_time_ms, plan_generation_num, total_rows,
                                avg_rows_returned, total_dop, avg_dop, total_grant_kb,
                                avg_grant_kb, total_used_grant_kb, avg_used_grant_kb,
                                total_ideal_grant_kb, avg_ideal_grant_kb, total_reserved_threads,
                                total_used_threads, total_clr_time_ms, avg_clr_time_ms,
                                total_spills, avg_spills, buffer_hit_ratio, cpu_efficiency_ratio,
                                query_hash, query_plan_hash, collection_timestamp
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                                      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            timestamp.isoformat(),
                            item['total_elapsed_time_ms'],
                            item['total_cpu_time_ms'],
                            item['total_logical_reads'],
                            item['total_physical_reads'],
                            item['total_logical_writes'],
                            item['execution_count'],
                            float(item['avg_elapsed_time_ms']),
                            float(item['avg_cpu_time_ms']),
                            float(item['avg_logical_reads']),
                            float(item['avg_physical_reads']),
                            float(item['avg_logical_writes']),
                            item['creation_time'].isoformat(),
                            item['last_execution_time'].isoformat(),
                            compress_data(str(item['query_text']).encode('utf-8')),  
                            compress_data(str(item.get('query_plan', '')).encode('utf-8')),  
                            item['min_elapsed_time_ms'],
                            item['max_elapsed_time_ms'],
                            item['min_cpu_time_ms'],
                            item['max_cpu_time_ms'],
                            item['plan_generation_num'],
                            item['total_rows'],
                            float(item['avg_rows_returned']),
                            item['total_dop'],
                            float(item['avg_dop']),
                            item['total_grant_kb'],
                            float(item['avg_grant_kb']),
                            item['total_used_grant_kb'],
                            float(item['avg_used_grant_kb']),
                            item['total_ideal_grant_kb'],
                            float(item['avg_ideal_grant_kb']),
                            item['total_reserved_threads'],
                            item['total_used_threads'],
                            item['total_clr_time_ms'],
                            float(item['avg_clr_time_ms']),
                            item['total_spills'],
                            float(item['avg_spills']),
                            float(item['buffer_hit_ratio']),
                            float(item['cpu_efficiency_ratio']),
                            item['query_hash'],
                            item['query_plan_hash'],
                            item['collection_timestamp'].isoformat()
                        ))
                
                conn.commit()
                self.logger.info(f"Saved {len(metrics)} custom metrics to database")
                
                # Notify observers about new data
                self._notify_observers(metrics)
                
                return True
                
        except sqlite3.Error as e:
            self.logger.error(f"Failed to save metrics: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error saving metrics: {e}")
            return False
    
    def _decompress_text_field(self, data) -> str:
        """Safely decompress text field data, handling both bytes and string types.
        
        Args:
            data: The data to decompress (bytes or str)
            
        Returns:
            Decompressed string
        """
        if data is None:
            return ''
        
        if isinstance(data, bytes):
            try:
                return decompress_data(data).decode('utf-8')
            except Exception:
                # If decompression fails, assume it's already a string encoded as bytes
                return data.decode('utf-8', errors='ignore')
        elif isinstance(data, str):
            return data
        else:
            return str(data)

    def get_all_metrics(self) -> List[CustomMetrics]:
        """Retrieve all custom metrics from the database.
        
        Returns:
            List of all custom metrics stored in the database
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT 
                        timestamp, total_elapsed_time_ms, total_cpu_time_ms,
                        total_logical_reads, total_physical_reads, total_logical_writes,
                        execution_count, avg_elapsed_time_ms, avg_cpu_time_ms,
                        avg_logical_reads, avg_physical_reads, avg_logical_writes,
                        creation_time, last_execution_time, query_text, query_plan,
                        min_elapsed_time_ms, max_elapsed_time_ms, min_cpu_time_ms,
                        max_cpu_time_ms, plan_generation_num, total_rows,
                        avg_rows_returned, total_dop, avg_dop, total_grant_kb,
                        avg_grant_kb, total_used_grant_kb, avg_used_grant_kb,
                        total_ideal_grant_kb, avg_ideal_grant_kb, total_reserved_threads,
                        total_used_threads, total_clr_time_ms, avg_clr_time_ms,
                        total_spills, avg_spills, buffer_hit_ratio, cpu_efficiency_ratio,
                        query_hash, query_plan_hash, collection_timestamp
                    FROM custom_metrics 
                    ORDER BY created_at ASC
                """)
                
                rows = cursor.fetchall()
                metrics = []
                
                for row in rows:
                    # Convert row back to CustomMetrics format with all fields
                    performance_data = {
                        'total_elapsed_time_ms': row[1],
                        'total_cpu_time_ms': row[2],
                        'total_logical_reads': row[3],
                        'total_physical_reads': row[4],
                        'total_logical_writes': row[5],
                        'execution_count': row[6],
                        'avg_elapsed_time_ms': row[7],
                        'avg_cpu_time_ms': row[8],
                        'avg_logical_reads': row[9],
                        'avg_physical_reads': row[10],
                        'avg_logical_writes': row[11],
                        'creation_time': datetime.fromisoformat(row[12]),
                        'last_execution_time': datetime.fromisoformat(row[13]),
                        'query_text': self._decompress_text_field(row[14]),
                        'query_plan': self._decompress_text_field(row[15]),
                        'min_elapsed_time_ms': row[16],
                        'max_elapsed_time_ms': row[17],
                        'min_cpu_time_ms': row[18],
                        'max_cpu_time_ms': row[19],
                        'plan_generation_num': row[20],
                        'total_rows': row[21],
                        'avg_rows_returned': row[22],
                        'total_dop': row[23],
                        'avg_dop': row[24],
                        'total_grant_kb': row[25],
                        'avg_grant_kb': row[26],
                        'total_used_grant_kb': row[27],
                        'avg_used_grant_kb': row[28],
                        'total_ideal_grant_kb': row[29],
                        'avg_ideal_grant_kb': row[30],
                        'total_reserved_threads': row[31],
                        'total_used_threads': row[32],
                        'total_clr_time_ms': row[33],
                        'avg_clr_time_ms': row[34],
                        'total_spills': row[35],
                        'avg_spills': row[36],
                        'buffer_hit_ratio': row[37],
                        'cpu_efficiency_ratio': row[38],
                        'query_hash': row[39],
                        'query_plan_hash': row[40],
                        'collection_timestamp': datetime.fromisoformat(row[41])
                    }
                    
                    # Create CustomMetrics as a dictionary rather than using class constructor
                    metric = {
                        'value': performance_data,  # Direct dict instead of RawPerformanceData
                        'timestamp': datetime.fromisoformat(row[0])
                    }
                    metrics.append(metric)
                
                self.logger.info(f"Retrieved {len(metrics)} metrics from database")
                return metrics
                
        except sqlite3.Error as e:
            self.logger.error(f"Failed to retrieve metrics: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error retrieving metrics: {e}")
            return []
    
    def get_metrics_by_timerange(
        self, 
        start_time: datetime, 
        end_time: datetime
    ) -> List[CustomMetrics]:
        """Retrieve custom metrics within a specific time range.
        
        Args:
            start_time: Start of the time range
            end_time: End of the time range
            
        Returns:
            List of custom metrics within the specified time range
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT 
                        timestamp, total_elapsed_time_ms, total_cpu_time_ms,
                        total_logical_reads, total_physical_reads, total_logical_writes,
                        execution_count, avg_elapsed_time_ms, avg_cpu_time_ms,
                        avg_logical_reads, avg_physical_reads, avg_logical_writes,
                        creation_time, last_execution_time, query_text, query_plan,
                        min_elapsed_time_ms, max_elapsed_time_ms, min_cpu_time_ms,
                        max_cpu_time_ms, plan_generation_num, total_rows,
                        avg_rows_returned, total_dop, avg_dop, total_grant_kb,
                        avg_grant_kb, total_used_grant_kb, avg_used_grant_kb,
                        total_ideal_grant_kb, avg_ideal_grant_kb, total_reserved_threads,
                        total_used_threads, total_clr_time_ms, avg_clr_time_ms,
                        total_spills, avg_spills, buffer_hit_ratio, cpu_efficiency_ratio,
                        query_hash, query_plan_hash, collection_timestamp
                    FROM custom_metrics 
                    WHERE timestamp BETWEEN ? AND ?
                    ORDER BY timestamp ASC
                """, (start_time.isoformat(), end_time.isoformat()))
                
                rows = cursor.fetchall()
                metrics = []
                
                for row in rows:
                    performance_data = {
                        'total_elapsed_time_ms': row[1],
                        'total_cpu_time_ms': row[2],
                        'total_logical_reads': row[3],
                        'total_physical_reads': row[4],
                        'total_logical_writes': row[5],
                        'execution_count': row[6],
                        'avg_elapsed_time_ms': row[7],
                        'avg_cpu_time_ms': row[8],
                        'avg_logical_reads': row[9],
                        'avg_physical_reads': row[10],
                        'avg_logical_writes': row[11],
                        'creation_time': datetime.fromisoformat(row[12]),
                        'last_execution_time': datetime.fromisoformat(row[13]),
                        'query_text': self._decompress_text_field(row[14]),
                        'query_plan': self._decompress_text_field(row[15]),
                        'min_elapsed_time_ms': row[16],
                        'max_elapsed_time_ms': row[17],
                        'min_cpu_time_ms': row[18],
                        'max_cpu_time_ms': row[19],
                        'plan_generation_num': row[20],
                        'total_rows': row[21],
                        'avg_rows_returned': row[22],
                        'total_dop': row[23],
                        'avg_dop': row[24],
                        'total_grant_kb': row[25],
                        'avg_grant_kb': row[26],
                        'total_used_grant_kb': row[27],
                        'avg_used_grant_kb': row[28],
                        'total_ideal_grant_kb': row[29],
                        'avg_ideal_grant_kb': row[30],
                        'total_reserved_threads': row[31],
                        'total_used_threads': row[32],
                        'total_clr_time_ms': row[33],
                        'avg_clr_time_ms': row[34],
                        'total_spills': row[35],
                        'avg_spills': row[36],
                        'buffer_hit_ratio': row[37],
                        'cpu_efficiency_ratio': row[38],
                        'query_hash': row[39],
                        'query_plan_hash': row[40],
                        'collection_timestamp': datetime.fromisoformat(row[41])
                    }
                    
                    # Create CustomMetrics as a dictionary rather than using class constructor
                    metric = {
                        'value': performance_data,  # Direct dict instead of RawPerformanceData
                        'timestamp': datetime.fromisoformat(row[0])
                    }
                    metrics.append(metric)
                
                self.logger.info(
                    f"Retrieved {len(metrics)} metrics from {start_time} to {end_time}"
                )
                return metrics
                
        except sqlite3.Error as e:
            self.logger.error(f"Failed to retrieve metrics by time range: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error retrieving metrics by time range: {e}")
            return []
    
    def get_latest_metrics(self, limit: int = 10) -> List[CustomMetrics]:
        """Retrieve the most recent custom metrics.
        
        Args:
            limit: Maximum number of metrics to retrieve
            
        Returns:
            List of the most recent custom metrics
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT 
                        timestamp, total_elapsed_time_ms, total_cpu_time_ms,
                        total_logical_reads, total_physical_reads, total_logical_writes,
                        execution_count, avg_elapsed_time_ms, avg_cpu_time_ms,
                        avg_logical_reads, avg_physical_reads, avg_logical_writes,
                        creation_time, last_execution_time, query_text, query_plan,
                        min_elapsed_time_ms, max_elapsed_time_ms, min_cpu_time_ms,
                        max_cpu_time_ms, plan_generation_num, total_rows,
                        avg_rows_returned, total_dop, avg_dop, total_grant_kb,
                        avg_grant_kb, total_used_grant_kb, avg_used_grant_kb,
                        total_ideal_grant_kb, avg_ideal_grant_kb, total_reserved_threads,
                        total_used_threads, total_clr_time_ms, avg_clr_time_ms,
                        total_spills, avg_spills, buffer_hit_ratio, cpu_efficiency_ratio,
                        query_hash, query_plan_hash, collection_timestamp
                    FROM custom_metrics 
                    ORDER BY created_at DESC
                    LIMIT ?
                """, (limit,))
                
                rows = cursor.fetchall()
                metrics = []
                
                for row in rows:
                    performance_data = {
                        'total_elapsed_time_ms': row[1],
                        'total_cpu_time_ms': row[2],
                        'total_logical_reads': row[3],
                        'total_physical_reads': row[4],
                        'total_logical_writes': row[5],
                        'execution_count': row[6],
                        'avg_elapsed_time_ms': row[7],
                        'avg_cpu_time_ms': row[8],
                        'avg_logical_reads': row[9],
                        'avg_physical_reads': row[10],
                        'avg_logical_writes': row[11],
                        'creation_time': datetime.fromisoformat(row[12]),
                        'last_execution_time': datetime.fromisoformat(row[13]),
                        'query_text': self._decompress_text_field(row[14]),
                        'query_plan': self._decompress_text_field(row[15]),
                        'min_elapsed_time_ms': row[16],
                        'max_elapsed_time_ms': row[17],
                        'min_cpu_time_ms': row[18],
                        'max_cpu_time_ms': row[19],
                        'plan_generation_num': row[20],
                        'total_rows': row[21],
                        'avg_rows_returned': row[22],
                        'total_dop': row[23],
                        'avg_dop': row[24],
                        'total_grant_kb': row[25],
                        'avg_grant_kb': row[26],
                        'total_used_grant_kb': row[27],
                        'avg_used_grant_kb': row[28],
                        'total_ideal_grant_kb': row[29],
                        'avg_ideal_grant_kb': row[30],
                        'total_reserved_threads': row[31],
                        'total_used_threads': row[32],
                        'total_clr_time_ms': row[33],
                        'avg_clr_time_ms': row[34],
                        'total_spills': row[35],
                        'avg_spills': row[36],
                        'buffer_hit_ratio': row[37],
                        'cpu_efficiency_ratio': row[38],
                        'query_hash': row[39],
                        'query_plan_hash': row[40],
                        'collection_timestamp': datetime.fromisoformat(row[41])
                    }
                    
                    # Create CustomMetrics as a dictionary rather than using class constructor
                    metric = {
                        'value': performance_data,  # Direct dict instead of RawPerformanceData
                        'timestamp': datetime.fromisoformat(row[0])
                    }
                    metrics.append(metric)
            
            # Reverse to get chronological order
            metrics.reverse()
            
            self.logger.info(f"Retrieved {len(metrics)} latest metrics")
            return metrics
            
        except sqlite3.Error as e:
            self.logger.error(f"Failed to retrieve latest metrics: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error retrieving latest metrics: {e}")
            return []
    
    def clear_all_metrics(self) -> bool:
        """Clear all metrics from the database.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM custom_metrics")
                conn.commit()
                
                self.logger.info("All metrics cleared from database")
                return True
                
        except sqlite3.Error as e:
            self.logger.error(f"Failed to clear metrics: {e}")
            return False
    
    def get_metrics_count(self) -> int:
        """Get the total number of metrics in the database.
        
        Returns:
            Total count of metrics
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM custom_metrics")
                count = cursor.fetchone()[0]
                
                return count
                
        except sqlite3.Error as e:
            self.logger.error(f"Failed to get metrics count: {e}")
            return 0
    
    def close(self) -> None:
        """Close the repository and cleanup resources."""
        self._observers.clear()
        self.logger.info("SQLite repository closed")
    
    def cleanup_old_metrics(self, days_to_keep: int = 30) -> bool:
        """Remove metrics older than specified days to control database size.
        
        Args:
            days_to_keep: Number of days of metrics to retain
            
        Returns:
            True if successful, False otherwise
        """
        try:
            from datetime import datetime, timedelta
            
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Count records to be deleted
                cursor.execute(
                    "SELECT COUNT(*) FROM custom_metrics WHERE timestamp < ?",
                    (cutoff_date.isoformat(),)
                )
                count_to_delete = cursor.fetchone()[0]
                
                if count_to_delete > 0:
                    # Delete old records
                    cursor.execute(
                        "DELETE FROM custom_metrics WHERE timestamp < ?",
                        (cutoff_date.isoformat(),)
                    )
                    conn.commit()
                    
                    # Vacuum to reclaim space
                    cursor.execute("VACUUM")
                    
                    self.logger.info(f"Cleaned up {count_to_delete} old metrics (older than {days_to_keep} days)")
                else:
                    self.logger.info("No old metrics to clean up")
                
                return True
                
        except sqlite3.Error as e:
            self.logger.error(f"Failed to cleanup old metrics: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error during cleanup: {e}")
            return False

    def get_database_size_info(self) -> dict:
        """Get information about the database size and record counts.
        
        Returns:
            Dictionary with size information
        """
        try:
            import os
            
            db_size_bytes = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
            db_size_mb = db_size_bytes / (1024 * 1024)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get total record count
                cursor.execute("SELECT COUNT(*) FROM custom_metrics")
                total_records = cursor.fetchone()[0]
                
                # Get date range
                cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM custom_metrics")
                date_range = cursor.fetchone()
                
                # Get records per day (approximate)
                if date_range[0] and date_range[1]:
                    from datetime import datetime
                    start_date = datetime.fromisoformat(date_range[0])
                    end_date = datetime.fromisoformat(date_range[1])
                    days_span = (end_date - start_date).days + 1
                    records_per_day = total_records / max(days_span, 1)
                else:
                    records_per_day = 0
                
                return {
                    'database_size_bytes': db_size_bytes,
                    'database_size_mb': round(db_size_mb, 2),
                    'total_records': total_records,
                    'oldest_record': date_range[0],
                    'newest_record': date_range[1],
                    'estimated_records_per_day': round(records_per_day, 2)
                }
                
        except Exception as e:
            self.logger.error(f"Failed to get database size info: {e}")
            return {
                'database_size_bytes': 0,
                'database_size_mb': 0,
                'total_records': 0,
                'oldest_record': None,
                'newest_record': None,
                'estimated_records_per_day': 0
            }

    def remove_duplicate_queries(self) -> bool:
        """Remove duplicate queries based on query_hash to reduce database size.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Count duplicates before removal
                cursor.execute("""
                    SELECT COUNT(*) - COUNT(DISTINCT query_hash) as duplicates
                    FROM custom_metrics 
                    WHERE query_hash IS NOT NULL
                """)
                duplicate_count = cursor.fetchone()[0]
                
                if duplicate_count > 0:
                    # Keep only the most recent record for each query_hash
                    cursor.execute("""
                        DELETE FROM custom_metrics 
                        WHERE id NOT IN (
                            SELECT MAX(id) 
                            FROM custom_metrics 
                            WHERE query_hash IS NOT NULL 
                            GROUP BY query_hash
                        )
                        AND query_hash IS NOT NULL
                    """)
                    
                    # Also remove records with NULL query_hash if they're duplicates by query_text
                    cursor.execute("""
                        DELETE FROM custom_metrics 
                        WHERE id NOT IN (
                            SELECT MAX(id) 
                            FROM custom_metrics 
                            WHERE query_hash IS NULL 
                            GROUP BY query_text
                        )
                        AND query_hash IS NULL
                    """)
                    
                    conn.commit()
                    
                    # Vacuum to reclaim space
                    cursor.execute("VACUUM")
                    
                    self.logger.info(f"Removed {duplicate_count} duplicate queries")
                else:
                    self.logger.info("No duplicate queries found")
                
                return True
                
        except sqlite3.Error as e:
            self.logger.error(f"Failed to remove duplicates: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error removing duplicates: {e}")
            return False