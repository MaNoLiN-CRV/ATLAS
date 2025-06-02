import sqlite3
import json
import logging
from datetime import datetime
from typing import List, Optional, Callable
from pathlib import Path
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
                
                # Create the custom_metrics table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS custom_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        total_elapsed_time_ms INTEGER NOT NULL,
                        total_cpu_time_ms INTEGER NOT NULL,
                        total_logical_reads INTEGER NOT NULL,
                        total_physical_reads INTEGER NOT NULL,
                        execution_count INTEGER NOT NULL,
                        avg_elapsed_time_ms REAL NOT NULL,
                        avg_cpu_time_ms REAL NOT NULL,
                        creation_time TEXT NOT NULL,
                        last_execution_time TEXT NOT NULL,
                        query_text TEXT NOT NULL,
                        query_plan TEXT NOT NULL,
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
                    raw_data = metric['value']
                    timestamp = metric['timestamp']
                    
                    # Get the first item from raw_data (assuming single item per metric)
                    data_items = raw_data.get_data()
                    if not data_items:
                        continue
                        
                    for item in data_items:
                        cursor.execute("""
                            INSERT INTO custom_metrics (
                                timestamp, total_elapsed_time_ms, total_cpu_time_ms,
                                total_logical_reads, total_physical_reads, execution_count,
                                avg_elapsed_time_ms, avg_cpu_time_ms, creation_time,
                                last_execution_time, query_text, query_plan
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            timestamp.isoformat(),
                            item['total_elapsed_time_ms'],
                            item['total_cpu_time_ms'],
                            item['total_logical_reads'],
                            item['total_physical_reads'],
                            item['execution_count'],
                            float(item['avg_elapsed_time_ms']),
                            float(item['avg_cpu_time_ms']),
                            item['creation_time'].isoformat(),
                            item['last_execution_time'].isoformat(),
                            item['query_text'],
                            item['query_plan']
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
                        total_logical_reads, total_physical_reads, execution_count,
                        avg_elapsed_time_ms, avg_cpu_time_ms, creation_time,
                        last_execution_time, query_text, query_plan
                    FROM custom_metrics 
                    ORDER BY created_at ASC
                """)
                
                rows = cursor.fetchall()
                metrics = []
                
                for row in rows:
                    # Convert row back to CustomMetrics format
                    performance_data = {
                        'total_elapsed_time_ms': row[1],
                        'total_cpu_time_ms': row[2],
                        'total_logical_reads': row[3],
                        'total_physical_reads': row[4],
                        'execution_count': row[5],
                        'avg_elapsed_time_ms': row[6],
                        'avg_cpu_time_ms': row[7],
                        'creation_time': datetime.fromisoformat(row[8]),
                        'last_execution_time': datetime.fromisoformat(row[9]),
                        'query_text': row[10],
                        'query_plan': row[11]
                    }
                    
                    metric = CustomMetrics(
                        value=RawPerformanceData([performance_data]),
                        timestamp=datetime.fromisoformat(row[0])
                    )
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
                        total_logical_reads, total_physical_reads, execution_count,
                        avg_elapsed_time_ms, avg_cpu_time_ms, creation_time,
                        last_execution_time, query_text, query_plan
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
                        'execution_count': row[5],
                        'avg_elapsed_time_ms': row[6],
                        'avg_cpu_time_ms': row[7],
                        'creation_time': datetime.fromisoformat(row[8]),
                        'last_execution_time': datetime.fromisoformat(row[9]),
                        'query_text': row[10],
                        'query_plan': row[11]
                    }
                    
                    metric = CustomMetrics(
                        value=RawPerformanceData([performance_data]),
                        timestamp=datetime.fromisoformat(row[0])
                    )
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
                        total_logical_reads, total_physical_reads, execution_count,
                        avg_elapsed_time_ms, avg_cpu_time_ms, creation_time,
                        last_execution_time, query_text, query_plan
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
                        'execution_count': row[5],
                        'avg_elapsed_time_ms': row[6],
                        'avg_cpu_time_ms': row[7],
                        'creation_time': datetime.fromisoformat(row[8]),
                        'last_execution_time': datetime.fromisoformat(row[9]),
                        'query_text': row[10],
                        'query_plan': row[11]
                    }
                    
                    metric = CustomMetrics(
                        value=RawPerformanceData([performance_data]),
                        timestamp=datetime.fromisoformat(row[0])
                    )
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