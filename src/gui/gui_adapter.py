from typing import List, Dict, Any, Callable, Optional
import pandas as pd
from datetime import datetime
import streamlit as st
from ..common.models import CustomMetrics, RawPerformanceData
from ..utils.memory_monitor import MemoryMonitor


class GUIAdapter:
    """Adapter class that interfaces between the core system and the GUI."""
    
    def __init__(self):
        self.data_cache: List[Dict[str, Any]] = []
        self.last_update: Optional[datetime] = None
        self.update_callbacks: List[Callable] = []
        self.memory_monitor = MemoryMonitor()
        
        # Initialize memory monitoring
        self.memory_monitor.print_memory_usage("GUI Adapter initialized", self.data_cache)
        
        # Start memory monitoring automatically with a 30-second interval
        self.memory_monitor.start_monitoring(30, lambda: self.data_cache)
    
    
    def load_initial_data(self, data: List[CustomMetrics]) -> None:
        """Load initial data for GUI display."""
        print(f"Loading {len(data)} initial records...")
        self.memory_monitor.print_memory_usage("Before loading initial data", self.data_cache)
        
        self.data_cache = []
        for metric in data:
            if isinstance(metric, dict) and 'value' in metric and 'timestamp' in metric:
                record_dict = dict(metric['value'])
                record_dict['metric_timestamp'] = metric['timestamp']
                self.data_cache.append(record_dict)
        
        self.last_update = datetime.now()
        self._notify_subscribers()
        
        self.memory_monitor.print_memory_usage("After loading initial data", self.data_cache)

    def add_new_data(self, new_data: List[CustomMetrics]) -> None:
        """Add new data to existing cache without full reload."""
        print(f"Adding {len(new_data)} new records...")
        self.memory_monitor.print_memory_usage("Before adding new data", self.data_cache)
        
        for metric in new_data:
            if isinstance(metric, dict) and 'value' in metric and 'timestamp' in metric:
                record_dict = dict(metric['value'])
                record_dict['metric_timestamp'] = metric['timestamp']
                self.data_cache.append(record_dict)
        
        self.last_update = datetime.now()
        self._notify_subscribers()
        
        self.memory_monitor.print_memory_usage("After adding new data", self.data_cache)

    def get_dataframe(self) -> pd.DataFrame:
        """Convert cached data to pandas DataFrame for visualization."""
        if not self.data_cache:
            return pd.DataFrame()
        
        print("Converting cache to DataFrame...")
        self.memory_monitor.print_memory_usage("Before DataFrame conversion", self.data_cache)
        
        df = pd.DataFrame(self.data_cache)
        
        # Calculate DataFrame memory usage
        df_memory_mb = self.memory_monitor.calculate_dataframe_memory(df)
        print(f"DataFrame memory usage: {df_memory_mb:.2f} MB")
        
        # Ensure datetime columns are properly typed
        datetime_columns = ['creation_time', 'last_execution_time', 'collection_timestamp', 'metric_timestamp']
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        
        # Ensure numeric columns are properly typed
        numeric_columns = [
            'total_elapsed_time_ms', 'total_cpu_time_ms', 'total_logical_reads',
            'total_physical_reads', 'total_logical_writes', 'execution_count',
            'avg_elapsed_time_ms', 'avg_cpu_time_ms', 'avg_logical_reads',
            'avg_physical_reads', 'avg_logical_writes', 'min_elapsed_time_ms',
            'max_elapsed_time_ms', 'min_cpu_time_ms', 'max_cpu_time_ms',
            'plan_generation_num', 'total_rows', 'avg_rows_returned',
            'total_dop', 'avg_dop', 'total_grant_kb', 'avg_grant_kb',
            'total_used_grant_kb', 'avg_used_grant_kb', 'total_ideal_grant_kb',
            'avg_ideal_grant_kb', 'total_reserved_threads', 'total_used_threads'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        self.memory_monitor.print_memory_usage("After DataFrame conversion", self.data_cache)
        return df
    
    def get_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics for dashboard."""
        df = self.get_dataframe()
        
        # Get memory stats
        memory_stats = self.memory_monitor.get_memory_stats(self.data_cache)
        
        if df.empty:
            return {
                'total_queries': 0,
                'avg_response_time': 0,
                'avg_cpu_time': 0,
                'avg_logical_reads': 0,
                'avg_physical_reads': 0,
                'total_executions': 0,
                'last_collection': None,
                'memory_usage_mb': memory_stats.get('rss_mb', 0),
                'cache_memory_mb': memory_stats.get('cache_memory_mb', 0),
                'cache_records': memory_stats.get('cache_records', 0),
                'memory_percent': memory_stats.get('memory_percent', 0)
            }
        
        return {
            'total_queries': len(df),
            'avg_response_time': df['avg_elapsed_time_ms'].mean() if 'avg_elapsed_time_ms' in df.columns else 0,
            'avg_cpu_time': df['avg_cpu_time_ms'].mean() if 'avg_cpu_time_ms' in df.columns else 0,
            'avg_logical_reads': df['avg_logical_reads'].mean() if 'avg_logical_reads' in df.columns else 0,
            'avg_physical_reads': df['avg_physical_reads'].mean() if 'avg_physical_reads' in df.columns else 0,
            'total_executions': df['execution_count'].sum() if 'execution_count' in df.columns else 0,
            'last_collection': self.last_update,
            'memory_usage_mb': memory_stats.get('rss_mb', 0),
            'cache_memory_mb': memory_stats.get('cache_memory_mb', 0),
            'cache_records': memory_stats.get('cache_records', 0),
            'memory_percent': memory_stats.get('memory_percent', 0)
        }
    
    def subscribe_to_updates(self, callback: Callable) -> None:
        """Subscribe to data update notifications."""
        self.update_callbacks.append(callback)
    
    def _notify_subscribers(self) -> None:
        """Notify all subscribers of data updates."""
        for callback in self.update_callbacks:
            try:
                callback()
            except Exception as e:
                print(f"Error notifying subscriber: {e}")
    
    def clear_data(self) -> None:
        """Clear all cached data."""
        self.memory_monitor.print_memory_usage("Before clearing data", self.data_cache)
        self.data_cache = []
        self.last_update = None
        self._notify_subscribers()
        self.memory_monitor.print_memory_usage("After clearing data", self.data_cache)
    
    def get_top_queries_by_metric(self, metric: str, limit: int = 10) -> pd.DataFrame:
        """Get top queries by specific metric."""
        df = self.get_dataframe()
        if df.empty or metric not in df.columns:
            return pd.DataFrame()
        
        return df.nlargest(limit, metric)[['query_text', metric, 'execution_count', 'avg_elapsed_time_ms']]
    
    def get_performance_trends(self, time_window_hours: int = 24) -> pd.DataFrame:
        """Get performance trends over time."""
        df = self.get_dataframe()
        if df.empty or 'collection_timestamp' not in df.columns:
            return pd.DataFrame()
        
        # Filter by time window
        cutoff_time = datetime.now() - pd.Timedelta(hours=time_window_hours)
        df_filtered = df[df['collection_timestamp'] >= cutoff_time]
        
        if df_filtered.empty:
            return pd.DataFrame()
        
        # Group by hour and calculate averages
        df_filtered['hour'] = df_filtered['collection_timestamp'].dt.floor('H')
        trends = df_filtered.groupby('hour').agg({
            'avg_elapsed_time_ms': 'mean',
            'avg_cpu_time_ms': 'mean',
            'avg_logical_reads': 'mean',
            'avg_physical_reads': 'mean',
            'execution_count': 'sum'
        }).reset_index()
        
        return trends
    
    def get_memory_stats(self) -> Dict[str, Any]:
        """Get detailed memory statistics."""
        return self.memory_monitor.get_memory_stats(self.data_cache)
    
    def get_memory_history(self) -> pd.DataFrame:
        """Get memory usage history as DataFrame."""
        return self.memory_monitor.get_memory_history()
    
    def start_memory_monitoring(self, interval_seconds: int = 30) -> None:
        """Start continuous memory monitoring in background thread."""
        self.memory_monitor.start_monitoring(interval_seconds, lambda: self.data_cache)
    
    def stop_memory_monitoring(self) -> None:
        """Stop memory monitoring."""
        self.memory_monitor.stop_monitoring()
    
    def optimize_memory(self) -> Dict[str, Any]:
        """Attempt to optimize memory usage."""
        result = self.memory_monitor.optimize_memory(self.data_cache)
        
        # Update the cache if duplicates were removed
        if result['removed_duplicates'] > 0:
            self.data_cache = result['unique_cache']
        
        return {
            'removed_duplicates': result['removed_duplicates'],
            'memory_before_mb': result['memory_before_mb'],
            'memory_after_mb': result['memory_after_mb'],
            'memory_saved_mb': result['memory_saved_mb']
        }