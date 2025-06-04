import sys
import psutil
import os
import threading
import time
from typing import List, Dict, Any, Callable, Optional
import pandas as pd
from datetime import datetime
import streamlit as st
from ..common.models import CustomMetrics, RawPerformanceData


class GUIAdapter:
    """Adapter class that interfaces between the core system and the GUI."""
    
    def __init__(self):
        self.data_cache: List[Dict[str, Any]] = []
        self.last_update: Optional[datetime] = None
        self.update_callbacks: List[Callable] = []
        self._memory_monitoring = False
        self._memory_history: List[Dict[str, Any]] = []
        
        # Initialize memory monitoring
        self.print_memory_usage("GUI Adapter initialized")
    
    def print_memory_usage(self, context: str = "") -> None:
        """Print current memory usage of the application."""
        try:
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            
            # Memory in MB
            rss_mb = memory_info.rss / 1024 / 1024
            vms_mb = memory_info.vms / 1024 / 1024
            
            # Cache size estimation
            cache_size_mb = sys.getsizeof(self.data_cache) / 1024 / 1024
            cache_records = len(self.data_cache)
            
            print(f"=== MEMORY USAGE ({context}) ===")
            print(f"RSS (Physical Memory): {rss_mb:.2f} MB")
            print(f"VMS (Virtual Memory): {vms_mb:.2f} MB")
            print(f"Cache Records: {cache_records}")
            print(f"Cache Size (estimated): {cache_size_mb:.2f} MB")
            print(f"Memory per record: {(cache_size_mb / cache_records):.4f} MB" if cache_records > 0 else "N/A")
            print("=" * 40)
        except Exception as e:
            print(f"Error getting memory usage: {e}")
    
    def get_memory_stats(self) -> Dict[str, Any]:
        """Get detailed memory statistics."""
        try:
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            
            # Calculate cache memory usage more accurately
            cache_memory = 0
            for record in self.data_cache:
                cache_memory += sys.getsizeof(record)
                for key, value in record.items():
                    cache_memory += sys.getsizeof(key) + sys.getsizeof(value)
            
            stats = {
                'rss_mb': memory_info.rss / 1024 / 1024,
                'vms_mb': memory_info.vms / 1024 / 1024,
                'cache_records': len(self.data_cache),
                'cache_memory_mb': cache_memory / 1024 / 1024,
                'avg_record_size_kb': (cache_memory / len(self.data_cache) / 1024) if self.data_cache else 0,
                'memory_percent': process.memory_percent(),
                'timestamp': datetime.now()
            }
            
            # Store in history for trending
            self._memory_history.append(stats)
            # Keep only last 100 measurements
            if len(self._memory_history) > 100:
                self._memory_history = self._memory_history[-100:]
            
            return stats
        except Exception as e:
            print(f"Error getting memory stats: {e}")
            return {}
    
    def get_memory_history(self) -> pd.DataFrame:
        """Get memory usage history as DataFrame."""
        if not self._memory_history:
            return pd.DataFrame()
        
        df = pd.DataFrame(self._memory_history)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    
    def start_memory_monitoring(self, interval_seconds: int = 30) -> None:
        """Start continuous memory monitoring in background thread."""
        if self._memory_monitoring:
            return
        
        def monitor():
            while self._memory_monitoring:
                self.get_memory_stats()  # This updates the history
                time.sleep(interval_seconds)
        
        self._memory_monitoring = True
        monitor_thread = threading.Thread(target=monitor, daemon=True)
        monitor_thread.start()
        print(f"Memory monitoring started (interval: {interval_seconds}s)")
    
    def stop_memory_monitoring(self) -> None:
        """Stop memory monitoring."""
        self._memory_monitoring = False
        print("Memory monitoring stopped")
    
    def optimize_memory(self) -> Dict[str, Any]:
        """Attempt to optimize memory usage."""
        import gc
        
        before_stats = self.get_memory_stats()
        
        # Force garbage collection
        gc.collect()
        
        # Remove duplicates if any (optional)
        seen_hashes = set()
        unique_cache = []
        for record in self.data_cache:
            query_hash = record.get('query_hash')
            if query_hash and query_hash not in seen_hashes:
                seen_hashes.add(query_hash)
                unique_cache.append(record)
            elif not query_hash:
                unique_cache.append(record)
        
        removed_count = len(self.data_cache) - len(unique_cache)
        if removed_count > 0:
            self.data_cache = unique_cache
        
        after_stats = self.get_memory_stats()
        
        return {
            'removed_duplicates': removed_count,
            'memory_before_mb': before_stats.get('rss_mb', 0),
            'memory_after_mb': after_stats.get('rss_mb', 0),
            'memory_saved_mb': before_stats.get('rss_mb', 0) - after_stats.get('rss_mb', 0)
        }

    def load_initial_data(self, data: List[CustomMetrics]) -> None:
        """Load initial data for GUI display."""
        print(f"Loading {len(data)} initial records...")
        self.print_memory_usage("Before loading initial data")
        
        self.data_cache = []
        for metric in data:
            if isinstance(metric, dict) and 'value' in metric and 'timestamp' in metric:
                record_dict = dict(metric['value'])
                record_dict['metric_timestamp'] = metric['timestamp']
                self.data_cache.append(record_dict)
        
        self.last_update = datetime.now()
        self._notify_subscribers()
        
        self.print_memory_usage("After loading initial data")

    def add_new_data(self, new_data: List[CustomMetrics]) -> None:
        """Add new data to existing cache without full reload."""
        print(f"Adding {len(new_data)} new records...")
        self.print_memory_usage("Before adding new data")
        
        for metric in new_data:
            if isinstance(metric, dict) and 'value' in metric and 'timestamp' in metric:
                record_dict = dict(metric['value'])
                record_dict['metric_timestamp'] = metric['timestamp']
                self.data_cache.append(record_dict)
        
        self.last_update = datetime.now()
        self._notify_subscribers()
        
        self.print_memory_usage("After adding new data")

    def get_dataframe(self) -> pd.DataFrame:
        """Convert cached data to pandas DataFrame for visualization."""
        if not self.data_cache:
            return pd.DataFrame()
        
        print("Converting cache to DataFrame...")
        self.print_memory_usage("Before DataFrame conversion")
        
        df = pd.DataFrame(self.data_cache)
        
        # Calculate DataFrame memory usage
        df_memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
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
        
        self.print_memory_usage("After DataFrame conversion")
        return df
    
    def get_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics for dashboard."""
        df = self.get_dataframe()
        if df.empty:
            return {}
        
        return {
            'total_queries': len(df),
            'avg_response_time': df['avg_elapsed_time_ms'].mean() if 'avg_elapsed_time_ms' in df.columns else 0,
            'avg_cpu_time': df['avg_cpu_time_ms'].mean() if 'avg_cpu_time_ms' in df.columns else 0,
            'avg_logical_reads': df['avg_logical_reads'].mean() if 'avg_logical_reads' in df.columns else 0,
            'avg_physical_reads': df['avg_physical_reads'].mean() if 'avg_physical_reads' in df.columns else 0,
            'total_executions': df['execution_count'].sum() if 'execution_count' in df.columns else 0,
            'last_collection': self.last_update
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
        self.print_memory_usage("Before clearing data")
        self.data_cache = []
        self.last_update = None
        self._notify_subscribers()
        self.print_memory_usage("After clearing data")
    
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