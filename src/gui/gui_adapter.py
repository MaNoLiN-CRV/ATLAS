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
    
    def load_initial_data(self, data: List[CustomMetrics]) -> None:
        """Load initial data for GUI display."""
        self.data_cache = []
        for metric in data:
            if hasattr(metric, 'value') and isinstance(metric.value, RawPerformanceData):
                for record in metric.value.get_data():
                    record_dict = dict(record)
                    record_dict['metric_timestamp'] = metric.timestamp
                    self.data_cache.append(record_dict)
        
        self.last_update = datetime.now()
        self._notify_subscribers()
    
    def add_new_data(self, new_data: List[CustomMetrics]) -> None:
        """Add new data to existing cache without full reload."""
        for metric in new_data:
            if hasattr(metric, 'value') and isinstance(metric.value, RawPerformanceData):
                for record in metric.value.get_data():
                    record_dict = dict(record)
                    record_dict['metric_timestamp'] = metric.timestamp
                    self.data_cache.append(record_dict)
        
        self.last_update = datetime.now()
        self._notify_subscribers()
    
    def get_dataframe(self) -> pd.DataFrame:
        """Convert cached data to pandas DataFrame for visualization."""
        if not self.data_cache:
            return pd.DataFrame()
        
        df = pd.DataFrame(self.data_cache)
        
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
        self.data_cache = []
        self.last_update = None
        self._notify_subscribers()
    
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