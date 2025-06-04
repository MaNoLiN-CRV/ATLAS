import sys
import psutil
import os
import threading
import time
import gc
from typing import List, Dict, Any, Callable, Optional
import pandas as pd
from datetime import datetime


class MemoryMonitor:
    """Utility class for monitoring and managing memory usage."""
    
    def __init__(self):
        self._monitoring = False
        self._history: List[Dict[str, Any]] = []
        self._monitor_thread: Optional[threading.Thread] = None
        
    def print_memory_usage(self, context: str = "", cache_data: Optional[List] = None) -> None:
        """Print current memory usage of the application."""
        try:
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            
            # Memory in MB
            rss_mb = memory_info.rss / 1024 / 1024
            vms_mb = memory_info.vms / 1024 / 1024
            
            print(f"=== MEMORY USAGE ({context}) ===")
            print(f"RSS (Physical Memory): {rss_mb:.2f} MB")
            print(f"VMS (Virtual Memory): {vms_mb:.2f} MB")
            
            if cache_data is not None:
                # Cache size estimation
                cache_size_mb = sys.getsizeof(cache_data) / 1024 / 1024
                cache_records = len(cache_data)
                
                print(f"Cache Records: {cache_records}")
                print(f"Cache Size (estimated): {cache_size_mb:.2f} MB")
                print(f"Memory per record: {(cache_size_mb / cache_records):.4f} MB" if cache_records > 0 else "N/A")
            
            print("=" * 40)
        except Exception as e:
            print(f"Error getting memory usage: {e}")
    
    def get_memory_stats(self, cache_data: Optional[List] = None) -> Dict[str, Any]:
        """Get detailed memory statistics."""
        try:
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            
            stats = {
                'rss_mb': memory_info.rss / 1024 / 1024,
                'vms_mb': memory_info.vms / 1024 / 1024,
                'memory_percent': process.memory_percent(),
                'timestamp': datetime.now()
            }
            
            if cache_data is not None:
                # Calculate cache memory usage more accurately
                cache_memory = 0
                for record in cache_data:
                    cache_memory += sys.getsizeof(record)
                    if isinstance(record, dict):
                        for key, value in record.items():
                            cache_memory += sys.getsizeof(key) + sys.getsizeof(value)
                
                stats.update({
                    'cache_records': len(cache_data),
                    'cache_memory_mb': cache_memory / 1024 / 1024,
                    'avg_record_size_kb': (cache_memory / len(cache_data) / 1024) if cache_data else 0,
                })
            
            # Store in history for trending
            self._history.append(stats)
            # Keep only last 100 measurements
            if len(self._history) > 100:
                self._history = self._history[-100:]
            
            return stats
        except Exception as e:
            print(f"Error getting memory stats: {e}")
            return {}
    
    def get_memory_history(self) -> pd.DataFrame:
        """Get memory usage history as DataFrame."""
        if not self._history:
            return pd.DataFrame()
        
        df = pd.DataFrame(self._history)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    
    def start_monitoring(self, interval_seconds: int = 30, cache_data_getter: Optional[Callable] = None) -> None:
        """Start continuous memory monitoring in background thread."""
        if self._monitoring:
            return
        
        def monitor():
            while self._monitoring:
                cache_data = cache_data_getter() if cache_data_getter else None
                self.get_memory_stats(cache_data)  # This updates the history
                time.sleep(interval_seconds)
        
        self._monitoring = True
        self._monitor_thread = threading.Thread(target=monitor, daemon=True)
        self._monitor_thread.start()
        print(f"Memory monitoring started (interval: {interval_seconds}s)")
    
    def stop_monitoring(self) -> None:
        """Stop memory monitoring."""
        self._monitoring = False
        print("Memory monitoring stopped")
    
    def optimize_memory(self, cache_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Attempt to optimize memory usage for cache data."""
        before_stats = self.get_memory_stats(cache_data)
        
        # Force garbage collection
        gc.collect()
        
        # Remove duplicates if any (optional)
        seen_hashes = set()
        unique_cache = []
        for record in cache_data:
            query_hash = record.get('query_hash')
            if query_hash and query_hash not in seen_hashes:
                seen_hashes.add(query_hash)
                unique_cache.append(record)
            elif not query_hash:
                unique_cache.append(record)
        
        removed_count = len(cache_data) - len(unique_cache)
        
        after_stats = self.get_memory_stats(unique_cache)
        
        return {
            'removed_duplicates': removed_count,
            'unique_cache': unique_cache,
            'memory_before_mb': before_stats.get('rss_mb', 0),
            'memory_after_mb': after_stats.get('rss_mb', 0),
            'memory_saved_mb': before_stats.get('rss_mb', 0) - after_stats.get('rss_mb', 0)
        }
    
    def calculate_dataframe_memory(self, df: pd.DataFrame) -> float:
        """Calculate DataFrame memory usage in MB."""
        return df.memory_usage(deep=True).sum() / 1024 / 1024
    
    def clear_history(self) -> None:
        """Clear memory monitoring history."""
        self._history = []
