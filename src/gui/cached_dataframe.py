# This will be the cached dataframe module, which will handle caching of dataframes in memory.
# Only can be one instance of the dataframe cache at a time.
from typing import List, Dict, Any, Callable, Optional
import pandas as pd
from threading import Lock
from datetime import datetime

class CachedDataFrame:
    """Singleton class to manage a single cached DataFrame instance."""
    
    _instance: Optional['CachedDataFrame'] = None
    _lock: Lock = Lock()
    
    def __new__(cls) -> 'CachedDataFrame':
        if cls._instance is None:
            with cls._lock:
                # Double-check locking pattern
                if cls._instance is None:
                    cls._instance = super(CachedDataFrame, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        # Only initialize once
        if hasattr(self, '_initialized') and self._initialized:
            return
            
        self._dataframe: pd.DataFrame = pd.DataFrame()
        self._update_callbacks: List[Callable] = []
        self._is_dirty: bool = False
        self._last_update: Optional[datetime] = None
        self._initialized = True
    
    def update_dataframe(self, new_dataframe: pd.DataFrame) -> None:
        """Update the cached DataFrame and notify subscribers."""
        self._dataframe = new_dataframe
        self._is_dirty = False
        self._last_update = datetime.now()
        self._notify_callbacks()
    
    def get_dataframe(self) -> pd.DataFrame:
        """Get the cached DataFrame."""
        return self._dataframe
    
    def is_empty(self) -> bool:
        """Check if the DataFrame is empty."""
        return self._dataframe.empty
    
    def invalidate(self) -> None:
        """Mark the cache as dirty for regeneration."""
        self._is_dirty = True
    
    def is_dirty(self) -> bool:
        """Check if the cache needs regeneration."""
        return self._is_dirty
    
    def subscribe_to_updates(self, callback: Callable) -> None:
        """Subscribe to DataFrame update notifications."""
        if callback not in self._update_callbacks:
            self._update_callbacks.append(callback)
    
    def unsubscribe_from_updates(self, callback: Callable) -> None:
        """Unsubscribe from DataFrame update notifications."""
        if callback in self._update_callbacks:
            self._update_callbacks.remove(callback)
    
    def _notify_callbacks(self) -> None:
        """Notify all subscribers of DataFrame updates."""
        for callback in self._update_callbacks:
            try:
                callback(self._dataframe)
            except Exception as e:
                print(f"Error notifying callback: {e}")
    
    def clear(self) -> None:
        """Clear the cached DataFrame."""
        self._dataframe = pd.DataFrame()
        self._is_dirty = False
        self._last_update = datetime.now()
        self._notify_callbacks()
    
    def get_memory_usage_mb(self) -> float:
        """Get the memory usage of the DataFrame in MB."""
        if self._dataframe.empty:
            return 0.0
        return self._dataframe.memory_usage(deep=True).sum() / (1024 * 1024)
    
    @property
    def last_update(self) -> Optional[datetime]:
        """Get the timestamp of the last update."""
        return self._last_update
    
    @property
    def shape(self) -> tuple:
        """Get the shape of the DataFrame."""
        return self._dataframe.shape

    @classmethod
    def get_instance(cls) -> 'CachedDataFrame':
        """Get the singleton instance explicitly."""
        return cls()

