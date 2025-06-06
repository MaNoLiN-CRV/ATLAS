"""
GUI module for Atlas - Database Performance Analysis Tool.

This module contains the graphical user interface components.
"""

from .gui_adapter import GUIAdapter
from .main_window import MainWindow
from .loading import LoadingManager, AsyncLoader, show_startup_loading
from .widgets import (
    PerformanceMetricCard,
    QueryPerformanceChart,
    PerformanceAlerts,
    QueryOptimizationSuggestions,
    DataExportWidget,
    RealTimeMonitor,
    MemoryMonitor,
    DatabaseUtilsWidget
)


def create_gui(gui_adapter: GUIAdapter) -> MainWindow:
    """
    Create and return a MainWindow instance with the provided GUI adapter.
    
    Args:
        gui_adapter: The GUIAdapter instance to use for data management
        
    Returns:
        MainWindow: Configured MainWindow instance ready to run
    """
    return MainWindow(gui_adapter)

__all__ = [
    'MainWindow',
    'GUIAdapter',
    'create_gui',
    'LoadingManager',
    'AsyncLoader',
    'show_startup_loading',
    'PerformanceMetricCard',
    'QueryPerformanceChart',
    'PerformanceAlerts',
    'QueryOptimizationSuggestions',
    'DataExportWidget',
    'RealTimeMonitor',
    'MemoryMonitor',
    'DatabaseUtilsWidget'
]