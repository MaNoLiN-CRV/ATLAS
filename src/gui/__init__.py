"""
GUI module for Atlas - Database Performance Analysis Tool.

This module contains the graphical user interface components.
"""

from .main_window import MainWindow, create_gui
from .gui_adapter import GUIAdapter
from .loading import LoadingManager, AsyncLoader, show_startup_loading
from .widgets import (
    PerformanceMetricCard,
    QueryPerformanceChart,
    PerformanceAlerts,
    QueryOptimizationSuggestions,
    DataExportWidget,
    RealTimeMonitor,
    MemoryMonitor
)

__all__ = [
    'MainWindow',
    'GUIAdapter',
    'LoadingManager',
    'AsyncLoader',
    'show_startup_loading',
    'create_gui',
    'PerformanceMetricCard',
    'QueryPerformanceChart',
    'PerformanceAlerts',
    'QueryOptimizationSuggestions',
    'DataExportWidget',
    'RealTimeMonitor',
    'MemoryMonitor'
]