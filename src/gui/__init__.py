"""
GUI module for Atlas - Database Performance Analysis Tool.

This module contains the graphical user interface components.
"""

from .main_window import MainWindow, create_gui
from .gui_adapter import GUIAdapter
from .widgets import (
    PerformanceMetricCard,
    QueryPerformanceChart,
    PerformanceAlerts,
    QueryOptimizationSuggestions,
    DataExportWidget,
    RealTimeMonitor
)

__all__ = [
    'MainWindow',
    'GUIAdapter',
    'create_gui',
    'PerformanceMetricCard',
    'QueryPerformanceChart',
    'PerformanceAlerts',
    'QueryOptimizationSuggestions',
    'DataExportWidget',
    'RealTimeMonitor'
]