"""
Atlas - Database Performance Analysis Tool

A comprehensive tool for collecting, analyzing, and monitoring database performance metrics.
"""

__version__ = "0.1.0"
__author__ = "Atlas Development Team"
__description__ = "Database Performance Analysis Tool"

# Import main modules
from . import common
from . import database
from . import performance
from . import core
from . import gui
from . import utils

__all__ = [
    'common',
    'database', 
    'performance',
    'core',
    'gui',
    'utils'
]