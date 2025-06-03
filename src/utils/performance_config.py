"""
Configuration manager for Atlas performance filtering settings.
"""

import json
import os
import logging
from typing import Optional, Dict, Any
from src.performance.performance_thresholds import PerformanceThresholds


class PerformanceConfig:
    """Manages performance filtering configuration."""
    
    def __init__(self, config_file_path: Optional[str] = None):
        if config_file_path is None:
            # Get the project root directory (assuming this file is in src/utils/)
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(current_dir))
            self.config_file_path = os.path.join(project_root, "performance_config.json")
        else:
            self.config_file_path = config_file_path
        
        self.logger = logging.getLogger(__name__)
        self._thresholds = None
        self._load_config()
    
    def _load_config(self) -> None:
        """Load configuration from file or create default."""
        try:
            if os.path.exists(self.config_file_path):
                with open(self.config_file_path, 'r') as f:
                    config_data = json.load(f)
                    self._thresholds = PerformanceThresholds.from_dict(config_data.get('thresholds', {}))
                    self.logger.info(f"Loaded performance configuration from {self.config_file_path}")
            else:
                self._thresholds = PerformanceThresholds()
                self._save_config()
                self.logger.info(f"Created default performance configuration at {self.config_file_path}")
        except Exception as e:
            self.logger.error(f"Error loading performance config: {e}")
            self._thresholds = PerformanceThresholds()
    
    def _save_config(self) -> None:
        """Save current configuration to file."""
        try:
            config_data = {
                'thresholds': self._thresholds.to_dict(),
                'description': 'Atlas Performance Filtering Configuration',
                'version': '1.0'
            }
            
            with open(self.config_file_path, 'w') as f:
                json.dump(config_data, f, indent=2)
            
            self.logger.info(f"Saved performance configuration to {self.config_file_path}")
        except Exception as e:
            self.logger.error(f"Error saving performance config: {e}")
    
    def get_thresholds(self) -> PerformanceThresholds:
        """Get current performance thresholds."""
        return self._thresholds
    
    def update_thresholds(self, new_thresholds: PerformanceThresholds) -> None:
        """Update and save new thresholds."""
        self._thresholds = new_thresholds
        self._save_config()
        self.logger.info("Performance thresholds updated and saved")
    
    def set_conservative_mode(self) -> None:
        """Set conservative thresholds for smaller databases."""
        self._thresholds = PerformanceThresholds.get_conservative_thresholds()
        self._save_config()
        self.logger.info("Switched to conservative performance thresholds")
    
    def set_aggressive_mode(self) -> None:
        """Set aggressive thresholds for larger databases."""
        self._thresholds = PerformanceThresholds.get_aggressive_thresholds()
        self._save_config()
        self.logger.info("Switched to aggressive performance thresholds")
    
    def set_custom_threshold(self, threshold_name: str, value: float) -> bool:
        """Set a specific threshold value."""
        try:
            if hasattr(self._thresholds, threshold_name):
                setattr(self._thresholds, threshold_name, value)
                self._save_config()
                self.logger.info(f"Updated {threshold_name} to {value}")
                return True
            else:
                self.logger.warning(f"Unknown threshold: {threshold_name}")
                return False
        except Exception as e:
            self.logger.error(f"Error setting custom threshold: {e}")
            return False
    
    def get_config_summary(self) -> Dict[str, Any]:
        """Get a summary of current configuration."""
        return {
            'config_file': self.config_file_path,
            'thresholds': self._thresholds.to_dict(),
            'mode_suggestions': {
                'conservative': 'For smaller databases or when you want to capture more issues',
                'default': 'Balanced approach for most databases',
                'aggressive': 'For large databases with many queries, captures only severe issues'
            }
        }
    
    def reset_to_defaults(self) -> None:
        """Reset configuration to default values."""
        self._thresholds = PerformanceThresholds()
        self._save_config()
        self.logger.info("Reset performance configuration to defaults")


# Global configuration instance
_performance_config = None

def get_performance_config() -> PerformanceConfig:
    """Get the global performance configuration instance."""
    global _performance_config
    if _performance_config is None:
        _performance_config = PerformanceConfig()
    return _performance_config
