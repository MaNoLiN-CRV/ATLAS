# This class centralizes error handling for the application, providing a consistent interface for logging and managing errors.

from src.core.types.error_log import ErrorLevel, ErrorLog
from typing import Callable, List



class ErrorManager:
    """Centralized error management system for the application."""

    def __init__(self):
        self.error_log: List[ErrorLog] = []
        self._observers: List[Callable[[ErrorLog], None]] = []


    def register_observer(self, observer: Callable[[ErrorLog], None]):
        """Register an observer to be notified of new errors."""
        self._observers.append(observer)

    def notify_observers(self, error_log: ErrorLog):
        """Notify all registered observers of a new error log entry."""
        for observer in self._observers:
            observer(error_log)

    def log_error(self, level: ErrorLevel, message: str, details: str | None = None):
        """Log an error with a specific level and message."""
        from datetime import datetime

        error_entry = ErrorLog(
            timestamp=datetime.now().isoformat(),
            level=level.value,
            message=message,
            details=details
        )
        self.error_log.append(error_entry)

        # Notify observers
        for observer in self._observers:
            observer(error_entry)

        print(f"Error logged: {error_entry}")

    def get_errors(self):
        """Retrieve all logged errors."""
        return self.error_log

    def clear_errors(self):
        """Clear the error log."""
        self.error_log.clear()
