from enum import Enum
from typing import TypedDict, Optional
class ErrorLog(TypedDict):
    """TypedDict for error log entries."""
    timestamp: str
    level: str
    message: str
    details: Optional[str] = None

    def __str__(self) -> str:
        """String representation of the error log entry."""
        return f"[{self.timestamp}] {self.level}: {self.message} (ID: {self.id})" + (f" - {self.details}" if self.details else "")
    

class ErrorLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"