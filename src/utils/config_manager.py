import os
from typing import Optional
from dotenv import load_dotenv


class ConfigManager:
    """Configuration manager that loads settings from .env file"""
    
    def __init__(self, env_file_path: Optional[str] = None):
        """
        Initialize the config manager and load environment variables
        
        Args:
            env_file_path: Optional path to .env file. If None, searches for .env in project root
        """
        if env_file_path is None:
            # Get the project root directory (assuming this file is in src/utils/)
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(current_dir))
            env_file_path = os.path.join(project_root, '.env')
        
        # Load environment variables from .env file
        load_dotenv(env_file_path)
        
        # Database configuration
        self.db_host = os.getenv('DB_HOST', 'localhost')
        self.username = os.getenv('USERNAME', '')
        self.password = os.getenv('PASSWORD', '')
        self.port = int(os.getenv('PORT', '1433'))  # Default SQL Server port
        
        # Collection configuration
        self.collection_lapse = int(os.getenv('COLLECTION_LAPSE', '60'))  # Default 60 seconds
    
    def get_db_connection_string(self) -> str:
        """
        Generate database connection string
        
        Returns:
            Database connection string for SQL Server
        """
        return (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.db_host},{self.port};"
                f"UID={self.username};"
                f"PWD={self.password};")
    
    def get_db_config(self) -> dict:
        """
        Get database configuration as dictionary
        
        Returns:
            Dictionary with database configuration
        """
        return {
            'host': self.db_host,
            'username': self.username,
            'password': self.password,
            'port': self.port
        }
    
    def get_collection_lapse(self) -> int:
        """
        Get collection lapse in seconds
        
        Returns:
            Collection interval in seconds
        """
        return self.collection_lapse
    
    def validate_config(self) -> bool:
        """
        Validate that all required configuration is present
        
        Returns:
            True if configuration is valid, False otherwise
        """
        required_fields = [self.db_host, self.username, self.password]
        return all(field for field in required_fields)
    
    def reload_config(self, env_file_path: Optional[str] = None):
        """
        Reload configuration from .env file
        
        Args:
            env_file_path: Optional path to .env file
        """
        self.__init__(env_file_path)
    
    def get_all_config(self) -> dict:
        """
        Get all configuration as dictionary
        
        Returns:
            Dictionary with all configuration values
        """
        return {
            'database': self.get_db_config(),
            'collection_lapse': self.collection_lapse,
            'valid': self.validate_config()
        }
    
    def __str__(self) -> str:
        """String representation of config (without sensitive data)"""
        return (f"ConfigManager(host={self.db_host}, port={self.port}, "
                f"username={self.username}, collection_lapse={self.collection_lapse})")
    
    def __repr__(self) -> str:
        """Detailed representation of config (without sensitive data)"""
        return self.__str__()


# Singleton instance for global access
config = ConfigManager()