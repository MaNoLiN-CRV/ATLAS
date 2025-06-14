import os
from typing import Optional
from dotenv import load_dotenv


class ConfigManager:
    """Configuration manager that loads settings from .env file"""

    # Class attributes to store configuration
    db_host: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    rabbitmq: Optional[bool] = False
    
    port: Optional[int] = None
    database: Optional[str] = None
    driver: Optional[str] = None
    collection_lapse: Optional[int] = None

    # RabbitMQ configuration
    rabbitmq_host: Optional[str] = None
    rabbitmq_port: Optional[int] = None
    rabbitmq_user: Optional[str] = None
    rabbitmq_password: Optional[str] = None
    rabbitmq_exchange: Optional[str] = None
    rabbitmq_queue: Optional[str] = None
    rabbitmq_routing_key: Optional[str] = None
    rabbitmq_vhost: Optional[str] = None
    rabbitmq_ssl: Optional[bool] = None
    rabbitmq_heartbeat: Optional[int] = None

    @staticmethod
    def _load_config_values(env_file_path: Optional[str] = None):
        """Helper static method to load environment variables into class attributes."""
        if env_file_path is None:
            # Get the project root directory (assuming this file is in src/utils/)
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(current_dir))
            env_file_path = os.path.join(project_root, '.env')
        
        # Load environment variables from .env file
        load_dotenv(env_file_path)
        
        # Database configuration
        ConfigManager.db_host = os.getenv('DB_HOST', 'localhost')
        ConfigManager.username = os.getenv('USERNAME', '')
        ConfigManager.password = os.getenv('PASSWORD', '')
        
        port_str = os.getenv('PORT', '1433')
        try:
            ConfigManager.port = int(port_str)
        except ValueError:
            ConfigManager.port = 1433  # Default SQL Server port if conversion fails
            # Consider logging a warning here if a logging mechanism is in place
            
        ConfigManager.database = os.getenv('DATABASE', '')
        ConfigManager.driver = os.getenv('ODBC_DRIVER', 'ODBC Driver 17 for SQL Server')
        
        collection_lapse_str = os.getenv('COLLECTION_LAPSE', '60')
        try:
            ConfigManager.collection_lapse = int(collection_lapse_str)
        except ValueError:
            ConfigManager.collection_lapse = 60  # Default 60 seconds if conversion fails
            # Consider logging a warning here

        # RabbitMQ configuration
        rabbitmq_str = os.getenv('RABBITMQ', 'false').lower()
        ConfigManager.rabbitmq = rabbitmq_str in ('true', '1', 'yes', 'on')
        
        ConfigManager.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        ConfigManager.rabbitmq_user = os.getenv('RABBITMQ_USER', 'guest')
        ConfigManager.rabbitmq_password = os.getenv('RABBITMQ_PASSWORD', 'guest')
        ConfigManager.rabbitmq_exchange = os.getenv('RABBITMQ_EXCHANGE', 'atlas_performance')
        ConfigManager.rabbitmq_queue = os.getenv('RABBITMQ_QUEUE', 'atlas_performance_queue')
        ConfigManager.rabbitmq_routing_key = os.getenv('RABBITMQ_ROUTING_KEY', 'atlas_performance_key')
        ConfigManager.rabbitmq_vhost = os.getenv('RABBITMQ_VHOST', '/')
        
        rabbitmq_port_str = os.getenv('RABBITMQ_PORT', '5672')
        try:
            ConfigManager.rabbitmq_port = int(rabbitmq_port_str)
        except ValueError:
            ConfigManager.rabbitmq_port = 5672  # Default RabbitMQ port
            
        rabbitmq_ssl_str = os.getenv('RABBITMQ_SSL', 'false').lower()
        ConfigManager.rabbitmq_ssl = rabbitmq_ssl_str in ('true', '1', 'yes', 'on')
        
        rabbitmq_heartbeat_str = os.getenv('RABBITMQ_HEARTBEAT', '60')
        try:
            ConfigManager.rabbitmq_heartbeat = int(rabbitmq_heartbeat_str)
        except ValueError:
            ConfigManager.rabbitmq_heartbeat = 60  # Default heartbeat interval

    def __init__(self, env_file_path: Optional[str] = None):
        """
        Initialize the config manager and load environment variables into class attributes.
        
        Args:
            env_file_path: Optional path to .env file. If None, searches for .env in project root
        """
        ConfigManager._load_config_values(env_file_path)
    
    @staticmethod
    def get_db_connection_string() -> str:
        """
        Generate database connection string
        
        Returns:
            Database connection string for SQL Server
        """
        if not all([ConfigManager.driver, ConfigManager.db_host, ConfigManager.port is not None, ConfigManager.database, ConfigManager.username is not None, ConfigManager.password is not None]):
            raise ValueError("Database configuration is not fully loaded. Ensure ConfigManager() was called and .env is correctly set.")
        
        return (f"DRIVER={{{ConfigManager.driver}}};"
                f"SERVER={ConfigManager.db_host},{ConfigManager.port};"
                f"DATABASE={ConfigManager.database};"
                f"UID={ConfigManager.username};"
                f"PWD={ConfigManager.password};")
    
    @staticmethod
    def get_db_config() -> dict:
        """
        Get database configuration as dictionary
        
        Returns:
            Dictionary with database configuration
        """
        return {
            'host': ConfigManager.db_host,
            'username': ConfigManager.username,
            'password': ConfigManager.password,
            'port': ConfigManager.port
        }
    
    @staticmethod
    def get_rabbitmq_config() -> dict:
        """
        Get RabbitMQ configuration as dictionary
        
        Returns:
            Dictionary with RabbitMQ configuration
        """
        return {
            'enabled': ConfigManager.rabbitmq,
            'host': ConfigManager.rabbitmq_host,
            'port': ConfigManager.rabbitmq_port,
            'user': ConfigManager.rabbitmq_user,
            'password': ConfigManager.rabbitmq_password,
            'exchange': ConfigManager.rabbitmq_exchange,
            'queue': ConfigManager.rabbitmq_queue,
            'routing_key': ConfigManager.rabbitmq_routing_key,
            'vhost': ConfigManager.rabbitmq_vhost,
            'ssl': ConfigManager.rabbitmq_ssl,
            'heartbeat': ConfigManager.rabbitmq_heartbeat
        }
    
    @staticmethod
    def get_collection_lapse() -> int:
        """
        Get collection lapse in seconds
        
        Returns:
            Collection interval in seconds
        """
        if ConfigManager.collection_lapse is None:
             raise ValueError("Collection lapse is not loaded. Ensure ConfigManager() was called and .env is correctly set.")
        return ConfigManager.collection_lapse
    
    @staticmethod
    def validate_config() -> bool:
        """
        Validate that all required configuration is present at the class level.
        Note: Considers empty strings as invalid for these fields.
        
        Returns:
            True if configuration is valid, False otherwise
        """
        required_fields_values = [
            ConfigManager.db_host, 
            ConfigManager.username, 
            # Password can be an empty string, so check for None if it should not be None
            # However, os.getenv defaults to '' so it won't be None unless default is None
            ConfigManager.password, # If empty password is valid, this check might need adjustment
            ConfigManager.database
        ]
        # Ensure all required string fields are not None AND not empty strings.
        # For password, an empty string might be a valid value.
        # The original logic `all(field for field in required_fields)` treats empty strings as False.
        # We'll maintain that for db_host, username, database. Password '' is also treated as False by `all()`.
        return all(field for field in required_fields_values)

    @staticmethod
    def reload_config(env_file_path: Optional[str] = None):
        """
        Reload configuration from .env file into class attributes
        
        Args:
            env_file_path: Optional path to .env file
        """
        ConfigManager._load_config_values(env_file_path)
    
    @staticmethod
    def get_all_config() -> dict:
        """
        Get all configuration as dictionary from class attributes
        
        Returns:
            Dictionary with all configuration values
        """
        return {
            'database': ConfigManager.get_db_config(),
            'rabbitmq': ConfigManager.get_rabbitmq_config(),
            'collection_lapse': ConfigManager.collection_lapse,
            'valid': ConfigManager.validate_config()
        }
    
    @staticmethod
    def detect_odbc_drivers() -> list:
        """
        Detects available ODBC drivers for SQL Server
        
        Returns:
            List of available SQL Server ODBC driver names
        """
        try:
            import pyodbc
            drivers = [driver for driver in pyodbc.drivers() if 'SQL Server' in driver]
            return drivers
        except ImportError: # Be specific about the import error
            # pyodbc might not be installed, or other import issues.
            return []
        except Exception: # Catch other potential pyodbc errors
            return []
    
    def __str__(self) -> str:
        """String representation of config (reads from class attributes via self)"""
        # self.attribute will resolve to ConfigManager.attribute if not set on instance
        return (f"ConfigManager(host={self.db_host}, port={self.port}, "
                f"username={self.username}, collection_lapse={self.collection_lapse})")
    
    def __repr__(self) -> str:
        """Detailed representation of config (reads from class attributes via self)"""
        return self.__str__()


# Singleton instance for global access
# This call to ConfigManager() will execute __init__ once, 
# loading values into class attributes via _load_config_values.
config = ConfigManager()