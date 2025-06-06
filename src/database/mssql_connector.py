import pyodbc

from src.utils.config_manager import ConfigManager

class MSSQLConnector:
    """A class to manage connections to a Microsoft SQL Server database using pyodbc."""
    def __init__(self):
        self.connection = None
        self.config = None
        
    """Initializes the MSSQLConnector with configuration from ConfigManager."""
    def connect(self, config: ConfigManager):
        try:
            self.config = config
            if self.validate_initialization():
                # Try to detect available drivers if connection fails
                try:
                    self.connection = pyodbc.connect(
                        config.get_db_connection_string(),
                        autocommit=True
                    )
                    print(config.get_db_connection_string())
                    print("Connection successful")
                except pyodbc.Error as initial_error:
                    # Check if there's a driver issue
                    available_drivers = config.detect_odbc_drivers()
                    if available_drivers:
                        print(f"Available SQL Server drivers: {', '.join(available_drivers)}")
                        print(f"Using first available driver: {available_drivers[0]}")
                        
                        # Try with the first available driver
                        self.connection = pyodbc.connect(
                            config.get_db_connection_string().replace(
                                f"DRIVER={{{config.driver}}}", 
                                f"DRIVER={{{available_drivers[0]}}}"
                            ),
                            autocommit=True
                        )
                        print("Connection successful with auto-detected driver")
                    else:
                        print(f"Error connecting to SQL Server: {initial_error}")
                        raise
        except pyodbc.Error as e:
            print(f"Error connecting to SQL Server: {e}")
            raise
        except Exception as e:
            print(f"An error occurred: {e}")
            raise

    """Closes the connection to the SQL Server database."""
    def close(self):
        if self.connection:
            self.connection.close()
            print("Connection closed")

    """Validates the initialization parameters."""
    def validate_initialization(self):
        result = True
        if self.config is None:
            raise ValueError("Configuration must be provided before connecting.")
        if not self.config.validate_config():
            raise ValueError("Database configuration is incomplete. Check your configuration.")
        if self.connection is not None:
            raise ValueError("Connection already initialized. Close the existing connection before re-initializing.")
        return result
    
    def get_connection(self):
        """Returns the current database connection."""
        if self.connection is None:
            raise RuntimeError("Connection not established. Call connect() first.")
            
        return self.connection

    def test_connection(self) -> bool:
        """Tests if the database connection is working.
        
        Returns:
            True if connection is successful, False otherwise
        """
        if self.connection is None:
            return False
            
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return True
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False
    
    def execute_query(self, query: str):
        """Execute a SQL query.
        
        Args:
            query: The SQL query to execute
            
        Raises:
            RuntimeError: If connection is not established
            Exception: If query execution fails
        """
        if self.connection is None:
            raise RuntimeError("Connection not established. Call connect() first.")
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                # For queries that return results, you might want to return them
                # For now, this is mainly for DDL/DML operations like BACKUP
        except Exception as e:
            print(f"Query execution failed: {e}")
            raise