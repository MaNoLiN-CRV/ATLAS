import pyodbc

from src.utils.config_manager import ConfigManager

class MSSQLConnector:
    """A class to manage connections to a Microsoft SQL Server database using pyodbc."""
    def __init__(self):
        self.connection = None

    """Initializes the MSSQLConnector with server, database, username, and password."""
    def connect(self , config: ConfigManager):
        try:
            if self.validate_initialization():
                self.connection = pyodbc.connect(
                    config.get_db_connection_string(),
                    autocommit=True
            )
            print("Connection successful")
        except pyodbc.Error as e:
            print(f"Error connecting to SQL Server: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

    """Closes the connection to the SQL Server database."""
    def close(self):
        if self.connection:
            self.connection.close()
            print("Connection closed")

    """Validates the initialization parameters."""
    def validate_initialization(self):
        result = True
        if not all([self.server, self.database, self.username, self.password]):
            raise ValueError("All parameters (server, database, username, password) must be provided.")
        if self.connection is not None:
            raise ValueError("Connection already initialized. Close the existing connection before re-initializing.")
        return result