import pyodbc

class MSSQLConnector:
    """A class to manage connections to a Microsoft SQL Server database using pyodbc."""
    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.connection = None

    """Initializes the MSSQLConnector with server, database, username, and password."""
    def connect(self):
        try:
            if self.validate_initialization():
                self.connection = pyodbc.connect(
                f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                f'SERVER={self.server};'
                f'DATABASE={self.database};'
                f'UID={self.username};'
                f'PWD={self.password}'
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