
from src.database import MSSQLConnector
from src.database import SQLiteRepository


class Core:
    """
    Core module for Atlas - Database Performance Analysis Tool.

    This class serves as the central coordination logic of the application.
    It will be expanded with methods and properties as the application develops.
    """

    def __init__(self):
        self.connector = MSSQLConnector()
        self.sqlite_repository = SQLiteRepository()
    
    def _initialize_database(self , connector : MSSQLConnector):
        connector.connect()

    def run(self):
        # Main execution logic for the core functionality
        print("Running Atlas Core...")