
from src.database.mssql_connector import MSSQLConnector


class DatabaseUtils:

    def __init__(self , connector : MSSQLConnector):
        self.connector = connector

    def make_backup(self, backup_path: str):
        """
        Creates a database backup at the specified path.
        This is a placeholder method and should be implemented with actual backup logic.
        """
        raise NotImplementedError("This method should be implemented in a subclass.")
