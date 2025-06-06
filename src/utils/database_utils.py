
from src.database.mssql_connector import MSSQLConnector
from src.utils.db_utils.backup_maker import BackupMaker


class DatabaseUtils:
    """A utility class for database operations."""
    def __init__(self , connector : MSSQLConnector):
        self.connector = connector
        self.backupMaker = BackupMaker(connector)

    def make_backup(self, backup_path: str):
        """
        Creates a database backup at the specified path.
        """
        self.backupMaker.make_backup(backup_path if backup_path is not None else "backup.bak")
