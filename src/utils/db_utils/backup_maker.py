from src.database.mssql_connector import MSSQLConnector
from src.utils.config_manager import ConfigManager

class BackupMaker:
    """
    A class to handle database backup operations.
    This class is designed to work with a MSSQLConnector instance.
    """

    SQL_BACKUP_TEMPLATE: str = """
    BACKUP DATABASE [{db_name}]
    TO DISK = '{target_backup_file_path}'
    WITH FORMAT, INIT, SKIP, NOREWIND, NOUNLOAD, STATS = 10;
    """

    def __init__(self, connector: MSSQLConnector):
        self.connector = connector
        self.database_name = ConfigManager.database

    def make_backup(self, backup_file_path: str) -> bool: 
        """
        Creates a database backup at the specified file path.
        """
        correct = True
        try:
            # Format the SQL query using the instance's database_name
            # and the provided backup_file_path.
            final_query = self.SQL_BACKUP_TEMPLATE.format(
                db_name=self.database_name,
                target_backup_file_path=backup_file_path
            )
            self.connector.execute_query(final_query)
            print(f"Backup of database '{self.database_name}' created at '{backup_file_path}'")
        except Exception as e:
            print(f"Error creating backup: {e}")
            correct = False
        return correct

