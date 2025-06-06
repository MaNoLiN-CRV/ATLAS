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
    
    SQL_BACKUP_COMPRESSED_TEMPLATE: str = """
    BACKUP DATABASE [{db_name}]
    TO DISK = '{target_backup_file_path}'
    WITH FORMAT, INIT, SKIP, NOREWIND, NOUNLOAD, STATS = 10, COMPRESSION;
    """

    def __init__(self, connector: MSSQLConnector):
        self.connector = connector
        self.database_name = ConfigManager.database

    def make_backup(self, backup_file_path: str , compress: bool = True) -> bool: 
        """
        Creates a database backup at the specified file path.
        """
        correct = True
        try:
            # Seleccionar el template adecuado según si se requiere compresión
            if compress:
                final_query = self.SQL_BACKUP_COMPRESSED_TEMPLATE.format(
                    db_name=self.database_name,
                    target_backup_file_path=backup_file_path
                )
                print(f"Creating compressed backup of database '{self.database_name}'...")
            else:
                final_query = self.SQL_BACKUP_TEMPLATE.format(
                    db_name=self.database_name,
                    target_backup_file_path=backup_file_path
                )
                print(f"Creating uncompressed backup of database '{self.database_name}'...")
            
            self.connector.execute_query(final_query)
            
            compression_status = "compressed" if compress else "uncompressed"
            print(f"{compression_status.capitalize()} backup of database '{self.database_name}' created at '{backup_file_path}'")
        except Exception as e:
            print(f"Error creating backup: {e}")
            correct = False
        return correct

