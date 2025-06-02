#!/usr/bin/env python3
"""
Test database connection for Atlas
"""
import os
import sys
import pyodbc
from pathlib import Path

# Add the parent directory to the path so we can import Atlas modules
parent_dir = Path(__file__).parent.parent
sys.path.append(str(parent_dir))

from src.utils.config_manager import ConfigManager
from src.database.mssql_connector import MSSQLConnector

def test_pyodbc_installation():
    """Test if pyodbc is properly installed"""
    print("Testing pyodbc installation...")
    try:
        print(f"pyodbc version: {pyodbc.version}")
        print("pyodbc is properly installed.")
        return True
    except Exception as e:
        print(f"Error with pyodbc: {e}")
        return False

def list_available_drivers():
    """List all available ODBC drivers"""
    print("\nAvailable ODBC drivers:")
    try:
        drivers = pyodbc.drivers()
        if not drivers:
            print("No ODBC drivers found!")
            return False
            
        for i, driver in enumerate(drivers, 1):
            print(f"{i}. {driver}")
            
        # Identify SQL Server drivers
        sql_server_drivers = [d for d in drivers if 'SQL Server' in d]
        if sql_server_drivers:
            print("\nSQL Server drivers found:")
            for i, driver in enumerate(sql_server_drivers, 1):
                print(f"{i}. {driver}")
        else:
            print("\nNo SQL Server drivers found. You need to install an ODBC driver for SQL Server.")
            
        return bool(sql_server_drivers)
    except Exception as e:
        print(f"Error listing drivers: {e}")
        return False

def test_database_connection():
    """Test connection to the database using settings from .env"""
    print("\nTesting database connection with .env settings...")
    
    try:
        # Load config from .env
        config = ConfigManager()
        
        # Print config for debugging
        print(f"Database host: {config.db_host}")
        print(f"Database: {config.database}")
        print(f"Using driver: {config.driver}")
        
        # Create connector
        connector = MSSQLConnector()
        
        # Test connection
        connector.connect(config)
        if connector.test_connection():
            print("Successfully connected to the database!")
            connector.close()
            return True
        else:
            print("Connection test failed.")
            return False
            
    except Exception as e:
        print(f"Connection error: {e}")
        return False
        
def main():
    """Main function"""
    print("Atlas Database Connection Test")
    print("=" * 30)
    
    # Check if .env exists
    if not os.path.exists(os.path.join(parent_dir, '.env')):
        print("ERROR: .env file not found! Create one based on README instructions.")
        sys.exit(1)
    
    # Run tests
    pyodbc_ok = test_pyodbc_installation()
    drivers_ok = list_available_drivers()
    connection_ok = test_database_connection()
    
    # Summary
    print("\n" + "=" * 30)
    print("Test Summary:")
    print(f"pyodbc installation: {'OK' if pyodbc_ok else 'FAILED'}")
    print(f"SQL Server drivers: {'OK' if drivers_ok else 'FAILED'}")
    print(f"Database connection: {'OK' if connection_ok else 'FAILED'}")
    
    if not pyodbc_ok or not drivers_ok or not connection_ok:
        print("\nSome tests failed. Please check the documentation for troubleshooting.")
        sys.exit(1)
    else:
        print("\nAll tests passed! Your Atlas setup is ready to go.")
        
if __name__ == "__main__":
    main()
