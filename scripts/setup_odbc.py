# scripts/setup_odbc.py
#!/usr/bin/env python3
import sys
import os
import subprocess
import platform

def main():
    """Setup ODBC drivers for Atlas."""
    print("Atlas ODBC Driver Setup")
    print("=" * 30)
    
    system = platform.system()
    if system == "Darwin":
        print("Setting up ODBC drivers for macOS...")
        try:
            # Check if homebrew is installed
            subprocess.run(["which", "brew"], check=True, stdout=subprocess.PIPE)
            
            # Install unixODBC
            print("Installing unixODBC...")
            subprocess.run(["brew", "install", "unixodbc"], check=True)
            
            # Add Microsoft tap
            print("Adding Microsoft tap...")
            subprocess.run(["brew", "tap", "microsoft/mssql-release", "https://github.com/Microsoft/homebrew-mssql-release"], check=True)
            
            # Update brew
            print("Updating brew...")
            subprocess.run(["brew", "update"], check=True)
            
            # Install driver
            print("Installing ODBC Driver 17 for SQL Server...")
            subprocess.run(["brew", "install", "microsoft/mssql-release/msodbcsql17"], check=True)
            
            print("\nSetup complete! You can now run Atlas with SQL Server connectivity.")
        except subprocess.CalledProcessError as e:
            print(f"Error: {e}")
            print("Please follow the manual installation instructions in the README.")
            sys.exit(1)
    elif system == "Linux":
        print("Please follow the Linux installation instructions in the README:")
        print("""
        curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
        curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list > /etc/apt/sources.list.d/mssql-release.list
        sudo apt-get update
        sudo apt-get install -y unixodbc-dev
        sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17
        """)
    elif system == "Windows":
        print("Please download and install the Microsoft ODBC Driver for SQL Server from the Microsoft Download Center:")
        print("https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server")
    
if __name__ == "__main__":
    main()