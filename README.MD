# ATLAS - Database Performance Analysis Tool

![Atlas Logo](atlas.jpg)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/python-3.6+-blue.svg)](https://www.python.org/downloads/)
[![SQL Server](https://img.shields.io/badge/database-SQL%20Server-red.svg)](https://www.microsoft.com/en-us/sql-server)

## 🎯 Overview

ATLAS is a comprehensive database performance monitoring and analysis tool designed to help database administrators and developers optimize SQL Server performance. It provides real-time monitoring, historical analysis, and intelligent insights to identify bottlenecks and problematic queries before they impact your applications.

## ✨ Features

### Core Capabilities
- 📊 **Real-time Performance Monitoring** - Live tracking of SQL Server metrics
- 📈 **Historical Data Analysis** - Trend visualization and pattern recognition
- 🔍 **Query Performance Tracking** - Identify slow and resource-intensive queries
- 🎛️ **Interactive Dashboard** - Customizable views with rich visualizations
- 📤 **Data Export** - Export performance data for further analysis and reporting
- 🔔 **Smart Alerts** - Configurable notifications for performance thresholds

### Technical Features
- **Observer Pattern Implementation** - Efficient real-time data flow
- **SQLite Local Storage** - Fast local caching and historical data retention
- **Modular Architecture** - Collector → Data Analyzer → Snapshot Repository → Core
- **Event-Driven System** - Seamless integration with external systems (email, GUI)

### Data Flow
1. **Collector** gathers performance data from SQL Server
2. **Data Analyzer** parses and transforms data into custom objects
3. **Snapshot Repository** stores objects in SQLite for persistence
4. **Core** receives notifications via Observer pattern and coordinates the system
5. **GUI Adapter** handles both initial data loading and incremental updates

### Architecture ⚙️



## 🚀 Installation

### Prerequisites

- **Python 3.6+** (Python 3.8+ recommended)
- **SQL Server** with appropriate monitoring permissions
- **ODBC Driver for SQL Server**

### Install ODBC Driver

#### Windows
Download and install the Microsoft ODBC Driver for SQL Server from the [Microsoft Download Center](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

#### macOS
```bash
brew install unixodbc
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
brew install microsoft/mssql-release/msodbcsql17
```

#### Linux (Ubuntu/Debian)
```bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list > /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo apt-get install -y unixodbc-dev
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17
```

#### Linux (CentOS/RHEL)
```bash
sudo curl -o /etc/yum.repos.d/mssql-release.repo https://packages.microsoft.com/config/rhel/8/prod.repo
sudo yum remove unixODBC-utf16 unixODBC-utf16-devel
sudo ACCEPT_EULA=Y yum install -y msodbcsql17
sudo yum install -y unixODBC-devel
```

### Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/MaNoLiN-CRV/ATLAS.git
   cd ATLAS
   ```

2. **Create and activate a virtual environment:**
   ```bash
   python -m venv .venv
   
   # On Linux/macOS:
   source .venv/bin/activate
   
   # On Windows:
   .venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure your environment:**
   
   Create a `.env` file in the project root:
   ```env
   # SQL Server Connection Settings
   DB_HOST=your_server_name_or_ip
   USERNAME=your_username
   PASSWORD=your_password
   PORT=1433
   DATABASE=your_database_name
   
   # ODBC Driver Configuration
   ODBC_DRIVER=ODBC Driver 17 for SQL Server
   
   # Performance Collection Settings
   COLLECTION_LAPSE=60
   
   # Optional: Advanced Settings
   MAX_CONNECTIONS=10
   TIMEOUT=30
   ```

## 📖 Usage

### GUI Mode (Default)
Launch the interactive dashboard:
```bash
streamlit run main.py
```

### Command-Line Mode
Run without GUI for server deployments:
```bash
python main.py --nogui
```

### Advanced Options
```bash
# Run with custom configuration
python main.py --config /path/to/config.json

# Run with specific collection interval
python main.py --interval 30

# Run in debug mode
python main.py --debug
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `DB_HOST` | SQL Server hostname or IP | - | ✅ |
| `USERNAME` | Database username | - | ✅ |
| `PASSWORD` | Database password | - | ✅ |
| `PORT` | SQL Server port | 1433 | ❌ |
| `DATABASE` | Target database name | - | ✅ |
| `ODBC_DRIVER` | ODBC driver name | ODBC Driver 17 for SQL Server | ❌ |
| `COLLECTION_LAPSE` | Data collection interval (seconds) | 60 | ❌ |

### Database Permissions

Ensure your SQL Server user has the following permissions:
- `VIEW SERVER STATE`
- `VIEW DATABASE STATE`
- Access to system DMVs (Dynamic Management Views)

## 🐛 Troubleshooting

### Common Issues

#### ODBC Driver Issues
1. **Check installed drivers:**
   ```python
   import pyodbc
   print("Available drivers:", pyodbc.drivers())
   ```

2. **Update driver name in `.env`:**
   ```env
   ODBC_DRIVER=SQL Server Native Client 11.0
   # or
   ODBC_DRIVER=ODBC Driver 13 for SQL Server
   ```

#### Connection Issues
1. **Verify SQL Server is accessible:**
   ```bash
   telnet your_server_name 1433
   ```

2. **Check SQL Server configuration:**
   - Enable TCP/IP protocol
   - Configure firewall rules
   - Verify SQL Server Browser service is running

#### Performance Issues
1. **Adjust collection interval:**
   ```env
   COLLECTION_LAPSE=120  # Increase to 2 minutes
   ```

2. **Monitor resource usage:**
   - Check CPU and memory usage
   - Verify disk space for SQLite database

### Getting Help

- **Documentation:** Check the `/docs` folder for detailed guides


### Development Setup

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes
4. Run tests: `pytest`
5. Commit changes: `git commit -m 'Add amazing feature'`
6. Push to branch: `git push origin feature/amazing-feature`
7. Submit a Pull Request

## 📄 License

This project is licensed under the MIT License.


---

**Made with ❤️ by Manolin**
