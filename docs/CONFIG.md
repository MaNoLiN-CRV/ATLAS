# Atlas Configuration Manager

The Atlas configuration manager handles all application settings by reading from environment variables defined in a `.env` file.

## Setup

1. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` with your actual configuration values:
   ```bash
   # Atlas Database Configuration
   DB_HOST=your_database_host
   USERNAME=your_database_username  
   PASSWORD=your_database_password
   PORT=1433
   
   # Collection interval in seconds
   COLLECTION_LAPSE=60
   ```

## Usage

### Basic Usage

```python
from src.utils.config_manager import config

# Access individual settings
print(f"Database host: {config.db_host}")
print(f"Collection interval: {config.collection_lapse} seconds")

# Get database configuration
db_config = config.get_db_config()
connection_string = config.get_db_connection_string()

# Validate configuration
if config.validate_config():
    print("Configuration is valid")
else:
    print("Configuration is missing required values")
```

### Advanced Usage

```python
from src.utils.config_manager import ConfigManager

# Create custom config manager with different .env file
custom_config = ConfigManager("/path/to/custom/.env")

# Get all configuration as dictionary
all_config = config.get_all_config()

# Reload configuration (useful for testing)
config.reload_config()
```

## Configuration Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `DB_HOST` | Database server hostname/IP | localhost | Yes |
| `USERNAME` | Database username | - | Yes |
| `PASSWORD` | Database password | - | Yes |
| `PORT` | Database port | 1433 | No |
| `COLLECTION_LAPSE` | Data collection interval (seconds) | 60 | No |

## Security

- The `.env` file is automatically added to `.gitignore` to prevent committing sensitive data
- Use `.env.example` as a template for new environments
- Never commit actual database credentials to version control

## Integration with Atlas Components

The config manager is designed to work seamlessly with other Atlas components:

- **Collector**: Uses `collection_lapse` to determine data collection frequency
- **Database Components**: Use database configuration for connections
- **Core**: Accesses all configuration for system initialization

## Error Handling

The config manager provides validation to ensure all required configuration is present:

```python
if not config.validate_config():
    raise ValueError("Missing required configuration. Check your .env file.")
```
