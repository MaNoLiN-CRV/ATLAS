import threading
import time
from typing import Optional
from src.core.mq_bridge import MQBridge
from src.database import MSSQLConnector
from src.database import SQLiteRepository
from src.performance.analyzer import PerformanceAnalyzer
from src.performance.collector import PerformanceCollector
from src.utils.config_manager import ConfigManager
from src.gui import GUIAdapter
from src.utils.database_utils import DatabaseUtils
from src.core.error_manager import ErrorManager


class Core:
    """
    Core module for Atlas - Database Performance Analysis Tool.

    This class serves as the central coordination logic of the application.
    It coordinates data collection, analysis, and GUI updates using the observer pattern.
    """

    # Class variable to track singleton instance
    _instance = None

    @classmethod
    def get_instance(cls):
        """Get or create the Core singleton instance."""
        if cls._instance is None:
            cls._instance = Core()
        return cls._instance

    def __init__(self):
        """Initialize Core components only if not already initialized."""
        # Check if this instance has been initialized before
        if hasattr(self, '_initialized') and self._initialized:
            print("Core already initialized, skipping initialization.")
            return

        print("Initializing Core components...")
        self.config = ConfigManager()
        self.connector = MSSQLConnector()
        self.sqlite_repository = SQLiteRepository()
        self.mq_bridge : Optional[MQBridge] = None
        self._initialize_database(self.connector)
        self.collector = PerformanceCollector(self.connector)
        self.analyzer = PerformanceAnalyzer(self.sqlite_repository)
        self.databaseUtils = DatabaseUtils(self.connector)
        self.errorManager = ErrorManager()

        # GUI adapter for observer pattern
        self.gui_adapter = GUIAdapter()

        # Initialize RabbitMQ if configured
        if self.config.get_rabbitmq_config().get('enabled', False):
            self.initialize_rabbitmq()



        # Subscribe to analyzer notifications
        self.sqlite_repository.add_observer(self._on_new_data)

        # Threading attributes
        self._collection_thread = None
        self._stop_collection = threading.Event()
        self._collection_interval = self.config.get_collection_lapse()

        # Mark as initialized
        self._initialized = True

    def initialize_rabbitmq(self):
        """Initialize RabbitMQ connection if needed."""
        config = self.config.get_rabbitmq_config()

        from pika import ConnectionParameters, PlainCredentials

        credentials = PlainCredentials(
            username=config.get('username', 'guest'),
            password=config.get('password', 'guest')
        )

        connection_params = ConnectionParameters(
            host=config.get('host', 'localhost'),
            port=config.get('port', 5672),
            virtual_host=config.get('virtual_host', '/'),
            credentials=credentials,
            heartbeat=config.get('heartbeat', 60)
        )

        self.mq_bridge = MQBridge(connection_params=connection_params)

        try:
            self.mq_bridge.connect()
            print("RabbitMQ connection established")

            # Declare necessary queues
            self.mq_bridge.declare_queue('performance_data', durable=True)
            print("Declared RabbitMQ queue 'performance_data'")

            # Verify connection is actually working
            if not hasattr(self.mq_bridge, 'is_connected') or not self.mq_bridge.is_connected():
                raise Exception("Connection established but verification failed")

            print("RabbitMQ initialization completed successfully")

        except Exception as e:
            print(f"WARNING: Failed to connect to RabbitMQ: {e}")
            print("The application will continue but message queue functionality may not work.")

            # Clean up failed connection attempt
            if hasattr(self, 'mq_bridge') and self.mq_bridge:
                try:
                    if hasattr(self.mq_bridge, 'close'):
                        self.mq_bridge.disconnect()
                except:
                    pass
                # Set to None only if the attribute allows it
                try:
                    self.mq_bridge = None
                except:
                    # If we can't set to None, at least try to mark it as disconnected
                    pass


    def get_error_manager(self) -> ErrorManager:
        """Get the ErrorManager instance."""
        return self.errorManager

    def _initialize_database(self, connector: MSSQLConnector):
        """Initialize database connections."""
        try:
            connector.connect(self.config)
            if not connector.test_connection():
                print("WARNING: SQL Server connection test failed.")
                print("The application will continue but data collection may not work.")
        except Exception as e:
            print(f"WARNING: Failed to initialize database connection: {e}")
            print("The application will continue but data collection may not work.")


    def getDatabaseUtils(self) -> DatabaseUtils:
        return self.databaseUtils

    def _on_new_data(self, new_data):
        """Observer callback for new data from analyzer."""
        try:
            # Update GUI adapter with new data
            # new_data is already a list of CustomMetrics
            self.gui_adapter.add_new_data(new_data)
            print(f"GUI updated with {len(new_data)} new performance data items")
        except Exception as e:
            print(f"Error updating GUI: {e}")

    def initialize_gui_data(self):
        """Load initial data for GUI on startup."""
        try:
            # Get all existing data from SQLite
            existing_data = self.analyzer.get_all_metrics()
            if existing_data:
                self.gui_adapter.load_initial_data(existing_data)
                print(f"Loaded {len(existing_data)} existing records for GUI")
            else:
                print("No existing data found for GUI initialization")
        except Exception as e:
            print(f"Error initializing GUI data: {e}")

    def start_data_collection(self):
        """Start the data collection thread."""
        if self._collection_thread is None or not self._collection_thread.is_alive():
            self._stop_collection.clear()
            self.collector.start()  # Initialize collector
            self._collection_thread = threading.Thread(target=self._collection_worker, daemon=True)
            self._collection_thread.start()
            print(f"Data collection started with interval: {self._collection_interval} seconds")

    def stop_data_collection(self):
        """Stop the data collection thread."""
        if self._collection_thread and self._collection_thread.is_alive():
            self._stop_collection.set()
            self.collector.stop()
            self._collection_thread.join(timeout=5)
            print("Data collection stopped")

    def _collection_worker(self):
        """Worker thread that collects data and passes it to analyzer."""
        while not self._stop_collection.is_set():
            try:
                # Collect data from database
                collected_data = self.collector.collect()

                if collected_data and collected_data.get_data():
                    # Pass data to analyzer for processing
                    self.analyzer.process_data(collected_data)
                    print(f"Collected and analyzed {len(collected_data.get_data())} performance records")
                else:
                    print("No new performance data collected in this cycle")

            except Exception as e:
                print(f"Error in data collection worker: {e}")
                import traceback
                traceback.print_exc()

            # Wait for the specified interval or until stop signal
            self._stop_collection.wait(self._collection_interval)

    def run(self):
        """Main execution logic for the core functionality."""
        print("Running Atlas Core...")

        # Initialize GUI with existing data
        self.initialize_gui_data()

        # Start data collection
        self.start_data_collection()

        print("Atlas Core is running. Use Ctrl+C to stop.")

        try:
            # Keep the main thread alive
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down Atlas Core...")
            self.stop_data_collection()

    def run_gui(self):
        """Run the GUI application."""
        from src.gui import create_gui
        from src.gui.loading import show_startup_loading, AsyncLoader
        import streamlit as st

        # Store Core instance in session state if not already there
        if 'core' not in st.session_state:
            # Show loading screen during initialization
            if 'initialization_complete' not in st.session_state:
                try:
                    loader = show_startup_loading()
                    async_loader = AsyncLoader(loader)

                    # Add initialization operations
                    async_loader.add_operation(
                        "🔧 Setting up Core",
                        "Initializing system components",
                        lambda: self._store_core_in_session()
                    )
                    async_loader.add_operation(
                        "🗄️ Connecting to Database",
                        "Establishing database connections",
                        lambda: self._verify_database_connections()
                    )
                    async_loader.add_operation(
                        "📊 Loading Existing Data",
                        "Retrieving performance metrics",
                        lambda: self.initialize_gui_data()
                    )
                    async_loader.add_operation(
                        "🚀 Starting Data Collection",
                        "Initiating background monitoring",
                        lambda: self.start_data_collection()
                    )
                    async_loader.add_operation(
                        "🎨 Preparing Interface",
                        "Setting up dashboard components",
                        lambda: self._prepare_gui_interface()
                    )
                    async_loader.add_operation(
                        "✅ Finalizing Setup",
                        "Completing initialization",
                        lambda: self._complete_initialization()
                    )

                    # Execute all operations
                    async_loader.execute_operations()

                    # Mark initialization as complete
                    st.session_state['initialization_complete'] = True
                    st.rerun()
                    return

                except Exception as e:
                    st.error(f"❌ Initialization failed: {str(e)}")
                    st.info("Please check your configuration and database connection.")
                    return

            st.session_state['core'] = self
            print("Core instance stored in session state")
        else:
            print("Using existing Core instance from session state")

        # Create and run GUI
        try:
            gui = create_gui(self.gui_adapter)
            gui.run()
        except Exception as e:
            st.error(f"❌ Error running GUI: {str(e)}")
            st.info("Please refresh the page or check the logs for more details.")

    def _store_core_in_session(self):
        """Store core instance in session state."""
        import streamlit as st
        st.session_state['core'] = self
        print("Core instance stored in session state")

    def _verify_database_connections(self):
        """Verify database connections are working."""
        try:
            # Test connection
            if hasattr(self.connector, 'test_connection'):
                self.connector.test_connection()
            print("Database connections verified")
        except Exception as e:
            print(f"Database connection verification failed: {e}")
            raise e

    def _prepare_gui_interface(self):
        """Prepare GUI interface components."""
        # Any GUI-specific preparations
        print("GUI interface prepared")

    def _complete_initialization(self):
        """Complete the initialization process."""
        print("Atlas initialization completed successfully")

    def get_gui_adapter(self) -> GUIAdapter:
        """Get the GUI adapter instance."""
        return self.gui_adapter
