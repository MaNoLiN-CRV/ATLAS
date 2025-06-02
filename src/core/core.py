import threading
import time
from src.database import MSSQLConnector
from src.database import SQLiteRepository
from src.performance.analyzer import PerformanceAnalyzer
from src.performance.collector import PerformanceCollector
from src.utils.config_manager import ConfigManager


class Core:
    """
    Core module for Atlas - Database Performance Analysis Tool.

    This class serves as the central coordination logic of the application.
    It will be expanded with methods and properties as the application develops.
    """

    def __init__(self):
        self.config = ConfigManager()
        self.connector = MSSQLConnector()
        self.sqlite_repository = SQLiteRepository()
        self._initialize_database(self.connector)
        self.collector = PerformanceCollector(self.connector)
        self.analyzer = PerformanceAnalyzer(self.sqlite_repository)
        
        # Threading attributes
        self._collection_thread = None
        self._stop_collection = threading.Event()
        self._collection_interval = self.config.get('COLLECTION_LAPSE', 30)

    def _initialize_database(self , connector : MSSQLConnector):
        connector.connect()
    
    def start_data_collection(self):
        """Start the data collection thread."""
        if self._collection_thread is None or not self._collection_thread.is_alive():
            self._stop_collection.clear()
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
                
                if collected_data:
                    # Pass data to analyzer for processing
                    self.analyzer.process_data(collected_data)
                    print(f"Collected and analyzed {len(collected_data)} performance records")
                
            except Exception as e:
                print(f"Error in data collection worker: {e}")
            
            # Wait for the specified interval or until stop signal
            self._stop_collection.wait(self._collection_interval)

    def run(self):
        # Main execution logic for the core functionality
        print("Running Atlas Core...")
        self.start_data_collection()