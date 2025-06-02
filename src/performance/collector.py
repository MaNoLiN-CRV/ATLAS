
class PerformanceCollector:
    """A simple performance collector that stores metrics in memory."""
    def __init__(self):
        self.data = []

        def start(self):
            """Initialize the collector."""
            self.data = []

    def collect(self, metric):
        self.data.append(metric)

    def get_report(self):
        return self.data