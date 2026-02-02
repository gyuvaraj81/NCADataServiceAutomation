import time
import threading
import sys
from typing import Optional


class Spinner:
    """Displays a spinning cursor while processing."""
    
    SPINNER_CHARS = ['|', '/', '-', '\\']

    def __init__(self, message: str = "Processing"):
        """
        Initialize spinner with a message.
        
        Args:
            message: Message to display while spinning
        """
        self.message = message
        self.stop_flag = False
        self.thread: Optional[threading.Thread] = None
        self.start_time = 0

    def _spin(self):
        """Internal spinning animation loop."""
        i = 0
        self.start_time = time.time()
        while not self.stop_flag:
            elapsed = time.time() - self.start_time
            sys.stdout.write(
                f"\r{self.message}... {self.SPINNER_CHARS[i % len(self.SPINNER_CHARS)]} ({elapsed:.1f}s)"
            )
            sys.stdout.flush()
            i += 1
            time.sleep(0.1)
        sys.stdout.write("\r" + " " * (len(self.message) + 30) + "\r")

    def start(self):
        """Start the spinner in a daemon thread."""
        self.stop_flag = False
        self.thread = threading.Thread(target=self._spin, daemon=True)
        self.thread.start()

    def stop(self):
        """Stop the spinner and wait for thread to finish."""
        self.stop_flag = True
        if self.thread:
            self.thread.join()