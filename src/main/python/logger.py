import os
import time
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


class LoggerSetup:
    """Manages application logging configuration."""
    
    LOG_FOLDER = "logs"
    LOG_FILE = os.path.join(LOG_FOLDER, "run_query.log")
    LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"

    @staticmethod
    def setup(name: str = None, max_bytes: int = 5 * 1024 * 1024) -> logging.Logger:
        """
        Setup a logger with rotating file handler.
        
        Args:
            name: Logger name
            max_bytes: Maximum file size before rotation
            
        Returns:
            Configured logger instance
        """
        Path(LoggerSetup.LOG_FOLDER).mkdir(parents=True, exist_ok=True)
        
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)

        # Avoid duplicate handlers
        if logger.handlers:
            return logger

        handler = RotatingFileHandler(
            LoggerSetup.LOG_FILE,
            maxBytes=max_bytes,
            backupCount=0,
            encoding="utf-8"
        )

        # Timestamped backup naming
        def namer(name):
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            return name.replace(".log", f"_{timestamp}.log")
        
        handler.namer = namer
        formatter = logging.Formatter(LoggerSetup.LOG_FORMAT)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger


def get_logger(name: str = None) -> logging.Logger:
    """Convenience function to get a configured logger."""
    return LoggerSetup.setup(name)