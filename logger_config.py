import logging
import sys
import uuid
from typing import Optional
from contextvars import ContextVar
from datetime import datetime

# Context variable to store request ID across the request lifecycle
request_id_context: ContextVar[Optional[str]] = ContextVar('request_id', default=None)

class RequestIdFormatter(logging.Formatter):
    """Custom formatter that includes request ID in log messages"""
    
    def format(self, record):
        # Get request ID from context
        request_id = request_id_context.get()
        record.request_id = request_id or 'no-request-id'
        return super().format(record)

def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """
    Setup structured logging configuration for the application
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    
    Returns:
        Configured logger instance
    """
    
    # Create logger
    logger = logging.getLogger("dynasty_stats")
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Remove any existing handlers to avoid duplicates
    logger.handlers = []
    
    # Create console handler with formatting
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    
    # Create detailed formatter with request ID
    formatter = RequestIdFormatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - [%(request_id)s] - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(console_handler)
    
    # Prevent propagation to root logger to avoid duplicate logs
    logger.propagate = False
    
    return logger

def generate_request_id() -> str:
    """Generate a unique request ID"""
    return str(uuid.uuid4())[:8]

def set_request_id(request_id: str) -> None:
    """Set request ID in context for current request"""
    request_id_context.set(request_id)

def get_request_id() -> Optional[str]:
    """Get current request ID from context"""
    return request_id_context.get()

def log_data_shape(logger: logging.Logger, df, stage: str, operation: str = "") -> None:
    """
    Helper function to log DataFrame shape and basic info
    
    Args:
        logger: Logger instance
        df: pandas DataFrame
        stage: Description of the processing stage
        operation: Specific operation being performed
    """
    if df is not None and hasattr(df, 'shape'):
        logger.info(f"Data shape at {stage}{' - ' + operation if operation else ''}: "
                   f"rows={df.shape[0]}, cols={df.shape[1]}")
        if df.empty:
            logger.warning(f"DataFrame is empty at stage: {stage}")
    else:
        logger.error(f"Invalid DataFrame at stage: {stage}")

def log_api_timing(logger: logging.Logger, start_time: datetime, endpoint: str) -> None:
    """
    Log API endpoint timing information
    
    Args:
        logger: Logger instance
        start_time: Request start time
        endpoint: API endpoint name
    """
    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"API endpoint '{endpoint}' completed in {duration:.3f}s")

def log_error_with_context(logger: logging.Logger, error: Exception, context: dict) -> None:
    """
    Log error with additional context information
    
    Args:
        logger: Logger instance
        error: Exception that occurred
        context: Dictionary with additional context
    """
    logger.error(f"Error occurred: {str(error)} | Context: {context}", exc_info=True)