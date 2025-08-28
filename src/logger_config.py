"""
Logging Configuration Module
Sets up logging for the ETL pipeline
"""

import logging
import logging.handlers
import os
from datetime import datetime
from config.config import ETLConfig, FileConfig

def setup_logging():
    """Setup logging configuration for the ETL pipeline"""
    
    # Ensure log directory exists
    FileConfig.ensure_directories()
    
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, ETLConfig.LOG_LEVEL.upper()))
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    
    simple_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)
    logger.addHandler(console_handler)
    
    # File handler for detailed logs
    log_file_path = os.path.join(FileConfig.LOG_DIR, 'etl_pipeline.log')
    file_handler = logging.handlers.RotatingFileHandler(
        log_file_path,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    logger.addHandler(file_handler)
    
    # Error file handler
    error_log_path = os.path.join(FileConfig.LOG_DIR, 'etl_errors.log')
    error_handler = logging.handlers.RotatingFileHandler(
        error_log_path,
        maxBytes=5*1024*1024,  # 5MB
        backupCount=3
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(detailed_formatter)
    logger.addHandler(error_handler)
    
    # Performance log handler
    perf_log_path = os.path.join(FileConfig.LOG_DIR, 'etl_performance.log')
    perf_handler = logging.FileHandler(perf_log_path)
    perf_handler.setLevel(logging.INFO)
    perf_formatter = logging.Formatter(
        '%(asctime)s - PERFORMANCE - %(message)s'
    )
    perf_handler.setFormatter(perf_formatter)
    
    # Create performance logger
    perf_logger = logging.getLogger('performance')
    perf_logger.addHandler(perf_handler)
    perf_logger.setLevel(logging.INFO)
    perf_logger.propagate = False
    
    logger.info("Logging configuration initialized successfully")

def log_performance(operation: str, duration: float, records_processed: int = 0):
    """Log performance metrics"""
    perf_logger = logging.getLogger('performance')
    
    message = f"Operation: {operation}, Duration: {duration:.2f}s"
    if records_processed > 0:
        rate = records_processed / duration if duration > 0 else 0
        message += f", Records: {records_processed}, Rate: {rate:.2f} records/sec"
    
    perf_logger.info(message)

