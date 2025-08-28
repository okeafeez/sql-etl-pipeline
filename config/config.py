"""
Configuration module for SQL ETL Pipeline
Contains database connection settings and ETL configuration parameters
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class DatabaseConfig:
    """Database connection configuration"""
    
    # Source database configuration
    SOURCE_DB_HOST = os.getenv('SOURCE_DB_HOST', 'localhost')
    SOURCE_DB_PORT = os.getenv('SOURCE_DB_PORT', '5432')
    SOURCE_DB_NAME = os.getenv('SOURCE_DB_NAME', 'ecommerce_source')
    SOURCE_DB_USER = os.getenv('SOURCE_DB_USER', 'postgres')
    SOURCE_DB_PASSWORD = os.getenv('SOURCE_DB_PASSWORD', 'password')
    
    # Target database configuration
    TARGET_DB_HOST = os.getenv('TARGET_DB_HOST', 'localhost')
    TARGET_DB_PORT = os.getenv('TARGET_DB_PORT', '5432')
    TARGET_DB_NAME = os.getenv('TARGET_DB_NAME', 'analytics_dw')
    TARGET_DB_USER = os.getenv('TARGET_DB_USER', 'postgres')
    TARGET_DB_PASSWORD = os.getenv('TARGET_DB_PASSWORD', 'password')
    
    @classmethod
    def get_source_connection_string(cls):
        """Get source database connection string"""
        return f"postgresql://{cls.SOURCE_DB_USER}:{cls.SOURCE_DB_PASSWORD}@{cls.SOURCE_DB_HOST}:{cls.SOURCE_DB_PORT}/{cls.SOURCE_DB_NAME}"
    
    @classmethod
    def get_target_connection_string(cls):
        """Get target database connection string"""
        return f"postgresql://{cls.TARGET_DB_USER}:{cls.TARGET_DB_PASSWORD}@{cls.TARGET_DB_HOST}:{cls.TARGET_DB_PORT}/{cls.TARGET_DB_NAME}"

class ETLConfig:
    """ETL process configuration"""
    
    # Batch processing settings
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
    RETRY_DELAY = int(os.getenv('RETRY_DELAY', '5'))  # seconds
    
    # Logging configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE_PATH = os.getenv('LOG_FILE_PATH', 'logs/etl_pipeline.log')
    
    # Data quality settings
    ENABLE_DATA_VALIDATION = os.getenv('ENABLE_DATA_VALIDATION', 'True').lower() == 'true'
    NULL_THRESHOLD = float(os.getenv('NULL_THRESHOLD', '0.1'))  # 10% null values threshold
    
    # Incremental load settings
    ENABLE_INCREMENTAL_LOAD = os.getenv('ENABLE_INCREMENTAL_LOAD', 'True').lower() == 'true'
    INCREMENTAL_COLUMN = os.getenv('INCREMENTAL_COLUMN', 'updated_date')
    
    # Parallel processing
    MAX_WORKERS = int(os.getenv('MAX_WORKERS', '4'))
    
    # Email notification settings (optional)
    ENABLE_EMAIL_NOTIFICATIONS = os.getenv('ENABLE_EMAIL_NOTIFICATIONS', 'False').lower() == 'true'
    SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
    SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
    EMAIL_USER = os.getenv('EMAIL_USER', '')
    EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD', '')
    NOTIFICATION_RECIPIENTS = os.getenv('NOTIFICATION_RECIPIENTS', '').split(',')

class FileConfig:
    """File and directory configuration"""
    
    # Data directories
    DATA_DIR = os.getenv('DATA_DIR', 'data')
    STAGING_DIR = os.path.join(DATA_DIR, 'staging')
    PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')
    ERROR_DIR = os.path.join(DATA_DIR, 'error')
    
    # SQL scripts directory
    SQL_DIR = os.getenv('SQL_DIR', 'sql')
    
    # Log directory
    LOG_DIR = os.getenv('LOG_DIR', 'logs')
    
    @classmethod
    def ensure_directories(cls):
        """Create necessary directories if they don't exist"""
        directories = [
            cls.DATA_DIR,
            cls.STAGING_DIR,
            cls.PROCESSED_DIR,
            cls.ERROR_DIR,
            cls.LOG_DIR
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)

