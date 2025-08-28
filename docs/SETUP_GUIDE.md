# Setup and Usage Guide

This guide provides step-by-step instructions for setting up and running the SQL ETL Pipeline project.

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Database Setup](#database-setup)
4. [Configuration](#configuration)
5. [Running the Pipeline](#running-the-pipeline)
6. [Testing](#testing)
7. [Troubleshooting](#troubleshooting)

## Prerequisites

### System Requirements
- **Operating System**: Linux, macOS, or Windows
- **Python**: Version 3.8 or higher
- **PostgreSQL**: Version 12 or higher
- **Memory**: Minimum 4GB RAM (8GB recommended)
- **Storage**: At least 2GB free space

### Software Dependencies
- Git (for cloning the repository)
- PostgreSQL client tools (psql)
- Python package manager (pip)

### PostgreSQL Installation

#### Ubuntu/Debian
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

#### macOS (using Homebrew)
```bash
brew install postgresql
brew services start postgresql
```

#### Windows
Download and install PostgreSQL from the [official website](https://www.postgresql.org/download/windows/).

## Installation

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/sql-etl-pipeline.git
cd sql-etl-pipeline
```

### 2. Create Virtual Environment (Recommended)
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Linux/macOS:
source venv/bin/activate
# On Windows:
venv\Scripts\activate
```

### 3. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 4. Verify Installation
```bash
python --version
pip list
```

## Database Setup

### 1. Create Databases
Connect to PostgreSQL as a superuser and create the required databases:

```sql
-- Connect to PostgreSQL
psql -U postgres

-- Create databases
CREATE DATABASE ecommerce_source;
CREATE DATABASE analytics_dw;

-- Create a user for the ETL pipeline (optional but recommended)
CREATE USER etl_user WITH PASSWORD 'secure_password';
GRANT ALL PRIVILEGES ON DATABASE ecommerce_source TO etl_user;
GRANT ALL PRIVILEGES ON DATABASE analytics_dw TO etl_user;

-- Exit psql
\q
```

### 2. Test Database Connections
```bash
# Test source database connection
psql -h localhost -U etl_user -d ecommerce_source -c "SELECT 1;"

# Test target database connection
psql -h localhost -U etl_user -d analytics_dw -c "SELECT 1;"
```

### 3. Initialize Database Schema
```bash
python main.py setup
```

This command will:
- Create all necessary tables and schemas
- Set up indexes for optimal performance
- Create stored procedures and functions
- Verify database connections

## Configuration

### 1. Environment Variables
Copy the example environment file and customize it:

```bash
cp .env.example .env
```

Edit the `.env` file with your database connection details:

```env
# Source Database Configuration
SOURCE_DB_HOST=localhost
SOURCE_DB_PORT=5432
SOURCE_DB_NAME=ecommerce_source
SOURCE_DB_USER=etl_user
SOURCE_DB_PASSWORD=secure_password

# Target Database Configuration
TARGET_DB_HOST=localhost
TARGET_DB_PORT=5432
TARGET_DB_NAME=analytics_dw
TARGET_DB_USER=etl_user
TARGET_DB_PASSWORD=secure_password

# ETL Configuration
BATCH_SIZE=1000
MAX_RETRIES=3
RETRY_DELAY=5
LOG_LEVEL=INFO

# Data Quality Settings
ENABLE_DATA_VALIDATION=True
NULL_THRESHOLD=0.1

# Incremental Load Settings
ENABLE_INCREMENTAL_LOAD=True
INCREMENTAL_COLUMN=updated_date

# Performance Settings
MAX_WORKERS=4
```

### 2. Configuration Validation
Test your configuration:

```bash
python main.py test
```

This will verify:
- Database connectivity
- Configuration file validity
- Required directories exist
- Permissions are correct

## Running the Pipeline

### 1. Generate Sample Data
Before running the ETL pipeline, generate sample data for testing:

```bash
python src/sample_data_generator.py
```

This creates realistic e-commerce data including:
- 1,000 customers
- 500 products
- 2,000 orders
- 5,000+ order items
- 1,400+ inventory records

### 2. Full Pipeline Execution
Run the complete ETL pipeline:

```bash
python main.py run
```

Expected output:
```
Starting full ETL pipeline at 2024-01-15 10:30:00
✓ Database connections tested successfully
Processing table: customers
Processing table: products
Processing table: orders
Processing table: order_items
Processing table: inventory

==================================================
ETL PIPELINE COMPLETED SUCCESSFULLY
==================================================
Extracted records: 9,942
Transformed records: 9,942
Loaded records: 9,942
Failed records: 0
Processing time: 45.67 seconds
Processing rate: 217.78 records/second
==================================================
```

### 3. Process Specific Tables
Run ETL for specific tables only:

```bash
# Process only customers and products
python main.py run --tables customers products

# Process orders and related data
python main.py run --tables orders order_items
```

### 4. Incremental Processing
Run incremental updates for changed data:

```bash
# Incremental update for customers
python main.py incremental --table customers

# Incremental update for products
python main.py incremental --table products
```

### 5. Pipeline Status
Check pipeline status and statistics:

```bash
python main.py status
```

Output includes:
- Current pipeline status
- Last run statistics
- Database connection status
- Log file locations and sizes

## Advanced Usage

### 1. Custom SQL Queries
Execute advanced analytical queries:

```bash
# Run customer analytics
python src/sql_query_executor.py
```

This executes complex queries demonstrating:
- Customer lifetime value analysis
- Product performance metrics
- Sales trend analysis
- Inventory optimization
- Customer segmentation (RFM analysis)

### 2. Data Validation
Run comprehensive data quality checks:

```bash
python -c "
from src.data_validator import DataValidator
from src.database_manager import DatabaseManager
import pandas as pd

db_manager = DatabaseManager()
validator = DataValidator()

# Load sample data
data = db_manager.execute_query('SELECT * FROM source_ecommerce.customers LIMIT 100')

# Validate data
results = validator.validate_data(data, 'customers')
print('Validation Results:', results)
"
```

### 3. Performance Monitoring
Monitor pipeline performance:

```bash
# View performance logs
tail -f logs/etl_performance.log

# View error logs
tail -f logs/etl_errors.log

# View main pipeline logs
tail -f logs/etl_pipeline.log
```

### 4. Scheduled Execution
Set up automated pipeline execution using cron:

```bash
# Edit crontab
crontab -e

# Add entry for daily execution at 2 AM
0 2 * * * /path/to/venv/bin/python /path/to/sql-etl-pipeline/main.py run >> /path/to/logs/cron.log 2>&1

# Add entry for hourly incremental updates
0 * * * * /path/to/venv/bin/python /path/to/sql-etl-pipeline/main.py incremental --table orders >> /path/to/logs/cron_incremental.log 2>&1
```

## Testing

### 1. Unit Tests
Run the test suite:

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_etl_pipeline.py -v

# Run with coverage report
python -m pytest --cov=src tests/ --cov-report=html
```

### 2. Integration Tests
Test end-to-end pipeline functionality:

```bash
# Test with small dataset
python -c "
from src.sample_data_generator import SampleDataGenerator
generator = SampleDataGenerator()
generator.generate_all_sample_data(num_customers=10, num_products=5, num_orders=20)
"

python main.py run --tables customers products orders
```

### 3. Performance Tests
Test pipeline performance with larger datasets:

```bash
# Generate larger dataset
python -c "
from src.sample_data_generator import SampleDataGenerator
generator = SampleDataGenerator()
generator.generate_all_sample_data(num_customers=10000, num_products=1000, num_orders=50000)
"

# Run performance test
time python main.py run
```

## Monitoring and Maintenance

### 1. Log Management
Monitor and manage log files:

```bash
# View log file sizes
ls -lh logs/

# Rotate logs (if needed)
find logs/ -name "*.log" -size +100M -exec gzip {} \;

# Clean old logs
find logs/ -name "*.log.gz" -mtime +30 -delete
```

### 2. Database Maintenance
Regular database maintenance tasks:

```sql
-- Connect to databases and run maintenance
psql -U etl_user -d ecommerce_source

-- Analyze tables for query optimization
ANALYZE;

-- Update table statistics
VACUUM ANALYZE;

-- Check table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

### 3. Performance Monitoring
Monitor system performance:

```bash
# Check system resources
htop

# Monitor database connections
psql -U etl_user -d analytics_dw -c "
SELECT 
    datname,
    numbackends,
    xact_commit,
    xact_rollback
FROM pg_stat_database 
WHERE datname IN ('ecommerce_source', 'analytics_dw');
"

# Check query performance
psql -U etl_user -d analytics_dw -c "
SELECT 
    query,
    calls,
    total_time,
    mean_time,
    rows
FROM pg_stat_statements 
ORDER BY total_time DESC 
LIMIT 10;
"
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Database Connection Errors

**Error**: `psycopg2.OperationalError: could not connect to server`

**Solutions**:
```bash
# Check PostgreSQL service status
sudo systemctl status postgresql

# Start PostgreSQL if not running
sudo systemctl start postgresql

# Check if PostgreSQL is listening on correct port
sudo netstat -tlnp | grep 5432

# Verify connection parameters
psql -h localhost -U etl_user -d ecommerce_source
```

#### 2. Permission Errors

**Error**: `permission denied for table`

**Solutions**:
```sql
-- Grant necessary permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO etl_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO etl_user;
GRANT USAGE ON SCHEMA public TO etl_user;
```

#### 3. Memory Issues

**Error**: `MemoryError` or slow performance

**Solutions**:
```bash
# Reduce batch size in .env file
BATCH_SIZE=500

# Reduce number of parallel workers
MAX_WORKERS=2

# Monitor memory usage
free -h
```

#### 4. Data Quality Issues

**Error**: Data validation failures

**Solutions**:
```bash
# Check data quality reports
python -c "
from src.data_validator import DataValidator
from src.database_manager import DatabaseManager

db_manager = DatabaseManager()
validator = DataValidator()

# Check specific table
data = db_manager.execute_query('SELECT * FROM source_ecommerce.customers')
results = validator.validate_data(data, 'customers')
print('Issues:', results['issues'])
print('Warnings:', results['warnings'])
"

# Clean data before processing
python -c "
from src.data_validator import DataValidator
validator = DataValidator()
cleaned_data = validator.clean_data(data, 'customers')
"
```

#### 5. Performance Issues

**Slow query performance**:
```sql
-- Check query execution plan
EXPLAIN ANALYZE SELECT * FROM large_table WHERE condition;

-- Check for missing indexes
SELECT 
    schemaname,
    tablename,
    attname,
    n_distinct,
    correlation
FROM pg_stats 
WHERE schemaname = 'source_ecommerce'
ORDER BY n_distinct DESC;

-- Create missing indexes
CREATE INDEX idx_table_column ON table_name(column_name);
```

### Getting Help

#### 1. Check Logs
Always check the log files first:
```bash
# Main pipeline logs
tail -n 100 logs/etl_pipeline.log

# Error logs
tail -n 50 logs/etl_errors.log

# Performance logs
tail -n 50 logs/etl_performance.log
```

#### 2. Enable Debug Mode
For detailed debugging information:
```bash
# Set debug level in .env
LOG_LEVEL=DEBUG

# Run with verbose output
python main.py run --verbose
```

#### 3. Test Individual Components
Test components separately:
```bash
# Test database connections only
python main.py test

# Test data generation only
python src/sample_data_generator.py

# Test specific transformation
python -c "
from src.etl_pipeline import ETLPipeline
pipeline = ETLPipeline()
result = pipeline.extract_data('customers')
print(f'Extracted {len(result)} records')
"
```

#### 4. Community Support
- Check the project's GitHub Issues page
- Review the documentation in the `docs/` directory
- Consult the SQL examples in `docs/SQL_EXAMPLES.md`
- Review the technical guide in `docs/TECHNICAL_GUIDE.md`

This setup guide provides comprehensive instructions for getting the SQL ETL Pipeline up and running in your environment. Follow the steps carefully, and refer to the troubleshooting section if you encounter any issues.

