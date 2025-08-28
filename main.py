#!/usr/bin/env python3
"""
Main Entry Point for SQL ETL Pipeline
Provides command-line interface for running ETL operations
"""

import argparse
import sys
import os
from datetime import datetime

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from etl_pipeline import ETLPipeline
from database_manager import DatabaseManager
from logger_config import setup_logging
from config.config import FileConfig

def main():
    """Main function with command-line interface"""
    
    parser = argparse.ArgumentParser(description='SQL ETL Pipeline')
    parser.add_argument(
        'command',
        choices=['run', 'test', 'incremental', 'status', 'setup'],
        help='Command to execute'
    )
    parser.add_argument(
        '--table',
        type=str,
        help='Specific table to process (for incremental command)'
    )
    parser.add_argument(
        '--tables',
        nargs='+',
        help='List of tables to process (for run command)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    
    # Ensure directories exist
    FileConfig.ensure_directories()
    
    try:
        if args.command == 'setup':
            setup_database()
        elif args.command == 'test':
            test_connections()
        elif args.command == 'run':
            run_full_pipeline(args.tables)
        elif args.command == 'incremental':
            if not args.table:
                print("Error: --table argument is required for incremental command")
                sys.exit(1)
            run_incremental_pipeline(args.table)
        elif args.command == 'status':
            show_pipeline_status()
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

def setup_database():
    """Setup database schemas and tables"""
    print("Setting up database schemas and tables...")
    
    try:
        db_manager = DatabaseManager()
        
        # Execute schema creation script
        schema_file = os.path.join('sql', 'schema.sql')
        if os.path.exists(schema_file):
            db_manager.execute_sql_file(schema_file, database='target')
            print("✓ Database schema created successfully")
        else:
            print("✗ Schema file not found: sql/schema.sql")
            return
        
        # Test connections
        connection_status = db_manager.test_connections()
        if all(connection_status.values()):
            print("✓ Database connections tested successfully")
        else:
            print("✗ Database connection test failed")
            for db, status in connection_status.items():
                print(f"  {db}: {'✓' if status else '✗'}")
        
        db_manager.close_connections()
        
    except Exception as e:
        print(f"✗ Database setup failed: {str(e)}")
        raise

def test_connections():
    """Test database connections"""
    print("Testing database connections...")
    
    try:
        db_manager = DatabaseManager()
        connection_status = db_manager.test_connections()
        
        print("\nConnection Status:")
        for db, status in connection_status.items():
            print(f"  {db.capitalize()} Database: {'✓ Connected' if status else '✗ Failed'}")
        
        if all(connection_status.values()):
            print("\n✓ All database connections successful")
        else:
            print("\n✗ Some database connections failed")
            sys.exit(1)
        
        db_manager.close_connections()
        
    except Exception as e:
        print(f"✗ Connection test failed: {str(e)}")
        raise

def run_full_pipeline(tables=None):
    """Run the complete ETL pipeline"""
    print("Starting full ETL pipeline...")
    print(f"Start time: {datetime.now()}")
    
    if tables:
        print(f"Processing tables: {', '.join(tables)}")
    else:
        print("Processing all tables")
    
    try:
        pipeline = ETLPipeline()
        stats = pipeline.run_full_pipeline(tables)
        
        print("\n" + "="*50)
        print("ETL PIPELINE COMPLETED SUCCESSFULLY")
        print("="*50)
        print(f"Extracted records: {stats['extracted_records']:,}")
        print(f"Transformed records: {stats['transformed_records']:,}")
        print(f"Loaded records: {stats['loaded_records']:,}")
        print(f"Failed records: {stats['failed_records']:,}")
        print(f"Processing time: {stats['processing_time']:.2f} seconds")
        
        if stats['loaded_records'] > 0:
            rate = stats['loaded_records'] / stats['processing_time']
            print(f"Processing rate: {rate:.2f} records/second")
        
        print("="*50)
        
    except Exception as e:
        print(f"\n✗ ETL Pipeline failed: {str(e)}")
        raise

def run_incremental_pipeline(table_name):
    """Run incremental ETL for a specific table"""
    print(f"Starting incremental ETL for table: {table_name}")
    print(f"Start time: {datetime.now()}")
    
    try:
        pipeline = ETLPipeline()
        stats = pipeline.run_incremental_pipeline(table_name)
        
        print(f"\n✓ Incremental ETL completed for {table_name}")
        print(f"Records processed: {stats['loaded_records']:,}")
        print(f"Processing time: {stats['processing_time']:.2f} seconds")
        
    except Exception as e:
        print(f"\n✗ Incremental ETL failed for {table_name}: {str(e)}")
        raise

def show_pipeline_status():
    """Show current pipeline status"""
    print("Pipeline Status")
    print("="*30)
    
    try:
        pipeline = ETLPipeline()
        status = pipeline.get_pipeline_status()
        
        print(f"Current time: {status['current_time']}")
        
        if status['start_time']:
            print(f"Last run start: {status['start_time']}")
            duration = (status['current_time'] - status['start_time']).total_seconds()
            print(f"Last run duration: {duration:.2f} seconds")
        
        print("\nDatabase Connections:")
        for db, connected in status['database_connections'].items():
            print(f"  {db.capitalize()}: {'✓ Connected' if connected else '✗ Disconnected'}")
        
        print("\nLast Run Statistics:")
        stats = status['pipeline_stats']
        for key, value in stats.items():
            if key != 'processing_time':
                print(f"  {key.replace('_', ' ').title()}: {value:,}")
            else:
                print(f"  {key.replace('_', ' ').title()}: {value:.2f} seconds")
        
        # Check log files
        log_dir = FileConfig.LOG_DIR
        if os.path.exists(log_dir):
            print(f"\nLog files location: {os.path.abspath(log_dir)}")
            log_files = [f for f in os.listdir(log_dir) if f.endswith('.log')]
            for log_file in log_files:
                file_path = os.path.join(log_dir, log_file)
                size = os.path.getsize(file_path)
                print(f"  {log_file}: {size:,} bytes")
        
    except Exception as e:
        print(f"✗ Failed to get pipeline status: {str(e)}")
        raise

if __name__ == "__main__":
    main()

