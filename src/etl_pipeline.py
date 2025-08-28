"""
Main ETL Pipeline Module
Orchestrates the Extract, Transform, Load process
"""

import logging
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import concurrent.futures
from pathlib import Path

from database_manager import DatabaseManager
from data_validator import DataValidator
from logger_config import setup_logging
from config.config import ETLConfig, FileConfig

class ETLPipeline:
    """Main ETL Pipeline orchestrator"""
    
    def __init__(self):
        # Setup logging
        setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.db_manager = DatabaseManager()
        self.data_validator = DataValidator()
        
        # Ensure directories exist
        FileConfig.ensure_directories()
        
        # Pipeline state
        self.pipeline_start_time = None
        self.pipeline_stats = {
            'extracted_records': 0,
            'transformed_records': 0,
            'loaded_records': 0,
            'failed_records': 0,
            'processing_time': 0
        }
        
        self.logger.info("ETL Pipeline initialized successfully")
    
    def run_full_pipeline(self, tables: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run the complete ETL pipeline
        
        Args:
            tables: List of tables to process. If None, processes all tables
        
        Returns:
            Dict containing pipeline execution statistics
        """
        self.pipeline_start_time = datetime.now()
        self.logger.info(f"Starting full ETL pipeline at {self.pipeline_start_time}")
        
        try:
            # Test database connections
            connection_status = self.db_manager.test_connections()
            if not all(connection_status.values()):
                raise Exception("Database connection test failed")
            
            # Define tables to process
            if tables is None:
                tables = ['customers', 'products', 'orders', 'order_items', 'inventory']
            
            # Process each table
            for table in tables:
                self.logger.info(f"Processing table: {table}")
                self._process_table(table)
            
            # Calculate final statistics
            self.pipeline_stats['processing_time'] = (
                datetime.now() - self.pipeline_start_time
            ).total_seconds()
            
            self.logger.info(f"ETL Pipeline completed successfully in {self.pipeline_stats['processing_time']:.2f} seconds")
            self.logger.info(f"Pipeline Statistics: {self.pipeline_stats}")
            
            return self.pipeline_stats
            
        except Exception as e:
            self.logger.error(f"ETL Pipeline failed: {str(e)}")
            raise
        finally:
            self.db_manager.close_connections()
    
    def _process_table(self, table_name: str):
        """Process a single table through the ETL pipeline"""
        try:
            # Extract
            extracted_data = self.extract_data(table_name)
            if extracted_data.empty:
                self.logger.warning(f"No data extracted for table: {table_name}")
                return
            
            self.pipeline_stats['extracted_records'] += len(extracted_data)
            
            # Transform
            transformed_data = self.transform_data(extracted_data, table_name)
            self.pipeline_stats['transformed_records'] += len(transformed_data)
            
            # Load
            loaded_count = self.load_data(transformed_data, table_name)
            self.pipeline_stats['loaded_records'] += loaded_count
            
            self.logger.info(f"Successfully processed {table_name}: {loaded_count} records loaded")
            
        except Exception as e:
            self.logger.error(f"Failed to process table {table_name}: {str(e)}")
            self.pipeline_stats['failed_records'] += 1
            raise
    
    def extract_data(self, table_name: str) -> pd.DataFrame:
        """
        Extract data from source system
        
        Args:
            table_name: Name of the table to extract
        
        Returns:
            pandas.DataFrame: Extracted data
        """
        self.logger.info(f"Extracting data from {table_name}")
        
        try:
            # Determine if incremental load is needed
            if ETLConfig.ENABLE_INCREMENTAL_LOAD:
                last_processed_timestamp = self._get_last_processed_timestamp(table_name)
                if last_processed_timestamp:
                    query = self._build_incremental_query(table_name, last_processed_timestamp)
                else:
                    query = self._build_full_extract_query(table_name)
            else:
                query = self._build_full_extract_query(table_name)
            
            # Execute extraction query
            data = self.db_manager.execute_query(query, database='source')
            
            self.logger.info(f"Extracted {len(data)} records from {table_name}")
            return data
            
        except Exception as e:
            self.logger.error(f"Data extraction failed for {table_name}: {str(e)}")
            raise
    
    def transform_data(self, data: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Transform extracted data
        
        Args:
            data: Raw extracted data
            table_name: Source table name
        
        Returns:
            pandas.DataFrame: Transformed data
        """
        self.logger.info(f"Transforming data for {table_name}")
        
        try:
            # Data validation
            if ETLConfig.ENABLE_DATA_VALIDATION:
                validation_results = self.data_validator.validate_data(data, table_name)
                if not validation_results['is_valid']:
                    self.logger.warning(f"Data validation issues found for {table_name}: {validation_results['issues']}")
            
            # Apply table-specific transformations
            if table_name == 'customers':
                transformed_data = self._transform_customers(data)
            elif table_name == 'products':
                transformed_data = self._transform_products(data)
            elif table_name == 'orders':
                transformed_data = self._transform_orders(data)
            elif table_name == 'order_items':
                transformed_data = self._transform_order_items(data)
            elif table_name == 'inventory':
                transformed_data = self._transform_inventory(data)
            else:
                # Default transformation
                transformed_data = self._apply_default_transformations(data)
            
            self.logger.info(f"Transformed {len(transformed_data)} records for {table_name}")
            return transformed_data
            
        except Exception as e:
            self.logger.error(f"Data transformation failed for {table_name}: {str(e)}")
            raise
    
    def load_data(self, data: pd.DataFrame, table_name: str) -> int:
        """
        Load transformed data into target system
        
        Args:
            data: Transformed data
            table_name: Source table name
        
        Returns:
            int: Number of records loaded
        """
        self.logger.info(f"Loading data for {table_name}")
        
        try:
            # Determine target table and schema
            target_info = self._get_target_table_info(table_name)
            
            # Load to staging first
            staging_table = f"stg_{table_name}"
            self.db_manager.bulk_insert(
                data, 
                staging_table, 
                schema='staging',
                if_exists='replace'
            )
            
            # Then load to final target using SQL
            self._load_to_final_target(staging_table, target_info)
            
            self.logger.info(f"Loaded {len(data)} records for {table_name}")
            return len(data)
            
        except Exception as e:
            self.logger.error(f"Data loading failed for {table_name}: {str(e)}")
            raise
    
    def _build_full_extract_query(self, table_name: str) -> str:
        """Build query for full data extraction"""
        return f"SELECT * FROM source_ecommerce.{table_name}"
    
    def _build_incremental_query(self, table_name: str, last_timestamp: datetime) -> str:
        """Build query for incremental data extraction"""
        return f"""
            SELECT * FROM source_ecommerce.{table_name}
            WHERE {ETLConfig.INCREMENTAL_COLUMN} > '{last_timestamp}'
        """
    
    def _get_last_processed_timestamp(self, table_name: str) -> Optional[datetime]:
        """Get the last processed timestamp for incremental loading"""
        try:
            # This would typically come from a control table
            # For now, return None to trigger full load
            return None
        except Exception:
            return None
    
    def _transform_customers(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform customer data"""
        transformed = data.copy()
        
        # Combine first and last name
        transformed['full_name'] = transformed['first_name'] + ' ' + transformed['last_name']
        
        # Clean phone numbers
        transformed['phone'] = transformed['phone'].str.replace(r'[^\d]', '', regex=True)
        
        # Standardize email to lowercase
        transformed['email'] = transformed['email'].str.lower()
        
        # Add derived fields
        transformed['registration_date'] = pd.to_datetime(transformed['registration_date']).dt.date
        
        return transformed
    
    def _transform_products(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform product data"""
        transformed = data.copy()
        
        # Calculate profit margin
        transformed['profit_margin'] = (
            (transformed['price'] - transformed['cost']) / transformed['price'] * 100
        ).round(2)
        
        # Clean product names
        transformed['product_name'] = transformed['product_name'].str.title()
        
        return transformed
    
    def _transform_orders(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform order data"""
        transformed = data.copy()
        
        # Convert dates
        transformed['order_date'] = pd.to_datetime(transformed['order_date'])
        
        # Calculate net amount
        transformed['net_amount'] = (
            transformed['order_total'] - 
            transformed['tax_amount'] - 
            transformed['shipping_cost']
        )
        
        return transformed
    
    def _transform_order_items(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform order items data"""
        transformed = data.copy()
        
        # Calculate line total
        transformed['line_total'] = transformed['quantity'] * transformed['unit_price']
        
        # Calculate discount percentage
        transformed['discount_percentage'] = (
            transformed['discount_applied'] / transformed['line_total'] * 100
        ).fillna(0).round(2)
        
        return transformed
    
    def _transform_inventory(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform inventory data"""
        transformed = data.copy()
        
        # Calculate available quantity
        transformed['available_quantity'] = (
            transformed['quantity_on_hand'] - transformed['quantity_reserved']
        )
        
        # Determine stock status
        def get_stock_status(row):
            if row['available_quantity'] <= 0:
                return 'Out of Stock'
            elif row['available_quantity'] <= row['reorder_level']:
                return 'Low Stock'
            else:
                return 'In Stock'
        
        transformed['stock_status'] = transformed.apply(get_stock_status, axis=1)
        
        return transformed
    
    def _apply_default_transformations(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply default transformations to any table"""
        transformed = data.copy()
        
        # Add load timestamp
        transformed['load_timestamp'] = datetime.now()
        
        # Clean string columns
        string_columns = transformed.select_dtypes(include=['object']).columns
        for col in string_columns:
            if col not in ['load_timestamp']:
                transformed[col] = transformed[col].astype(str).str.strip()
        
        return transformed
    
    def _get_target_table_info(self, source_table: str) -> Dict[str, str]:
        """Get target table information for a source table"""
        mapping = {
            'customers': {'table': 'dim_customers', 'schema': 'dw_analytics'},
            'products': {'table': 'dim_products', 'schema': 'dw_analytics'},
            'orders': {'table': 'fact_sales', 'schema': 'dw_analytics'},
            'order_items': {'table': 'fact_sales', 'schema': 'dw_analytics'},
            'inventory': {'table': 'fact_inventory', 'schema': 'dw_analytics'}
        }
        
        return mapping.get(source_table, {'table': source_table, 'schema': 'dw_analytics'})
    
    def _load_to_final_target(self, staging_table: str, target_info: Dict[str, str]):
        """Load data from staging to final target table"""
        # This would contain the actual SQL for loading to dimension/fact tables
        # For now, we'll just log the operation
        self.logger.info(f"Loading from staging.{staging_table} to {target_info['schema']}.{target_info['table']}")
    
    def run_incremental_pipeline(self, table_name: str) -> Dict[str, Any]:
        """Run incremental ETL for a specific table"""
        self.logger.info(f"Running incremental ETL for {table_name}")
        
        try:
            self._process_table(table_name)
            return self.pipeline_stats
        except Exception as e:
            self.logger.error(f"Incremental ETL failed for {table_name}: {str(e)}")
            raise
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status and statistics"""
        return {
            'pipeline_stats': self.pipeline_stats,
            'start_time': self.pipeline_start_time,
            'current_time': datetime.now(),
            'database_connections': self.db_manager.test_connections()
        }

