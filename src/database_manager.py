"""
Database Manager Module
Handles database connections, query execution, and connection pooling
"""

import logging
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
from typing import Optional, Dict, Any, List
import time

from config.config import DatabaseConfig, ETLConfig

class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.source_engine = None
        self.target_engine = None
        self._initialize_engines()
    
    def _initialize_engines(self):
        """Initialize SQLAlchemy engines for source and target databases"""
        try:
            # Source database engine
            self.source_engine = create_engine(
                DatabaseConfig.get_source_connection_string(),
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            # Target database engine
            self.target_engine = create_engine(
                DatabaseConfig.get_target_connection_string(),
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            self.logger.info("Database engines initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize database engines: {str(e)}")
            raise
    
    @contextmanager
    def get_source_connection(self):
        """Context manager for source database connections"""
        connection = None
        try:
            connection = self.source_engine.connect()
            yield connection
        except Exception as e:
            self.logger.error(f"Source database connection error: {str(e)}")
            if connection:
                connection.rollback()
            raise
        finally:
            if connection:
                connection.close()
    
    @contextmanager
    def get_target_connection(self):
        """Context manager for target database connections"""
        connection = None
        try:
            connection = self.target_engine.connect()
            yield connection
        except Exception as e:
            self.logger.error(f"Target database connection error: {str(e)}")
            if connection:
                connection.rollback()
            raise
        finally:
            if connection:
                connection.close()
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None, 
                     database: str = 'source') -> pd.DataFrame:
        """
        Execute a SELECT query and return results as DataFrame
        
        Args:
            query: SQL query to execute
            params: Query parameters
            database: 'source' or 'target'
        
        Returns:
            pandas.DataFrame: Query results
        """
        engine = self.source_engine if database == 'source' else self.target_engine
        
        try:
            with engine.connect() as connection:
                result = pd.read_sql(text(query), connection, params=params)
                self.logger.debug(f"Query executed successfully, returned {len(result)} rows")
                return result
                
        except Exception as e:
            self.logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def execute_non_query(self, query: str, params: Optional[Dict[str, Any]] = None,
                         database: str = 'target') -> int:
        """
        Execute INSERT, UPDATE, DELETE queries
        
        Args:
            query: SQL query to execute
            params: Query parameters
            database: 'source' or 'target'
        
        Returns:
            int: Number of affected rows
        """
        engine = self.source_engine if database == 'source' else self.target_engine
        
        try:
            with engine.connect() as connection:
                with connection.begin():
                    result = connection.execute(text(query), params or {})
                    affected_rows = result.rowcount
                    self.logger.debug(f"Non-query executed successfully, {affected_rows} rows affected")
                    return affected_rows
                    
        except Exception as e:
            self.logger.error(f"Non-query execution failed: {str(e)}")
            raise
    
    def bulk_insert(self, dataframe: pd.DataFrame, table_name: str, 
                   schema: str = None, database: str = 'target',
                   if_exists: str = 'append', method: str = 'multi') -> int:
        """
        Bulk insert DataFrame into database table
        
        Args:
            dataframe: Data to insert
            table_name: Target table name
            schema: Database schema
            database: 'source' or 'target'
            if_exists: 'fail', 'replace', or 'append'
            method: 'multi' for batch insert
        
        Returns:
            int: Number of inserted rows
        """
        engine = self.source_engine if database == 'source' else self.target_engine
        
        try:
            rows_inserted = dataframe.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                method=method,
                chunksize=ETLConfig.BATCH_SIZE
            )
            
            self.logger.info(f"Bulk insert completed: {len(dataframe)} rows inserted into {schema}.{table_name}")
            return len(dataframe)
            
        except Exception as e:
            self.logger.error(f"Bulk insert failed: {str(e)}")
            raise
    
    def get_table_row_count(self, table_name: str, schema: str = None, 
                           database: str = 'source') -> int:
        """Get row count for a table"""
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        query = f"SELECT COUNT(*) as row_count FROM {full_table_name}"
        
        result = self.execute_query(query, database=database)
        return result.iloc[0]['row_count']
    
    def get_max_value(self, table_name: str, column_name: str, 
                     schema: str = None, database: str = 'source'):
        """Get maximum value from a column (useful for incremental loads)"""
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        query = f"SELECT MAX({column_name}) as max_value FROM {full_table_name}"
        
        result = self.execute_query(query, database=database)
        return result.iloc[0]['max_value']
    
    def table_exists(self, table_name: str, schema: str = None, 
                    database: str = 'source') -> bool:
        """Check if table exists"""
        if schema:
            query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = :schema 
                    AND table_name = :table_name
                )
            """
            params = {'schema': schema, 'table_name': table_name}
        else:
            query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = :table_name
                )
            """
            params = {'table_name': table_name}
        
        result = self.execute_query(query, params=params, database=database)
        return result.iloc[0]['exists']
    
    def execute_sql_file(self, file_path: str, database: str = 'target'):
        """Execute SQL commands from a file"""
        try:
            with open(file_path, 'r') as file:
                sql_content = file.read()
            
            # Split by semicolon and execute each statement
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            engine = self.source_engine if database == 'source' else self.target_engine
            
            with engine.connect() as connection:
                with connection.begin():
                    for statement in statements:
                        if statement:
                            connection.execute(text(statement))
            
            self.logger.info(f"SQL file executed successfully: {file_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to execute SQL file {file_path}: {str(e)}")
            raise
    
    def test_connections(self) -> Dict[str, bool]:
        """Test both source and target database connections"""
        results = {}
        
        # Test source connection
        try:
            with self.get_source_connection() as conn:
                conn.execute(text("SELECT 1"))
            results['source'] = True
            self.logger.info("Source database connection test: PASSED")
        except Exception as e:
            results['source'] = False
            self.logger.error(f"Source database connection test: FAILED - {str(e)}")
        
        # Test target connection
        try:
            with self.get_target_connection() as conn:
                conn.execute(text("SELECT 1"))
            results['target'] = True
            self.logger.info("Target database connection test: PASSED")
        except Exception as e:
            results['target'] = False
            self.logger.error(f"Target database connection test: FAILED - {str(e)}")
        
        return results
    
    def close_connections(self):
        """Close all database connections"""
        try:
            if self.source_engine:
                self.source_engine.dispose()
            if self.target_engine:
                self.target_engine.dispose()
            self.logger.info("Database connections closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing database connections: {str(e)}")
    
    def __del__(self):
        """Cleanup when object is destroyed"""
        self.close_connections()

