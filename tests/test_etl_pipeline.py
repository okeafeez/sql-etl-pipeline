"""
Unit tests for ETL Pipeline
"""

import unittest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from etl_pipeline import ETLPipeline
from database_manager import DatabaseManager

class TestETLPipeline(unittest.TestCase):
    """Test cases for ETL Pipeline"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_db_manager = Mock(spec=DatabaseManager)
        self.mock_data_validator = Mock()
        
    @patch('etl_pipeline.DatabaseManager')
    @patch('etl_pipeline.DataValidator')
    @patch('etl_pipeline.setup_logging')
    def test_pipeline_initialization(self, mock_logging, mock_validator, mock_db_manager):
        """Test ETL pipeline initialization"""
        pipeline = ETLPipeline()
        
        # Verify components are initialized
        self.assertIsNotNone(pipeline.db_manager)
        self.assertIsNotNone(pipeline.data_validator)
        self.assertIsNotNone(pipeline.pipeline_stats)
        
        # Verify logging is set up
        mock_logging.assert_called_once()
    
    def test_pipeline_stats_initialization(self):
        """Test pipeline statistics initialization"""
        with patch('etl_pipeline.DatabaseManager'), \
             patch('etl_pipeline.DataValidator'), \
             patch('etl_pipeline.setup_logging'):
            
            pipeline = ETLPipeline()
            
            expected_stats = {
                'extracted_records': 0,
                'transformed_records': 0,
                'loaded_records': 0,
                'failed_records': 0,
                'processing_time': 0
            }
            
            self.assertEqual(pipeline.pipeline_stats, expected_stats)
    
    @patch('etl_pipeline.DatabaseManager')
    @patch('etl_pipeline.DataValidator')
    @patch('etl_pipeline.setup_logging')
    def test_extract_data(self, mock_logging, mock_validator, mock_db_manager):
        """Test data extraction"""
        # Set up mock data
        mock_data = pd.DataFrame({
            'customer_id': [1, 2, 3],
            'first_name': ['John', 'Jane', 'Bob'],
            'last_name': ['Doe', 'Smith', 'Johnson'],
            'email': ['john@example.com', 'jane@example.com', 'bob@example.com']
        })
        
        # Configure mock database manager
        mock_db_instance = mock_db_manager.return_value
        mock_db_instance.execute_query.return_value = mock_data
        
        pipeline = ETLPipeline()
        result = pipeline.extract_data('customers')
        
        # Verify extraction
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        self.assertIn('customer_id', result.columns)
        
        # Verify database query was called
        mock_db_instance.execute_query.assert_called_once()
    
    @patch('etl_pipeline.DatabaseManager')
    @patch('etl_pipeline.DataValidator')
    @patch('etl_pipeline.setup_logging')
    def test_transform_customers(self, mock_logging, mock_validator, mock_db_manager):
        """Test customer data transformation"""
        # Set up mock data
        input_data = pd.DataFrame({
            'customer_id': [1, 2],
            'first_name': ['John', 'Jane'],
            'last_name': ['Doe', 'Smith'],
            'email': ['JOHN@EXAMPLE.COM', 'jane@example.com'],
            'phone': ['(555) 123-4567', '555.987.6543']
        })
        
        # Configure mock validator
        mock_validator_instance = mock_validator.return_value
        mock_validator_instance.validate_data.return_value = {
            'is_valid': True,
            'issues': [],
            'warnings': []
        }
        
        pipeline = ETLPipeline()
        result = pipeline._transform_customers(input_data)
        
        # Verify transformations
        self.assertIn('full_name', result.columns)
        self.assertEqual(result.iloc[0]['full_name'], 'John Doe')
        self.assertEqual(result.iloc[0]['email'], 'john@example.com')  # Lowercase
        self.assertEqual(result.iloc[0]['phone'], '5551234567')  # Digits only

if __name__ == '__main__':
    unittest.main()

