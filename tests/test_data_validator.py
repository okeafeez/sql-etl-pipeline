"""
Unit tests for Data Validator
"""

import unittest
import pandas as pd
import numpy as np
from unittest.mock import patch
import sys
import os

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from data_validator import DataValidator

class TestDataValidator(unittest.TestCase):
    """Test cases for Data Validator"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.validator = DataValidator()
        
        # Sample customer data for testing
        self.sample_customers = pd.DataFrame({
            'customer_id': [1, 2, 3, 4, 5],
            'first_name': ['John', 'Jane', 'Bob', '', 'Alice'],
            'last_name': ['Doe', 'Smith', 'Johnson', 'Brown', 'Wilson'],
            'email': ['john@example.com', 'jane@example.com', 'invalid-email', 'bob@example.com', 'alice@example.com'],
            'phone': ['555-123-4567', '(555) 987-6543', '555.555.5555', '123', '555-000-0000'],
            'registration_date': ['2023-01-01', '2023-02-15', '2023-03-20', '2023-04-10', '2023-05-05']
        })
    
    def test_validator_initialization(self):
        """Test validator initialization"""
        self.assertIsInstance(self.validator.validation_rules, dict)
        self.assertIn('customers', self.validator.validation_rules)
        self.assertIn('products', self.validator.validation_rules)
    
    def test_validate_customers_success(self):
        """Test successful customer validation"""
        # Create clean customer data
        clean_data = pd.DataFrame({
            'customer_id': [1, 2, 3],
            'first_name': ['John', 'Jane', 'Bob'],
            'last_name': ['Doe', 'Smith', 'Johnson'],
            'email': ['john@example.com', 'jane@example.com', 'bob@example.com']
        })
        
        result = self.validator.validate_data(clean_data, 'customers')
        
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['issues']), 0)

if __name__ == '__main__':
    unittest.main()

