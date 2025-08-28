"""
Data Validator Module
Provides data quality validation and cleansing functions
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import re
from datetime import datetime

from config.config import ETLConfig

class DataValidator:
    """Data validation and quality checking class"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Define validation rules for each table
        self.validation_rules = {
            'customers': {
                'required_columns': ['customer_id', 'first_name', 'last_name', 'email'],
                'unique_columns': ['customer_id', 'email'],
                'email_columns': ['email'],
                'phone_columns': ['phone'],
                'date_columns': ['registration_date', 'last_login']
            },
            'products': {
                'required_columns': ['product_id', 'product_name', 'price'],
                'unique_columns': ['product_id'],
                'numeric_columns': ['price', 'cost', 'weight'],
                'positive_columns': ['price'],
                'date_columns': ['created_date', 'updated_date']
            },
            'orders': {
                'required_columns': ['order_id', 'customer_id', 'order_date', 'order_total'],
                'unique_columns': ['order_id'],
                'numeric_columns': ['order_total', 'tax_amount', 'shipping_cost'],
                'positive_columns': ['order_total'],
                'date_columns': ['order_date'],
                'foreign_key_columns': {'customer_id': 'customers'}
            },
            'order_items': {
                'required_columns': ['order_item_id', 'order_id', 'product_id', 'quantity', 'unit_price'],
                'unique_columns': ['order_item_id'],
                'numeric_columns': ['quantity', 'unit_price', 'total_price'],
                'positive_columns': ['quantity', 'unit_price'],
                'foreign_key_columns': {'order_id': 'orders', 'product_id': 'products'}
            },
            'inventory': {
                'required_columns': ['inventory_id', 'product_id', 'quantity_on_hand'],
                'unique_columns': ['inventory_id'],
                'numeric_columns': ['quantity_on_hand', 'quantity_reserved', 'reorder_level'],
                'non_negative_columns': ['quantity_on_hand', 'quantity_reserved'],
                'date_columns': ['last_updated'],
                'foreign_key_columns': {'product_id': 'products'}
            }
        }
    
    def validate_data(self, data: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Comprehensive data validation
        
        Args:
            data: DataFrame to validate
            table_name: Name of the table being validated
        
        Returns:
            Dict containing validation results
        """
        self.logger.info(f"Starting data validation for {table_name}")
        
        validation_results = {
            'is_valid': True,
            'issues': [],
            'warnings': [],
            'row_count': len(data),
            'column_count': len(data.columns),
            'null_percentages': {},
            'duplicate_count': 0
        }
        
        if table_name not in self.validation_rules:
            self.logger.warning(f"No validation rules defined for table: {table_name}")
            return validation_results
        
        rules = self.validation_rules[table_name]
        
        try:
            # Check for empty dataset
            if data.empty:
                validation_results['issues'].append("Dataset is empty")
                validation_results['is_valid'] = False
                return validation_results
            
            # Validate required columns
            self._validate_required_columns(data, rules, validation_results)
            
            # Check for null values
            self._validate_null_values(data, rules, validation_results)
            
            # Validate unique constraints
            self._validate_unique_constraints(data, rules, validation_results)
            
            # Validate data types and formats
            self._validate_data_types(data, rules, validation_results)
            
            # Validate business rules
            self._validate_business_rules(data, table_name, validation_results)
            
            # Check for duplicates
            self._check_duplicates(data, validation_results)
            
            # Validate ranges and constraints
            self._validate_ranges(data, rules, validation_results)
            
            self.logger.info(f"Data validation completed for {table_name}. Valid: {validation_results['is_valid']}")
            
        except Exception as e:
            self.logger.error(f"Data validation failed for {table_name}: {str(e)}")
            validation_results['issues'].append(f"Validation error: {str(e)}")
            validation_results['is_valid'] = False
        
        return validation_results
    
    def _validate_required_columns(self, data: pd.DataFrame, rules: Dict, results: Dict):
        """Validate that required columns are present"""
        if 'required_columns' in rules:
            missing_columns = set(rules['required_columns']) - set(data.columns)
            if missing_columns:
                results['issues'].append(f"Missing required columns: {list(missing_columns)}")
                results['is_valid'] = False
    
    def _validate_null_values(self, data: pd.DataFrame, rules: Dict, results: Dict):
        """Check for null values and calculate percentages"""
        null_counts = data.isnull().sum()
        total_rows = len(data)
        
        for column in data.columns:
            null_percentage = (null_counts[column] / total_rows) * 100
            results['null_percentages'][column] = round(null_percentage, 2)
            
            # Check if null percentage exceeds threshold
            if null_percentage > ETLConfig.NULL_THRESHOLD * 100:
                results['warnings'].append(
                    f"Column '{column}' has {null_percentage:.2f}% null values (threshold: {ETLConfig.NULL_THRESHOLD * 100}%)"
                )
            
            # Check required columns for nulls
            if 'required_columns' in rules and column in rules['required_columns']:
                if null_counts[column] > 0:
                    results['issues'].append(f"Required column '{column}' contains {null_counts[column]} null values")
                    results['is_valid'] = False
    
    def _validate_unique_constraints(self, data: pd.DataFrame, rules: Dict, results: Dict):
        """Validate unique constraints"""
        if 'unique_columns' in rules:
            for column in rules['unique_columns']:
                if column in data.columns:
                    duplicate_count = data[column].duplicated().sum()
                    if duplicate_count > 0:
                        results['issues'].append(f"Column '{column}' has {duplicate_count} duplicate values")
                        results['is_valid'] = False
    
    def _validate_data_types(self, data: pd.DataFrame, rules: Dict, results: Dict):
        """Validate data types and formats"""
        
        # Validate email format
        if 'email_columns' in rules:
            for column in rules['email_columns']:
                if column in data.columns:
                    invalid_emails = self._validate_email_format(data[column])
                    if invalid_emails > 0:
                        results['warnings'].append(f"Column '{column}' has {invalid_emails} invalid email formats")
        
        # Validate phone format
        if 'phone_columns' in rules:
            for column in rules['phone_columns']:
                if column in data.columns:
                    invalid_phones = self._validate_phone_format(data[column])
                    if invalid_phones > 0:
                        results['warnings'].append(f"Column '{column}' has {invalid_phones} invalid phone formats")
        
        # Validate numeric columns
        if 'numeric_columns' in rules:
            for column in rules['numeric_columns']:
                if column in data.columns:
                    if not pd.api.types.is_numeric_dtype(data[column]):
                        try:
                            pd.to_numeric(data[column], errors='raise')
                        except (ValueError, TypeError):
                            results['issues'].append(f"Column '{column}' contains non-numeric values")
                            results['is_valid'] = False
        
        # Validate date columns
        if 'date_columns' in rules:
            for column in rules['date_columns']:
                if column in data.columns:
                    invalid_dates = self._validate_date_format(data[column])
                    if invalid_dates > 0:
                        results['warnings'].append(f"Column '{column}' has {invalid_dates} invalid date formats")
    
    def _validate_business_rules(self, data: pd.DataFrame, table_name: str, results: Dict):
        """Validate business-specific rules"""
        
        if table_name == 'products':
            # Price should be greater than cost
            if 'price' in data.columns and 'cost' in data.columns:
                invalid_pricing = (data['price'] < data['cost']).sum()
                if invalid_pricing > 0:
                    results['warnings'].append(f"{invalid_pricing} products have price less than cost")
        
        elif table_name == 'orders':
            # Order total should be positive
            if 'order_total' in data.columns:
                negative_totals = (data['order_total'] <= 0).sum()
                if negative_totals > 0:
                    results['issues'].append(f"{negative_totals} orders have non-positive total amounts")
                    results['is_valid'] = False
        
        elif table_name == 'order_items':
            # Quantity should be positive
            if 'quantity' in data.columns:
                zero_quantity = (data['quantity'] <= 0).sum()
                if zero_quantity > 0:
                    results['issues'].append(f"{zero_quantity} order items have non-positive quantities")
                    results['is_valid'] = False
            
            # Total price should equal quantity * unit_price
            if all(col in data.columns for col in ['quantity', 'unit_price', 'total_price']):
                calculated_total = data['quantity'] * data['unit_price']
                price_mismatch = (abs(data['total_price'] - calculated_total) > 0.01).sum()
                if price_mismatch > 0:
                    results['warnings'].append(f"{price_mismatch} order items have price calculation mismatches")
    
    def _check_duplicates(self, data: pd.DataFrame, results: Dict):
        """Check for duplicate rows"""
        duplicate_count = data.duplicated().sum()
        results['duplicate_count'] = duplicate_count
        
        if duplicate_count > 0:
            results['warnings'].append(f"Found {duplicate_count} duplicate rows")
    
    def _validate_ranges(self, data: pd.DataFrame, rules: Dict, results: Dict):
        """Validate value ranges"""
        
        # Check positive columns
        if 'positive_columns' in rules:
            for column in rules['positive_columns']:
                if column in data.columns:
                    non_positive = (data[column] <= 0).sum()
                    if non_positive > 0:
                        results['issues'].append(f"Column '{column}' has {non_positive} non-positive values")
                        results['is_valid'] = False
        
        # Check non-negative columns
        if 'non_negative_columns' in rules:
            for column in rules['non_negative_columns']:
                if column in data.columns:
                    negative = (data[column] < 0).sum()
                    if negative > 0:
                        results['issues'].append(f"Column '{column}' has {negative} negative values")
                        results['is_valid'] = False
    
    def _validate_email_format(self, email_series: pd.Series) -> int:
        """Validate email format using regex"""
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        # Remove null values for validation
        non_null_emails = email_series.dropna()
        
        if len(non_null_emails) == 0:
            return 0
        
        valid_emails = non_null_emails.str.match(email_pattern, na=False)
        return (~valid_emails).sum()
    
    def _validate_phone_format(self, phone_series: pd.Series) -> int:
        """Validate phone format"""
        # Remove null values for validation
        non_null_phones = phone_series.dropna()
        
        if len(non_null_phones) == 0:
            return 0
        
        # Check for reasonable phone number length (after removing non-digits)
        cleaned_phones = non_null_phones.astype(str).str.replace(r'[^\d]', '', regex=True)
        invalid_length = ((cleaned_phones.str.len() < 10) | (cleaned_phones.str.len() > 15)).sum()
        
        return invalid_length
    
    def _validate_date_format(self, date_series: pd.Series) -> int:
        """Validate date format"""
        # Remove null values for validation
        non_null_dates = date_series.dropna()
        
        if len(non_null_dates) == 0:
            return 0
        
        try:
            pd.to_datetime(non_null_dates, errors='raise')
            return 0
        except (ValueError, TypeError):
            # Count invalid dates
            try:
                valid_dates = pd.to_datetime(non_null_dates, errors='coerce')
                return valid_dates.isna().sum()
            except:
                return len(non_null_dates)
    
    def clean_data(self, data: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Clean and standardize data
        
        Args:
            data: DataFrame to clean
            table_name: Name of the table
        
        Returns:
            pandas.DataFrame: Cleaned data
        """
        self.logger.info(f"Cleaning data for {table_name}")
        
        cleaned_data = data.copy()
        
        try:
            # Remove duplicate rows
            initial_count = len(cleaned_data)
            cleaned_data = cleaned_data.drop_duplicates()
            removed_duplicates = initial_count - len(cleaned_data)
            
            if removed_duplicates > 0:
                self.logger.info(f"Removed {removed_duplicates} duplicate rows from {table_name}")
            
            # Clean string columns
            string_columns = cleaned_data.select_dtypes(include=['object']).columns
            for col in string_columns:
                if col in cleaned_data.columns:
                    # Strip whitespace
                    cleaned_data[col] = cleaned_data[col].astype(str).str.strip()
                    
                    # Replace empty strings with None
                    cleaned_data[col] = cleaned_data[col].replace('', None)
            
            # Table-specific cleaning
            if table_name == 'customers':
                cleaned_data = self._clean_customers(cleaned_data)
            elif table_name == 'products':
                cleaned_data = self._clean_products(cleaned_data)
            
            self.logger.info(f"Data cleaning completed for {table_name}")
            
        except Exception as e:
            self.logger.error(f"Data cleaning failed for {table_name}: {str(e)}")
            raise
        
        return cleaned_data
    
    def _clean_customers(self, data: pd.DataFrame) -> pd.DataFrame:
        """Clean customer-specific data"""
        cleaned = data.copy()
        
        # Standardize email to lowercase
        if 'email' in cleaned.columns:
            cleaned['email'] = cleaned['email'].str.lower()
        
        # Clean phone numbers
        if 'phone' in cleaned.columns:
            cleaned['phone'] = cleaned['phone'].str.replace(r'[^\d]', '', regex=True)
        
        # Capitalize names
        for col in ['first_name', 'last_name']:
            if col in cleaned.columns:
                cleaned[col] = cleaned[col].str.title()
        
        return cleaned
    
    def _clean_products(self, data: pd.DataFrame) -> pd.DataFrame:
        """Clean product-specific data"""
        cleaned = data.copy()
        
        # Standardize product names
        if 'product_name' in cleaned.columns:
            cleaned['product_name'] = cleaned['product_name'].str.title()
        
        # Ensure positive prices
        if 'price' in cleaned.columns:
            cleaned['price'] = cleaned['price'].abs()
        
        return cleaned

