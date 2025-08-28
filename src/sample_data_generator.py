"""
Sample Data Generator Module
Generates realistic sample data for testing the ETL pipeline
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from faker import Faker
import os
import sys

# Add config to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config.config import FileConfig

class SampleDataGenerator:
    """Generates realistic sample data for e-commerce system"""
    
    def __init__(self, seed=42):
        """Initialize the data generator with a seed for reproducibility"""
        self.fake = Faker()
        Faker.seed(seed)
        np.random.seed(seed)
        random.seed(seed)
        
        # Product categories and brands
        self.categories = [
            'Electronics', 'Clothing', 'Home & Garden', 'Sports & Outdoors',
            'Books', 'Health & Beauty', 'Toys & Games', 'Automotive',
            'Jewelry', 'Food & Beverages'
        ]
        
        self.subcategories = {
            'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'Headphones', 'Cameras'],
            'Clothing': ['Shirts', 'Pants', 'Dresses', 'Shoes', 'Accessories'],
            'Home & Garden': ['Furniture', 'Kitchen', 'Bedding', 'Decor', 'Tools'],
            'Sports & Outdoors': ['Fitness', 'Camping', 'Sports Equipment', 'Outdoor Gear'],
            'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Children', 'Comics'],
            'Health & Beauty': ['Skincare', 'Makeup', 'Supplements', 'Personal Care'],
            'Toys & Games': ['Board Games', 'Action Figures', 'Educational Toys', 'Video Games'],
            'Automotive': ['Parts', 'Accessories', 'Tools', 'Care Products'],
            'Jewelry': ['Rings', 'Necklaces', 'Earrings', 'Watches', 'Bracelets'],
            'Food & Beverages': ['Snacks', 'Beverages', 'Organic', 'Gourmet', 'Health Foods']
        }
        
        self.brands = [
            'TechCorp', 'StyleMax', 'HomeComfort', 'SportsPro', 'BookWorld',
            'BeautyPlus', 'PlayTime', 'AutoExpert', 'GemCraft', 'FreshTaste',
            'InnovateTech', 'FashionForward', 'LivingSpace', 'ActiveLife',
            'WisdomBooks', 'GlowUp', 'FunZone', 'DriveRight', 'Sparkle',
            'PureFlavor'
        ]
        
        # Customer segments
        self.customer_segments = ['Regular', 'Premium', 'VIP', 'New']
        
        # Order statuses
        self.order_statuses = ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled']
        
        # Payment methods
        self.payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer', 'Cash on Delivery']
        
        # Shipping methods
        self.shipping_methods = ['Standard', 'Express', 'Overnight', 'Economy', 'Priority']
        
        # Warehouse locations
        self.warehouse_locations = ['Warehouse A', 'Warehouse B', 'Warehouse C', 'Warehouse D']
    
    def generate_customers(self, num_customers=1000):
        """Generate customer data"""
        customers = []
        
        for i in range(num_customers):
            # Registration date between 2 years ago and now
            registration_date = self.fake.date_time_between(
                start_date='-2y', 
                end_date='now'
            )
            
            # Last login between registration and now (80% chance of having logged in)
            last_login = None
            if random.random() < 0.8:
                last_login = self.fake.date_time_between(
                    start_date=registration_date,
                    end_date='now'
                )
            
            customer = {
                'customer_id': i + 1,
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name(),
                'email': self.fake.unique.email(),
                'phone': self.fake.phone_number(),
                'address': self.fake.street_address(),
                'city': self.fake.city(),
                'state': self.fake.state(),
                'zip_code': self.fake.zipcode(),
                'country': 'USA',
                'registration_date': registration_date,
                'last_login': last_login,
                'is_active': random.choice([True, True, True, False]),  # 75% active
                'customer_segment': np.random.choice(
                    self.customer_segments,
                    p=[0.6, 0.25, 0.1, 0.05]  # Most are regular customers
                )
            }
            customers.append(customer)
        
        return pd.DataFrame(customers)
    
    def generate_products(self, num_products=500):
        """Generate product data"""
        products = []
        
        for i in range(num_products):
            category = random.choice(self.categories)
            subcategory = random.choice(self.subcategories[category])
            brand = random.choice(self.brands)
            
            # Generate realistic prices based on category
            if category == 'Electronics':
                base_price = random.uniform(50, 2000)
            elif category == 'Clothing':
                base_price = random.uniform(15, 200)
            elif category == 'Home & Garden':
                base_price = random.uniform(20, 500)
            elif category == 'Books':
                base_price = random.uniform(10, 50)
            else:
                base_price = random.uniform(10, 300)
            
            # Cost is typically 40-70% of price
            cost = base_price * random.uniform(0.4, 0.7)
            
            # Created date between 1 year ago and now
            created_date = self.fake.date_time_between(
                start_date='-1y',
                end_date='now'
            )
            
            # Updated date between created date and now
            updated_date = self.fake.date_time_between(
                start_date=created_date,
                end_date='now'
            )
            
            product = {
                'product_id': i + 1,
                'product_name': f"{brand} {subcategory} {self.fake.word().title()}",
                'category': category,
                'subcategory': subcategory,
                'brand': brand,
                'price': round(base_price, 2),
                'cost': round(cost, 2),
                'weight': round(random.uniform(0.1, 10.0), 2),
                'dimensions': f"{random.randint(5, 50)}x{random.randint(5, 50)}x{random.randint(5, 50)} cm",
                'description': self.fake.text(max_nb_chars=200),
                'created_date': created_date,
                'updated_date': updated_date,
                'is_active': random.choice([True, True, True, False])  # 75% active
            }
            products.append(product)
        
        return pd.DataFrame(products)
    
    def generate_orders(self, customers_df, num_orders=2000):
        """Generate order data"""
        orders = []
        
        # Get active customers for order generation
        active_customers = customers_df[customers_df['is_active'] == True]['customer_id'].tolist()
        
        for i in range(num_orders):
            customer_id = random.choice(active_customers)
            
            # Order date between 6 months ago and now
            order_date = self.fake.date_time_between(
                start_date='-6m',
                end_date='now'
            )
            
            # Generate order amounts based on customer segment
            customer_segment = customers_df[customers_df['customer_id'] == customer_id]['customer_segment'].iloc[0]
            
            if customer_segment == 'VIP':
                base_amount = random.uniform(200, 1000)
            elif customer_segment == 'Premium':
                base_amount = random.uniform(100, 500)
            else:
                base_amount = random.uniform(25, 200)
            
            tax_rate = 0.08  # 8% tax
            shipping_cost = random.uniform(5, 25)
            discount_amount = base_amount * random.uniform(0, 0.15)  # Up to 15% discount
            
            order_total = base_amount + (base_amount * tax_rate) + shipping_cost - discount_amount
            
            order = {
                'order_id': i + 1,
                'customer_id': customer_id,
                'order_date': order_date,
                'order_status': np.random.choice(
                    self.order_statuses,
                    p=[0.1, 0.15, 0.25, 0.45, 0.05]  # Most orders are delivered
                ),
                'shipping_address': self.fake.address(),
                'billing_address': self.fake.address(),
                'payment_method': random.choice(self.payment_methods),
                'shipping_method': random.choice(self.shipping_methods),
                'order_total': round(order_total, 2),
                'tax_amount': round(base_amount * tax_rate, 2),
                'shipping_cost': round(shipping_cost, 2),
                'discount_amount': round(discount_amount, 2),
                'notes': self.fake.text(max_nb_chars=100) if random.random() < 0.3 else None
            }
            orders.append(order)
        
        return pd.DataFrame(orders)
    
    def generate_order_items(self, orders_df, products_df, avg_items_per_order=2.5):
        """Generate order items data"""
        order_items = []
        active_products = products_df[products_df['is_active'] == True]
        
        order_item_id = 1
        
        for _, order in orders_df.iterrows():
            # Determine number of items in this order
            num_items = max(1, int(np.random.poisson(avg_items_per_order)))
            
            # Select random products for this order
            selected_products = active_products.sample(n=min(num_items, len(active_products)))
            
            for _, product in selected_products.iterrows():
                quantity = random.randint(1, 5)
                unit_price = product['price']
                
                # Apply random discount (0-20%)
                discount_applied = unit_price * quantity * random.uniform(0, 0.2)
                total_price = (unit_price * quantity) - discount_applied
                
                order_item = {
                    'order_item_id': order_item_id,
                    'order_id': order['order_id'],
                    'product_id': product['product_id'],
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'total_price': round(total_price, 2),
                    'discount_applied': round(discount_applied, 2)
                }
                order_items.append(order_item)
                order_item_id += 1
        
        return pd.DataFrame(order_items)
    
    def generate_inventory(self, products_df):
        """Generate inventory data"""
        inventory = []
        
        for i, product in products_df.iterrows():
            for location in self.warehouse_locations:
                # Skip some products in some warehouses (not all products in all locations)
                if random.random() < 0.3:
                    continue
                
                quantity_on_hand = random.randint(0, 1000)
                quantity_reserved = random.randint(0, min(quantity_on_hand, 50))
                reorder_level = random.randint(10, 100)
                
                # Last updated between 30 days ago and now
                last_updated = self.fake.date_time_between(
                    start_date='-30d',
                    end_date='now'
                )
                
                inventory_record = {
                    'inventory_id': len(inventory) + 1,
                    'product_id': product['product_id'],
                    'warehouse_location': location,
                    'quantity_on_hand': quantity_on_hand,
                    'quantity_reserved': quantity_reserved,
                    'reorder_level': reorder_level,
                    'last_updated': last_updated
                }
                inventory.append(inventory_record)
        
        return pd.DataFrame(inventory)
    
    def generate_all_sample_data(self, num_customers=1000, num_products=500, num_orders=2000):
        """Generate all sample data and save to CSV files"""
        print("Generating sample data...")
        
        # Ensure data directory exists
        FileConfig.ensure_directories()
        data_dir = FileConfig.DATA_DIR
        
        # Generate data
        print("Generating customers...")
        customers_df = self.generate_customers(num_customers)
        
        print("Generating products...")
        products_df = self.generate_products(num_products)
        
        print("Generating orders...")
        orders_df = self.generate_orders(customers_df, num_orders)
        
        print("Generating order items...")
        order_items_df = self.generate_order_items(orders_df, products_df)
        
        print("Generating inventory...")
        inventory_df = self.generate_inventory(products_df)
        
        # Save to CSV files
        datasets = {
            'customers': customers_df,
            'products': products_df,
            'orders': orders_df,
            'order_items': order_items_df,
            'inventory': inventory_df
        }
        
        file_paths = {}
        for name, df in datasets.items():
            file_path = os.path.join(data_dir, f'sample_{name}.csv')
            df.to_csv(file_path, index=False)
            file_paths[name] = file_path
            print(f"Saved {len(df)} {name} records to {file_path}")
        
        # Generate summary statistics
        self._generate_data_summary(datasets, data_dir)
        
        return file_paths
    
    def _generate_data_summary(self, datasets, data_dir):
        """Generate a summary of the generated data"""
        summary = []
        summary.append("# Sample Data Summary")
        summary.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        summary.append("")
        
        for name, df in datasets.items():
            summary.append(f"## {name.title()}")
            summary.append(f"- Records: {len(df):,}")
            summary.append(f"- Columns: {len(df.columns)}")
            summary.append(f"- Memory usage: {df.memory_usage(deep=True).sum() / 1024:.1f} KB")
            
            # Show column info
            summary.append("- Columns:")
            for col in df.columns:
                dtype = str(df[col].dtype)
                null_count = df[col].isnull().sum()
                summary.append(f"  - {col} ({dtype}) - {null_count} nulls")
            summary.append("")
        
        # Save summary
        summary_path = os.path.join(data_dir, 'data_summary.md')
        with open(summary_path, 'w') as f:
            f.write('\n'.join(summary))
        
        print(f"Data summary saved to {summary_path}")

if __name__ == "__main__":
    # Generate sample data when run directly
    generator = SampleDataGenerator()
    file_paths = generator.generate_all_sample_data()
    
    print("\nSample data generation completed!")
    print("Files generated:")
    for name, path in file_paths.items():
        print(f"  {name}: {path}")

