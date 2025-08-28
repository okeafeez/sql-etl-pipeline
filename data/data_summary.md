# Sample Data Summary
Generated on: 2025-08-10 20:04:07

## Customers
- Records: 1,000
- Columns: 14
- Memory usage: 717.3 KB
- Columns:
  - customer_id (int64) - 0 nulls
  - first_name (object) - 0 nulls
  - last_name (object) - 0 nulls
  - email (object) - 0 nulls
  - phone (object) - 0 nulls
  - address (object) - 0 nulls
  - city (object) - 0 nulls
  - state (object) - 0 nulls
  - zip_code (object) - 0 nulls
  - country (object) - 0 nulls
  - registration_date (datetime64[ns]) - 0 nulls
  - last_login (datetime64[ns]) - 210 nulls
  - is_active (bool) - 0 nulls
  - customer_segment (object) - 0 nulls

## Products
- Records: 500
- Columns: 13
- Memory usage: 293.5 KB
- Columns:
  - product_id (int64) - 0 nulls
  - product_name (object) - 0 nulls
  - category (object) - 0 nulls
  - subcategory (object) - 0 nulls
  - brand (object) - 0 nulls
  - price (float64) - 0 nulls
  - cost (float64) - 0 nulls
  - weight (float64) - 0 nulls
  - dimensions (object) - 0 nulls
  - description (object) - 0 nulls
  - created_date (datetime64[ns]) - 0 nulls
  - updated_date (datetime64[ns]) - 0 nulls
  - is_active (bool) - 0 nulls

## Orders
- Records: 2,000
- Columns: 13
- Memory usage: 1063.2 KB
- Columns:
  - order_id (int64) - 0 nulls
  - customer_id (int64) - 0 nulls
  - order_date (datetime64[ns]) - 0 nulls
  - order_status (object) - 0 nulls
  - shipping_address (object) - 0 nulls
  - billing_address (object) - 0 nulls
  - payment_method (object) - 0 nulls
  - shipping_method (object) - 0 nulls
  - order_total (float64) - 0 nulls
  - tax_amount (float64) - 0 nulls
  - shipping_cost (float64) - 0 nulls
  - discount_amount (float64) - 0 nulls
  - notes (object) - 1384 nulls

## Order_Items
- Records: 5,031
- Columns: 7
- Memory usage: 275.3 KB
- Columns:
  - order_item_id (int64) - 0 nulls
  - order_id (int64) - 0 nulls
  - product_id (int64) - 0 nulls
  - quantity (int64) - 0 nulls
  - unit_price (float64) - 0 nulls
  - total_price (float64) - 0 nulls
  - discount_applied (float64) - 0 nulls

## Inventory
- Records: 1,411
- Columns: 7
- Memory usage: 160.0 KB
- Columns:
  - inventory_id (int64) - 0 nulls
  - product_id (int64) - 0 nulls
  - warehouse_location (object) - 0 nulls
  - quantity_on_hand (int64) - 0 nulls
  - quantity_reserved (int64) - 0 nulls
  - reorder_level (int64) - 0 nulls
  - last_updated (datetime64[ns]) - 0 nulls
