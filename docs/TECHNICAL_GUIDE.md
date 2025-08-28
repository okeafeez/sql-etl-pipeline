
# Technical Guide: SQL ETL Pipeline

## Table of Contents
1. [Architecture Deep Dive](#architecture-deep-dive)
2. [Database Design](#database-design)
3. [ETL Process Flow](#etl-process-flow)
4. [Advanced SQL Techniques](#advanced-sql-techniques)
5. [Performance Optimization](#performance-optimization)
6. [Data Quality Framework](#data-quality-framework)
7. [Monitoring and Observability](#monitoring-and-observability)
8. [Troubleshooting Guide](#troubleshooting-guide)

## Architecture Deep Dive

### System Components

The SQL ETL pipeline is built using a modular architecture that separates concerns and enables scalability. The system consists of several key components that work together to provide a robust data processing solution.

#### Database Manager (`database_manager.py`)

The Database Manager serves as the central component for all database interactions. It implements connection pooling using SQLAlchemy's QueuePool to efficiently manage database connections and prevent connection exhaustion. The manager provides context managers for safe connection handling, ensuring that connections are properly closed even in error scenarios.

The connection pooling configuration includes:
- Pool size of 5 connections for normal operations
- Maximum overflow of 10 additional connections during peak loads
- Pre-ping functionality to validate connections before use
- Connection recycling every 3600 seconds to prevent stale connections

#### ETL Pipeline Orchestrator (`etl_pipeline.py`)

The main ETL orchestrator coordinates the entire data processing workflow. It implements a three-phase approach: Extract, Transform, and Load. The orchestrator maintains pipeline statistics and provides comprehensive error handling throughout the process.

Key features include:
- Configurable batch processing for large datasets
- Incremental loading capabilities to process only changed data
- Parallel processing support for independent operations
- Comprehensive logging and performance metrics collection

#### Data Validator (`data_validator.py`)

The data validation component ensures data quality throughout the ETL process. It implements multiple validation strategies including schema validation, business rule checking, and statistical analysis for outlier detection.

Validation categories include:
- **Structural Validation**: Ensures data conforms to expected schema
- **Business Rule Validation**: Validates domain-specific constraints
- **Statistical Validation**: Identifies outliers and anomalies
- **Referential Integrity**: Validates foreign key relationships

### Data Flow Architecture

The pipeline follows a layered architecture with clear separation between operational and analytical systems:

```
[Source Systems] → [Staging Layer] → [Transformation Layer] → [Data Warehouse]
```

#### Source Layer
The source layer represents operational systems containing transactional data. In our e-commerce example, this includes customer management systems, product catalogs, order processing systems, and inventory management systems.

#### Staging Layer
The staging layer provides a temporary holding area for extracted data. This layer enables:
- Data validation before transformation
- Recovery capabilities in case of transformation failures
- Audit trails for data lineage tracking
- Isolation of source systems from transformation processes

#### Transformation Layer
The transformation layer applies business logic and data quality rules. This is where the advanced SQL techniques are primarily utilized, including:
- Complex aggregations using window functions
- Data enrichment through sophisticated joins
- Business logic implementation using CTEs
- Data standardization and cleansing

#### Data Warehouse Layer
The target data warehouse implements a star schema optimized for analytical queries. The schema includes dimension tables for master data and fact tables for transactional data.

## Database Design

### Source System Schema

The source system implements a normalized relational design typical of operational systems. This design prioritizes data integrity and transaction processing efficiency.

#### Customers Table
The customers table stores master customer data with proper normalization to avoid data redundancy. Key design decisions include:
- Use of surrogate keys (customer_id) for stable references
- Separation of contact information for flexibility
- Customer segmentation fields for business analytics
- Audit fields for tracking data changes

#### Products Table
The products table maintains the product catalog with hierarchical categorization:
- Category and subcategory fields enable flexible product organization
- Separate price and cost fields support profitability analysis
- Product lifecycle management through is_active flags
- Comprehensive product attributes for detailed analytics

#### Orders and Order Items
The order processing tables implement a header-detail pattern:
- Orders table contains header information (customer, dates, totals)
- Order items table contains line-level details (products, quantities, prices)
- This design supports complex order structures and detailed analytics

### Data Warehouse Schema

The data warehouse implements a dimensional model optimized for analytical queries and reporting.

#### Dimension Tables

**Customer Dimension (dim_customers)**
The customer dimension provides a single source of truth for customer analytics. It includes:
- Slowly Changing Dimension (SCD) Type 1 implementation for current state
- Derived attributes like full_name for reporting convenience
- Customer segmentation fields for analytical grouping
- Audit fields for tracking dimension changes

**Product Dimension (dim_products)**
The product dimension enables comprehensive product analytics:
- Product hierarchy (category, subcategory) for drill-down analysis
- Calculated fields like profit_margin for business metrics
- Product lifecycle tracking through is_active flags
- Brand and category groupings for market analysis

**Date Dimension (dim_date)**
The date dimension provides comprehensive time-based analytics:
- Complete date attributes (year, quarter, month, week, day)
- Business calendar support with holiday flags
- Fiscal year support for financial reporting
- Pre-calculated date ranges for common reporting periods

#### Fact Tables

**Sales Fact Table (fact_sales)**
The sales fact table captures transactional sales data:
- Grain: One row per order line item
- Measures: quantity, sales amount, cost, profit
- Foreign keys to all relevant dimensions
- Additive measures supporting aggregation at any level

**Inventory Fact Table (fact_inventory)**
The inventory fact table provides point-in-time inventory snapshots:
- Grain: One row per product per warehouse per day
- Measures: on-hand quantity, reserved quantity, available quantity
- Support for inventory trend analysis and stock optimization

### Indexing Strategy

The database design includes a comprehensive indexing strategy to optimize query performance:

#### Primary Indexes
- All tables include primary key indexes for unique identification
- Surrogate keys used consistently for stable references
- Composite primary keys where natural keys span multiple columns

#### Foreign Key Indexes
- All foreign key columns include indexes for join optimization
- Composite indexes for multi-column foreign keys
- Covering indexes where beneficial for query performance

#### Analytical Indexes
- Date-based indexes for time-series analysis
- Category-based indexes for product analytics
- Customer segment indexes for targeted analysis

## ETL Process Flow

### Extract Phase

The extraction phase retrieves data from source systems using configurable strategies:

#### Full Extraction
Full extraction retrieves all data from source tables. This approach is used for:
- Initial data loads
- Small reference tables
- Tables without reliable change tracking

#### Incremental Extraction
Incremental extraction processes only changed data since the last successful run:
- Uses timestamp-based change detection
- Maintains high-water marks for each table
- Supports both insert and update detection
- Handles deleted records through soft delete patterns

#### Change Data Capture (CDC)
For high-volume tables, the pipeline supports CDC patterns:
- Log-based change detection
- Real-time or near-real-time processing
- Minimal impact on source systems
- Complete audit trail of all changes

### Transform Phase

The transformation phase applies business logic and data quality rules:

#### Data Validation
Comprehensive validation ensures data quality:
- Schema validation against expected structure
- Business rule validation for domain constraints
- Statistical validation for outlier detection
- Cross-table validation for referential integrity

#### Data Cleansing
Automated cleansing improves data quality:
- Standardization of formats (phone numbers, addresses)
- Duplicate detection and resolution
- Missing value imputation where appropriate
- Data type conversions and formatting

#### Business Logic Application
Complex business rules are applied during transformation:
- Customer segmentation based on behavior
- Product categorization and hierarchy management
- Calculated fields and derived metrics
- Time-based calculations and aging

### Load Phase

The load phase moves transformed data into the target data warehouse:

#### Staging Load
Data is first loaded into staging tables:
- Provides recovery point for failed loads
- Enables data validation before final load
- Supports parallel processing of multiple tables
- Maintains audit trail of all loads

#### Dimension Loading
Dimension tables are loaded using SCD patterns:
- Type 1 SCD for current state dimensions
- Type 2 SCD for historical tracking where needed
- Surrogate key management for stable references
- Business key validation and duplicate handling

#### Fact Loading
Fact tables are loaded with referential integrity validation:
- Foreign key validation against dimension tables
- Measure validation for business rules
- Duplicate detection and handling
- Aggregation validation for consistency

## Advanced SQL Techniques

### Window Functions

Window functions provide powerful analytical capabilities without requiring complex self-joins or subqueries.

#### Ranking Functions
Ranking functions assign ranks to rows based on specified criteria:

```sql
-- Customer lifetime value ranking
SELECT 
    customer_id,
    lifetime_value,
    ROW_NUMBER() OVER (ORDER BY lifetime_value DESC) as ltv_rank,
    RANK() OVER (ORDER BY lifetime_value DESC) as ltv_rank_with_ties,
    DENSE_RANK() OVER (ORDER BY lifetime_value DESC) as ltv_dense_rank,
    NTILE(10) OVER (ORDER BY lifetime_value DESC) as ltv_decile
FROM customer_metrics;
```

The ROW_NUMBER() function assigns unique sequential numbers, while RANK() and DENSE_RANK() handle ties differently. NTILE() divides the result set into specified number of groups.

#### Aggregate Window Functions
Aggregate window functions perform calculations across row sets:

```sql
-- Running totals and moving averages
SELECT 
    order_date,
    daily_sales,
    SUM(daily_sales) OVER (
        ORDER BY order_date 
        ROWS UNBOUNDED PRECEDING
    ) as running_total,
    AVG(daily_sales) OVER (
        ORDER BY order_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as seven_day_avg
FROM daily_sales_summary;
```

#### Lag and Lead Functions
These functions access data from other rows relative to the current row:

```sql
-- Month-over-month growth calculation
SELECT 
    month,
    revenue,
    LAG(revenue) OVER (ORDER BY month) as prev_month_revenue,
    (revenue - LAG(revenue) OVER (ORDER BY month)) / 
    LAG(revenue) OVER (ORDER BY month) * 100 as mom_growth_rate
FROM monthly_revenue;
```

### Common Table Expressions (CTEs)

CTEs provide a way to create temporary named result sets that can be referenced multiple times within a query.

#### Simple CTEs
Simple CTEs replace complex subqueries with more readable code:

```sql
-- Customer order summary using CTE
WITH customer_orders AS (
    SELECT 
        customer_id,
        COUNT(*) as order_count,
        SUM(order_total) as total_spent,
        AVG(order_total) as avg_order_value
    FROM orders
    WHERE order_status = 'Completed'
    GROUP BY customer_id
)
SELECT 
    c.customer_name,
    co.order_count,
    co.total_spent,
    co.avg_order_value
FROM customers c
JOIN customer_orders co ON c.customer_id = co.customer_id;
```

#### Recursive CTEs
Recursive CTEs handle hierarchical data and complex calculations:

```sql
-- Product category hierarchy traversal
WITH RECURSIVE category_hierarchy AS (
    -- Base case: top-level categories
    SELECT 
        category_id,
        category_name,
        parent_category_id,
        0 as level,
        category_name as path
    FROM categories
    WHERE parent_category_id IS NULL
    
    UNION ALL
    
    -- Recursive case: child categories
    SELECT 
        c.category_id,
        c.category_name,
        c.parent_category_id,
        ch.level + 1,
        ch.path || ' > ' || c.category_name
    FROM categories c
    JOIN category_hierarchy ch ON c.parent_category_id = ch.category_id
)
SELECT * FROM category_hierarchy ORDER BY path;
```

#### Multiple CTEs
Complex queries can use multiple CTEs for better organization:

```sql
-- Customer segmentation with multiple CTEs
WITH customer_metrics AS (
    SELECT 
        customer_id,
        EXTRACT(DAYS FROM (CURRENT_DATE - MAX(order_date))) as recency,
        COUNT(*) as frequency,
        SUM(order_total) as monetary
    FROM orders
    GROUP BY customer_id
),
rfm_scores AS (
    SELECT 
        customer_id,
        NTILE(5) OVER (ORDER BY recency DESC) as recency_score,
        NTILE(5) OVER (ORDER BY frequency) as frequency_score,
        NTILE(5) OVER (ORDER BY monetary) as monetary_score
    FROM customer_metrics
),
customer_segments AS (
    SELECT 
        customer_id,
        CASE 
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 
            THEN 'Champions'
            WHEN recency_score >= 3 AND frequency_score >= 3 
            THEN 'Loyal Customers'
            ELSE 'Others'
        END as segment
    FROM rfm_scores
)
SELECT 
    segment,
    COUNT(*) as customer_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
FROM customer_segments
GROUP BY segment;
```

### Advanced JOIN Techniques

#### Self-Joins
Self-joins compare rows within the same table:

```sql
-- Find customers who made orders on consecutive days
SELECT DISTINCT
    o1.customer_id,
    o1.order_date as first_order,
    o2.order_date as next_order
FROM orders o1
JOIN orders o2 ON o1.customer_id = o2.customer_id
    AND o2.order_date = o1.order_date + INTERVAL '1 day';
```

#### Cross Joins with Filtering
Cross joins can be useful for generating combinations:

```sql
-- Product affinity analysis
SELECT 
    p1.product_name as product_a,
    p2.product_name as product_b,
    COUNT(*) as times_bought_together
FROM order_items oi1
JOIN order_items oi2 ON oi1.order_id = oi2.order_id 
    AND oi1.product_id < oi2.product_id
JOIN products p1 ON oi1.product_id = p1.product_id
JOIN products p2 ON oi2.product_id = p2.product_id
GROUP BY p1.product_name, p2.product_name
HAVING COUNT(*) >= 10
ORDER BY times_bought_together DESC;
```

#### Lateral Joins
Lateral joins allow subqueries to reference columns from preceding tables:

```sql
-- Top 3 products per category
SELECT 
    c.category_name,
    top_products.product_name,
    top_products.total_sales
FROM categories c
CROSS JOIN LATERAL (
    SELECT 
        p.product_name,
        SUM(oi.total_price) as total_sales
    FROM products p
    JOIN order_items oi ON p.product_id = oi.product_id
    WHERE p.category_id = c.category_id
    GROUP BY p.product_id, p.product_name
    ORDER BY total_sales DESC
    LIMIT 3
) top_products;
```

## Performance Optimization

### Query Optimization Strategies

#### Index Usage
Proper indexing is crucial for query performance:

```sql
-- Create composite index for common query patterns
CREATE INDEX idx_orders_customer_date ON orders(customer_id, order_date);

-- Create partial index for active records only
CREATE INDEX idx_products_active ON products(category) WHERE is_active = true;

-- Create covering index to avoid table lookups
CREATE INDEX idx_order_items_covering ON order_items(order_id) 
INCLUDE (product_id, quantity, unit_price);
```

#### Query Rewriting
Sometimes queries can be rewritten for better performance:

```sql
-- Instead of correlated subquery
SELECT customer_id, order_total
FROM orders o1
WHERE order_total > (
    SELECT AVG(order_total) 
    FROM orders o2 
    WHERE o2.customer_id = o1.customer_id
);

-- Use window function
SELECT customer_id, order_total
FROM (
    SELECT 
        customer_id, 
        order_total,
        AVG(order_total) OVER (PARTITION BY customer_id) as avg_total
    FROM orders
) t
WHERE order_total > avg_total;
```

#### Materialized Views
For frequently accessed aggregations:

```sql
-- Create materialized view for monthly sales summary
CREATE MATERIALIZED VIEW mv_monthly_sales AS
SELECT 
    DATE_TRUNC('month', order_date) as month,
    COUNT(*) as order_count,
    SUM(order_total) as total_revenue,
    AVG(order_total) as avg_order_value
FROM orders
WHERE order_status = 'Completed'
GROUP BY DATE_TRUNC('month', order_date);

-- Create index on materialized view
CREATE INDEX idx_mv_monthly_sales_month ON mv_monthly_sales(month);

-- Refresh materialized view
REFRESH MATERIALIZED VIEW mv_monthly_sales;
```

### Batch Processing Optimization

#### Chunked Processing
For large datasets, process data in chunks:

```python
def process_large_table(table_name, chunk_size=10000):
    offset = 0
    while True:
        query = f"""
            SELECT * FROM {table_name}
            ORDER BY id
            LIMIT {chunk_size} OFFSET {offset}
        """
        chunk = execute_query(query)
        if chunk.empty:
            break
        
        process_chunk(chunk)
        offset += chunk_size
```

#### Parallel Processing
Process independent operations in parallel:

```python
from concurrent.futures import ThreadPoolExecutor

def parallel_table_processing(tables):
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_table, table) for table in tables]
        results = [future.result() for future in futures]
    return results
```

### Memory Management

#### Connection Pooling
Efficient connection management:

```python
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

engine = create_engine(
    connection_string,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=3600
)
```

#### Streaming Results
For very large result sets:

```python
def stream_large_query(query):
    with engine.connect() as connection:
        result = connection.execution_options(stream_results=True).execute(query)
        for row in result:
            yield row
```

## Data Quality Framework

### Validation Rules

#### Schema Validation
Ensure data conforms to expected structure:

```python
def validate_schema(df, expected_columns):
    missing_columns = set(expected_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing columns: {missing_columns}")
    
    extra_columns = set(df.columns) - set(expected_columns)
    if extra_columns:
        logger.warning(f"Extra columns found: {extra_columns}")
```

#### Business Rule Validation
Validate domain-specific constraints:

```sql
-- Data quality checks
SELECT 
    'Negative Prices' as check_name,
    COUNT(*) as violations
FROM products 
WHERE price < 0

UNION ALL

SELECT 
    'Future Order Dates' as check_name,
    COUNT(*) as violations
FROM orders 
WHERE order_date > CURRENT_DATE

UNION ALL

SELECT 
    'Orphaned Order Items' as check_name,
    COUNT(*) as violations
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;
```

#### Statistical Validation
Identify outliers and anomalies:

```python
def detect_outliers(df, column, method='iqr'):
    if method == 'iqr':
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
    
    return outliers
```

### Data Cleansing

#### Standardization
Standardize data formats:

```python
def clean_phone_numbers(phone_series):
    # Remove all non-digit characters
    cleaned = phone_series.str.replace(r'[^\d]', '', regex=True)
    
    # Format as (XXX) XXX-XXXX for 10-digit numbers
    formatted = cleaned.apply(lambda x: 
        f"({x[:3]}) {x[3:6]}-{x[6:]}" if len(x) == 10 else x
    )
    
    return formatted
```

#### Duplicate Detection
Identify and handle duplicates:

```sql
-- Find potential duplicate customers
WITH customer_duplicates AS (
    SELECT 
        email,
        COUNT(*) as duplicate_count,
        STRING_AGG(customer_id::text, ', ') as customer_ids
    FROM customers
    GROUP BY email
    HAVING COUNT(*) > 1
)
SELECT * FROM customer_duplicates;
```

### Data Lineage Tracking

Track data flow through the pipeline:

```python
def log_data_lineage(source_table, target_table, transformation, record_count):
    lineage_record = {
        'source_table': source_table,
        'target_table': target_table,
        'transformation': transformation,
        'record_count': record_count,
        'timestamp': datetime.now(),
        'pipeline_run_id': get_current_run_id()
    }
    
    insert_lineage_record(lineage_record)
```

## Monitoring and Observability

### Performance Monitoring

#### Query Performance Tracking
Monitor query execution times:

```python
def monitor_query_performance(query_name, execution_time, record_count):
    performance_metrics = {
        'query_name': query_name,
        'execution_time': execution_time,
        'record_count': record_count,
        'records_per_second': record_count / execution_time if execution_time > 0 else 0,
        'timestamp': datetime.now()
    }
    
    log_performance_metrics(performance_metrics)
```

#### Resource Utilization
Monitor system resources:

```python
import psutil

def monitor_system_resources():
    return {
        'cpu_percent': psutil.cpu_percent(),
        'memory_percent': psutil.virtual_memory().percent,
        'disk_usage': psutil.disk_usage('/').percent,
        'active_connections': get_active_connection_count()
    }
```

### Error Handling and Alerting

#### Comprehensive Error Logging
Log errors with context:

```python
def log_etl_error(error, context):
    error_record = {
        'error_message': str(error),
        'error_type': type(error).__name__,
        'context': context,
        'timestamp': datetime.now(),
        'pipeline_run_id': get_current_run_id(),
        'stack_trace': traceback.format_exc()
    }
    
    logger.error(f"ETL Error: {error_record}")
    send_alert_if_critical(error_record)
```

#### Retry Logic
Implement intelligent retry mechanisms:

```python
def retry_with_backoff(func, max_retries=3, backoff_factor=2):
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            
            wait_time = backoff_factor ** attempt
            logger.warning(f"Attempt {attempt + 1} failed, retrying in {wait_time}s")
            time.sleep(wait_time)
```

### Health Checks

#### Database Health
Monitor database connectivity and performance:

```sql
-- Database health check query
SELECT 
    'Database Connection' as check_name,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM information_schema.tables
WHERE table_schema = 'public'

UNION ALL

SELECT 
    'Recent ETL Runs' as check_name,
    CASE 
        WHEN MAX(end_time) > CURRENT_TIMESTAMP - INTERVAL '24 hours' 
        THEN 'PASS' 
        ELSE 'FAIL' 
    END as status
FROM etl_log
WHERE status = 'SUCCESS';
```

#### Data Freshness
Monitor data currency:

```python
def check_data_freshness(table_name, max_age_hours=24):
    query = f"""
        SELECT MAX(created_at) as last_update
        FROM {table_name}
    """
    
    result = execute_query(query)
    last_update = result.iloc[0]['last_update']
    
    age_hours = (datetime.now() - last_update).total_seconds() / 3600
    
    return {
        'table': table_name,
        'last_update': last_update,
        'age_hours': age_hours,
        'is_fresh': age_hours <= max_age_hours
    }
```

## Troubleshooting Guide

### Common Issues and Solutions

#### Performance Issues

**Slow Query Performance**
1. Check query execution plan using EXPLAIN ANALYZE
2. Verify appropriate indexes exist
3. Consider query rewriting or optimization
4. Check for table bloat and run VACUUM if needed

```sql
-- Analyze query performance
EXPLAIN (ANALYZE, BUFFERS) 
SELECT customer_id, SUM(order_total)
FROM orders
WHERE order_date >= '2023-01-01'
GROUP BY customer_id;
```

**Memory Issues**
1. Reduce batch sizes for large operations
2. Implement streaming for very large result sets
3. Optimize connection pool settings
4. Monitor and tune garbage collection

#### Data Quality Issues

**Missing Data**
1. Check source system connectivity
2. Verify extraction queries and filters
3. Review data validation rules
4. Check for upstream data issues

**Data Inconsistencies**
1. Verify transformation logic
2. Check for race conditions in parallel processing
3. Review business rules and validation
4. Implement additional data quality checks

#### Connection Issues

**Database Connection Failures**
1. Verify database server availability
2. Check connection string configuration
3. Review firewall and network settings
4. Monitor connection pool exhaustion

```python
def diagnose_connection_issues():
    try:
        # Test basic connectivity
        with engine.connect() as conn:
            result = conn.execute("SELECT 1")
            logger.info("Database connection successful")
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        
        # Additional diagnostics
        check_network_connectivity()
        check_database_server_status()
        check_connection_pool_status()
```

### Debugging Techniques

#### Logging Best Practices
Implement comprehensive logging:

```python
# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

# Log with context
logger.info("Processing table", extra={
    'table_name': table_name,
    'record_count': record_count,
    'processing_time': processing_time
})
```

#### Data Sampling for Testing
Use data sampling for development and testing:

```sql
-- Sample data for testing
SELECT * FROM large_table
TABLESAMPLE BERNOULLI(1)  -- 1% sample
LIMIT 10000;
```

#### Incremental Debugging
Debug incrementally by processing smaller datasets:

```python
def debug_transformation(data, debug_mode=False):
    if debug_mode:
        # Process only first 100 records
        data = data.head(100)
        logger.info(f"Debug mode: processing {len(data)} records")
    
    return apply_transformation(data)
```

This technical guide provides comprehensive coverage of the SQL ETL pipeline's architecture, implementation details, and operational considerations. The advanced SQL techniques demonstrated here showcase modern data engineering practices essential for building scalable, maintainable data processing systems.

