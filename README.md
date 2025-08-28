# SQL ETL Pipeline

A comprehensive, production-ready SQL ETL (Extract, Transform, Load) pipeline built with Python and PostgreSQL, demonstrating advanced SQL techniques including JOINs, CTEs (Common Table Expressions), and window functions for data analytics and business intelligence.

## 🎯 Project Overview

This project implements a complete ETL pipeline for an e-commerce analytics system, showcasing modern data engineering practices and advanced SQL capabilities. The pipeline extracts data from operational systems, transforms it using sophisticated SQL queries, and loads it into a data warehouse optimized for analytical workloads.

### Key Features

- **Advanced SQL Techniques**: Demonstrates complex JOINs, CTEs, window functions, and analytical queries
- **Production-Ready Architecture**: Modular design with proper error handling, logging, and monitoring
- **Data Quality Validation**: Comprehensive data validation and cleansing mechanisms
- **Incremental Loading**: Efficient incremental data processing to handle large datasets
- **Performance Optimization**: Optimized queries, indexing strategies, and materialized views
- **Comprehensive Analytics**: Customer segmentation, sales trends, inventory optimization, and more
- **Realistic Sample Data**: Generated sample e-commerce data for testing and demonstration

## 🏗️ Architecture

The ETL pipeline follows a modern three-tier architecture:

### Source Layer
- **E-commerce Operational Database**: Contains transactional data (customers, products, orders, inventory)
- **Multiple Data Sources**: Designed to handle various input formats and systems

### Transformation Layer
- **Staging Area**: Temporary storage for raw extracted data
- **Data Validation**: Quality checks and data cleansing processes
- **Business Logic**: Complex transformations using advanced SQL techniques

### Target Layer
- **Data Warehouse**: Star schema with dimension and fact tables
- **Analytics Views**: Pre-computed aggregations and analytical datasets
- **Reporting Layer**: Optimized for business intelligence and reporting tools

## 📊 Database Schema

### Source System (E-commerce)
```
source_ecommerce/
├── customers          # Customer master data
├── products           # Product catalog
├── orders             # Order headers
├── order_items        # Order line items
└── inventory          # Stock levels by warehouse
```

### Data Warehouse (Analytics)
```
dw_analytics/
├── dim_customers      # Customer dimension
├── dim_products       # Product dimension
├── dim_date          # Date dimension
├── fact_sales        # Sales transactions fact table
└── fact_inventory    # Inventory snapshots fact table
```

## 🚀 Getting Started

### Prerequisites

- Python 3.8 or higher
- PostgreSQL 12 or higher
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/sql-etl-pipeline.git
   cd sql-etl-pipeline
   ```

2. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your database connection details
   ```

4. **Initialize the database**
   ```bash
   python main.py setup
   ```

5. **Generate sample data**
   ```bash
   python src/sample_data_generator.py
   ```

### Configuration

Edit the `.env` file with your database connection details:

```env
# Source Database
SOURCE_DB_HOST=localhost
SOURCE_DB_PORT=5432
SOURCE_DB_NAME=ecommerce_source
SOURCE_DB_USER=postgres
SOURCE_DB_PASSWORD=your_password

# Target Database
TARGET_DB_HOST=localhost
TARGET_DB_PORT=5432
TARGET_DB_NAME=analytics_dw
TARGET_DB_USER=postgres
TARGET_DB_PASSWORD=your_password
```

## 🔧 Usage

### Running the ETL Pipeline

**Full Pipeline Execution**
```bash
python main.py run
```

**Process Specific Tables**
```bash
python main.py run --tables customers products orders
```

**Incremental Processing**
```bash
python main.py incremental --table customers
```

**Test Database Connections**
```bash
python main.py test
```

**Check Pipeline Status**
```bash
python main.py status
```

### Command Line Options

- `run`: Execute the complete ETL pipeline
- `test`: Test database connections
- `incremental`: Run incremental ETL for a specific table
- `status`: Show pipeline status and statistics
- `setup`: Initialize database schemas and tables

## 📈 Advanced SQL Features

This project demonstrates sophisticated SQL techniques essential for modern data analytics:

### 1. Window Functions
```sql
-- Customer lifetime value ranking with window functions
SELECT 
    customer_id,
    lifetime_value,
    ROW_NUMBER() OVER (ORDER BY lifetime_value DESC) as ltv_rank,
    NTILE(10) OVER (ORDER BY lifetime_value DESC) as ltv_decile,
    AVG(lifetime_value) OVER (
        PARTITION BY customer_segment 
        ORDER BY registration_date 
        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
    ) as rolling_avg_ltv
FROM customer_metrics;
```

### 2. Common Table Expressions (CTEs)
```sql
-- Complex customer segmentation using multiple CTEs
WITH customer_rfm AS (
    SELECT customer_id, recency_days, frequency, monetary_value
    FROM customer_orders
),
rfm_scores AS (
    SELECT *, 
           NTILE(5) OVER (ORDER BY recency_days DESC) as recency_score,
           NTILE(5) OVER (ORDER BY frequency) as frequency_score,
           NTILE(5) OVER (ORDER BY monetary_value) as monetary_score
    FROM customer_rfm
)
SELECT customer_id, 
       CASE WHEN recency_score >= 4 AND frequency_score >= 4 
            THEN 'Champions' ELSE 'Others' END as segment
FROM rfm_scores;
```

### 3. Advanced JOINs
```sql
-- Multi-table joins with complex business logic
SELECT p.product_name, c.category_revenue_share, i.stock_status
FROM products p
JOIN (SELECT category, SUM(revenue) as category_revenue_share 
      FROM product_sales GROUP BY category) c ON p.category = c.category
LEFT JOIN inventory_summary i ON p.product_id = i.product_id
WHERE p.is_active = TRUE;
```

## 📊 Analytics Capabilities

### Customer Analytics
- **Lifetime Value Analysis**: Customer ranking and segmentation by total spend
- **RFM Segmentation**: Recency, Frequency, Monetary analysis for targeted marketing
- **Cohort Analysis**: Customer retention and behavior tracking over time
- **Churn Prediction**: Identification of at-risk customers

### Product Performance
- **Sales Velocity**: Product performance metrics and trends
- **Category Analysis**: Cross-category performance comparison
- **Profitability Analysis**: Margin analysis and profit optimization
- **Inventory Optimization**: Stock level recommendations and reorder points

### Sales Trends
- **Time Series Analysis**: Monthly, quarterly, and yearly sales trends
- **Seasonality Detection**: Seasonal pattern identification
- **Growth Metrics**: Month-over-month and year-over-year growth rates
- **Forecasting**: Predictive analytics for future sales

## 🔍 Data Quality & Validation

The pipeline includes comprehensive data quality checks:

- **Schema Validation**: Ensures data conforms to expected structure
- **Business Rule Validation**: Validates business logic constraints
- **Data Completeness**: Checks for missing required fields
- **Data Consistency**: Cross-table referential integrity validation
- **Outlier Detection**: Statistical analysis to identify anomalies

## 📁 Project Structure

```
sql-etl-pipeline/
├── src/                          # Source code
│   ├── etl_pipeline.py          # Main ETL orchestrator
│   ├── database_manager.py      # Database connection management
│   ├── data_validator.py        # Data quality validation
│   ├── sql_query_executor.py    # Advanced SQL query execution
│   ├── sample_data_generator.py # Sample data generation
│   └── logger_config.py         # Logging configuration
├── sql/                          # SQL scripts
│   ├── schema.sql               # Database schema definitions
│   ├── complex_queries.sql      # Advanced analytical queries
│   └── etl_transformations.sql  # ETL transformation logic
├── config/                       # Configuration files
│   └── config.py                # Application configuration
├── data/                         # Data files
│   ├── sample_*.csv             # Generated sample data
│   └── analytics_results/       # Query results
├── logs/                         # Log files
├── tests/                        # Unit tests
├── docs/                         # Additional documentation
├── main.py                       # Command-line interface
├── requirements.txt              # Python dependencies
├── .env.example                  # Environment variables template
└── README.md                     # This file
```

## 🧪 Testing

Run the test suite to validate pipeline functionality:

```bash
# Run all tests
python -m pytest tests/

# Run specific test categories
python -m pytest tests/test_data_validation.py
python -m pytest tests/test_etl_pipeline.py

# Run with coverage
python -m pytest --cov=src tests/
```

## 📊 Performance Optimization

The pipeline includes several performance optimization techniques:

### Indexing Strategy
- Primary and foreign key indexes
- Composite indexes for common query patterns
- Partial indexes for filtered queries

### Query Optimization
- Materialized views for frequently accessed aggregations
- Query plan analysis and optimization
- Batch processing for large datasets

### Resource Management
- Connection pooling for database connections
- Parallel processing for independent operations
- Memory-efficient data processing

## 🔧 Monitoring & Logging

Comprehensive monitoring and logging capabilities:

- **Performance Metrics**: Query execution times and throughput
- **Error Tracking**: Detailed error logging and alerting
- **Data Lineage**: Track data flow through the pipeline
- **Audit Trail**: Complete history of data transformations

## 🚀 Deployment

### Local Development
```bash
# Start the pipeline
python main.py run

# Monitor logs
tail -f logs/etl_pipeline.log
```

### Production Deployment
1. Set up production database connections
2. Configure environment variables
3. Set up scheduled execution (cron, Airflow, etc.)
4. Configure monitoring and alerting

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines
- Follow PEP 8 style guidelines
- Add unit tests for new functionality
- Update documentation for any changes
- Ensure all tests pass before submitting

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Mode Analytics SQL Tutorial**: Inspiration for advanced SQL techniques
- **PostgreSQL Documentation**: Comprehensive SQL reference
- **Python Community**: Excellent libraries and tools for data processing

## 📞 Support

For questions, issues, or contributions:

- **GitHub Issues**: [Create an issue](https://github.com/okeafeez/sql-etl-pipeline/issues)
- **Documentation**: Check the `docs/` directory for detailed guides
- **Email**: adeshinaoke@gmail.com

---

**Built with ❤️ by [Your Name]**

*This project demonstrates production-ready ETL pipeline development with advanced SQL techniques for modern data analytics and business intelligence.*

