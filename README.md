# sql-etl-pipeline

## **Data Sources**
- Source 1: [CSV file from X dataset]  
- Source 2: [API endpoint: https://api.example.com/data]  
- Source 3: [PostgreSQL database table `raw_orders`]  

## **Transformations Needed**
1. Clean missing values in customer data.  
2. Standardize date formats.  
3. Aggregate sales by region.  

## **Destination**
- Target: PostgreSQL database (`analytics` schema)  
- Tables: `dim_customers`, `fact_orders`  

## **Tools**
- SQL: PostgreSQL  
- Orchestration: GitHub Actions (or Apache Airflow)  
- Testing: `pgTAP` (for SQL unit tests)  
