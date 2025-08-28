"""
SQL Query Executor Module
Executes complex SQL queries and integrates them into the ETL process
"""

import logging
import pandas as pd
import os
from typing import Dict, List, Any, Optional
from datetime import datetime
import time

from database_manager import DatabaseManager
from logger_config import log_performance

class SQLQueryExecutor:
    """Executes SQL queries and manages query results"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.logger = logging.getLogger(__name__)
        self.db_manager = db_manager
        
        # Load SQL queries from files
        self.queries = self._load_sql_queries()
        
    def _load_sql_queries(self) -> Dict[str, str]:
        """Load SQL queries from files"""
        queries = {}
        sql_dir = os.path.join(os.path.dirname(__file__), '..', 'sql')
        
        # Load complex queries
        complex_queries_file = os.path.join(sql_dir, 'complex_queries.sql')
        if os.path.exists(complex_queries_file):
            queries.update(self._parse_sql_file(complex_queries_file))
        
        # Load ETL transformation queries
        etl_queries_file = os.path.join(sql_dir, 'etl_transformations.sql')
        if os.path.exists(etl_queries_file):
            queries.update(self._parse_sql_file(etl_queries_file))
        
        self.logger.info(f"Loaded {len(queries)} SQL queries")
        return queries
    
    def _parse_sql_file(self, file_path: str) -> Dict[str, str]:
        """Parse SQL file and extract named queries"""
        queries = {}
        
        try:
            with open(file_path, 'r') as file:
                content = file.read()
            
            # Split by comment blocks that start with query names
            sections = content.split('-- =====================================================')
            
            for section in sections:
                if section.strip():
                    lines = section.strip().split('\n')
                    if len(lines) > 1:
                        # Extract query name from first comment line
                        first_line = lines[0].strip()
                        if first_line.startswith('--'):
                            query_name = first_line.replace('--', '').strip().lower().replace(' ', '_')
                            
                            # Extract SQL content (skip comment lines)
                            sql_lines = []
                            for line in lines[1:]:
                                if not line.strip().startswith('--') or 'SELECT' in line or 'WITH' in line:
                                    sql_lines.append(line)
                            
                            if sql_lines:
                                queries[query_name] = '\n'.join(sql_lines).strip()
            
        except Exception as e:
            self.logger.error(f"Error parsing SQL file {file_path}: {str(e)}")
        
        return queries
    
    def execute_customer_analytics(self) -> pd.DataFrame:
        """Execute customer analytics query with window functions"""
        query_name = "customer_analytics_with_window_functions"
        
        if query_name not in self.queries:
            # Fallback query if not found in file
            query = """
            WITH customer_metrics AS (
                SELECT 
                    c.customer_id,
                    CONCAT(c.first_name, ' ', c.last_name) as full_name,
                    c.customer_segment,
                    c.registration_date,
                    COUNT(o.order_id) as total_orders,
                    COALESCE(SUM(o.order_total), 0) as lifetime_value,
                    COALESCE(AVG(o.order_total), 0) as avg_order_value,
                    MAX(o.order_date) as last_order_date,
                    MIN(o.order_date) as first_order_date
                FROM source_ecommerce.customers c
                LEFT JOIN source_ecommerce.orders o ON c.customer_id = o.customer_id
                WHERE o.order_status NOT IN ('Cancelled') OR o.order_status IS NULL
                GROUP BY c.customer_id, c.first_name, c.last_name, c.customer_segment, c.registration_date
            ),
            customer_rankings AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (ORDER BY lifetime_value DESC) as ltv_rank,
                    RANK() OVER (PARTITION BY customer_segment ORDER BY lifetime_value DESC) as segment_rank,
                    NTILE(10) OVER (ORDER BY lifetime_value DESC) as ltv_decile,
                    AVG(lifetime_value) OVER (
                        PARTITION BY customer_segment 
                        ORDER BY registration_date 
                        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
                    ) as rolling_avg_ltv
                FROM customer_metrics
            )
            SELECT 
                customer_id,
                full_name,
                customer_segment,
                total_orders,
                ROUND(lifetime_value, 2) as lifetime_value,
                ROUND(avg_order_value, 2) as avg_order_value,
                ltv_rank,
                segment_rank,
                ltv_decile,
                CASE 
                    WHEN ltv_decile <= 2 THEN 'Top 20%'
                    WHEN ltv_decile <= 5 THEN 'Middle 30%'
                    ELSE 'Bottom 50%'
                END as customer_tier,
                ROUND(rolling_avg_ltv, 2) as rolling_avg_ltv,
                last_order_date,
                first_order_date
            FROM customer_rankings
            ORDER BY ltv_rank
            LIMIT 100
            """
        else:
            query = self.queries[query_name]
        
        start_time = time.time()
        try:
            result = self.db_manager.execute_query(query, database='source')
            duration = time.time() - start_time
            log_performance("Customer Analytics Query", duration, len(result))
            
            self.logger.info(f"Customer analytics query executed successfully: {len(result)} records")
            return result
            
        except Exception as e:
            self.logger.error(f"Customer analytics query failed: {str(e)}")
            raise
    
    def execute_product_performance_analysis(self) -> pd.DataFrame:
        """Execute product performance analysis with complex joins"""
        query = """
        WITH product_sales AS (
            SELECT 
                p.product_id,
                p.product_name,
                p.category,
                p.subcategory,
                p.brand,
                p.price,
                p.cost,
                (p.price - p.cost) as profit_per_unit,
                COUNT(oi.order_item_id) as times_ordered,
                COALESCE(SUM(oi.quantity), 0) as total_quantity_sold,
                COALESCE(SUM(oi.total_price), 0) as total_revenue,
                COALESCE(SUM(oi.quantity * p.cost), 0) as total_cost,
                COALESCE(AVG(oi.quantity), 0) as avg_quantity_per_order,
                MAX(o.order_date) as last_sold_date,
                MIN(o.order_date) as first_sold_date
            FROM source_ecommerce.products p
            LEFT JOIN source_ecommerce.order_items oi ON p.product_id = oi.product_id
            LEFT JOIN source_ecommerce.orders o ON oi.order_id = o.order_id
            WHERE p.is_active = TRUE AND (o.order_status NOT IN ('Cancelled') OR o.order_status IS NULL)
            GROUP BY p.product_id, p.product_name, p.category, p.subcategory, p.brand, p.price, p.cost
        ),
        category_metrics AS (
            SELECT 
                category,
                COUNT(*) as products_in_category,
                AVG(total_revenue) as avg_revenue_per_product,
                SUM(total_revenue) as category_total_revenue
            FROM product_sales
            GROUP BY category
        )
        SELECT 
            ps.product_id,
            ps.product_name,
            ps.category,
            ps.subcategory,
            ps.brand,
            ROUND(ps.price, 2) as price,
            ROUND(ps.profit_per_unit, 2) as profit_per_unit,
            ps.times_ordered,
            ps.total_quantity_sold,
            ROUND(ps.total_revenue, 2) as total_revenue,
            ROUND(ps.total_revenue - ps.total_cost, 2) as total_profit,
            ROUND(ps.avg_quantity_per_order, 2) as avg_quantity_per_order,
            ROUND(cm.category_total_revenue, 2) as category_total_revenue,
            ROUND(
                CASE 
                    WHEN cm.category_total_revenue > 0 THEN 
                        ps.total_revenue / cm.category_total_revenue * 100
                    ELSE 0
                END, 2
            ) as category_revenue_share,
            CASE 
                WHEN ps.total_quantity_sold = 0 THEN 'No Sales'
                WHEN ps.last_sold_date < CURRENT_DATE - INTERVAL '30 days' THEN 'Slow Moving'
                ELSE 'Active'
            END as product_status,
            ps.last_sold_date,
            ps.first_sold_date
        FROM product_sales ps
        JOIN category_metrics cm ON ps.category = cm.category
        ORDER BY ps.total_revenue DESC
        LIMIT 100
        """
        
        start_time = time.time()
        try:
            result = self.db_manager.execute_query(query, database='source')
            duration = time.time() - start_time
            log_performance("Product Performance Analysis", duration, len(result))
            
            self.logger.info(f"Product performance analysis executed successfully: {len(result)} records")
            return result
            
        except Exception as e:
            self.logger.error(f"Product performance analysis failed: {str(e)}")
            raise
    
    def execute_sales_trend_analysis(self) -> pd.DataFrame:
        """Execute sales trend analysis with window functions"""
        query = """
        WITH monthly_sales AS (
            SELECT 
                EXTRACT(YEAR FROM o.order_date) as year,
                EXTRACT(MONTH FROM o.order_date) as month,
                DATE_TRUNC('month', o.order_date) as month_start,
                COUNT(DISTINCT o.order_id) as orders_count,
                COUNT(DISTINCT o.customer_id) as unique_customers,
                SUM(o.order_total) as total_revenue,
                AVG(o.order_total) as avg_order_value,
                SUM(oi.quantity) as total_items_sold
            FROM source_ecommerce.orders o
            JOIN source_ecommerce.order_items oi ON o.order_id = oi.order_id
            WHERE o.order_status NOT IN ('Cancelled')
            GROUP BY EXTRACT(YEAR FROM o.order_date), EXTRACT(MONTH FROM o.order_date), DATE_TRUNC('month', o.order_date)
        ),
        sales_with_trends AS (
            SELECT 
                year,
                month,
                month_start,
                orders_count,
                unique_customers,
                total_revenue,
                avg_order_value,
                total_items_sold,
                -- Month-over-month growth
                LAG(total_revenue) OVER (ORDER BY year, month) as prev_month_revenue,
                CASE 
                    WHEN LAG(total_revenue) OVER (ORDER BY year, month) > 0 THEN
                        (total_revenue - LAG(total_revenue) OVER (ORDER BY year, month)) / 
                        LAG(total_revenue) OVER (ORDER BY year, month) * 100
                    ELSE NULL
                END as mom_growth_rate,
                -- Rolling averages
                AVG(total_revenue) OVER (
                    ORDER BY year, month 
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as rolling_3month_avg,
                -- Cumulative metrics
                SUM(total_revenue) OVER (
                    PARTITION BY year 
                    ORDER BY month 
                    ROWS UNBOUNDED PRECEDING
                ) as ytd_revenue,
                -- Ranking
                RANK() OVER (ORDER BY total_revenue DESC) as revenue_rank
            FROM monthly_sales
        )
        SELECT 
            year,
            month,
            TO_CHAR(month_start, 'Month YYYY') as month_name,
            orders_count,
            unique_customers,
            ROUND(total_revenue, 2) as total_revenue,
            ROUND(avg_order_value, 2) as avg_order_value,
            total_items_sold,
            ROUND(mom_growth_rate, 2) as mom_growth_rate,
            ROUND(rolling_3month_avg, 2) as rolling_3month_avg,
            ROUND(ytd_revenue, 2) as ytd_revenue,
            revenue_rank,
            CASE 
                WHEN mom_growth_rate > 10 THEN 'High Growth'
                WHEN mom_growth_rate > 0 THEN 'Growth'
                WHEN mom_growth_rate > -10 THEN 'Stable'
                ELSE 'Declining'
            END as growth_category
        FROM sales_with_trends
        ORDER BY year DESC, month DESC
        """
        
        start_time = time.time()
        try:
            result = self.db_manager.execute_query(query, database='source')
            duration = time.time() - start_time
            log_performance("Sales Trend Analysis", duration, len(result))
            
            self.logger.info(f"Sales trend analysis executed successfully: {len(result)} records")
            return result
            
        except Exception as e:
            self.logger.error(f"Sales trend analysis failed: {str(e)}")
            raise
    
    def execute_inventory_optimization(self) -> pd.DataFrame:
        """Execute inventory optimization analysis"""
        query = """
        WITH product_velocity AS (
            SELECT 
                p.product_id,
                p.product_name,
                p.category,
                p.price,
                COUNT(oi.order_item_id) as order_frequency,
                COALESCE(SUM(oi.quantity), 0) as total_sold_90days,
                COALESCE(AVG(oi.quantity), 0) as avg_quantity_per_order,
                MAX(o.order_date) as last_sale_date,
                COALESCE(EXTRACT(DAYS FROM (CURRENT_DATE - MAX(o.order_date))), 999) as days_since_last_sale
            FROM source_ecommerce.products p
            LEFT JOIN source_ecommerce.order_items oi ON p.product_id = oi.product_id
            LEFT JOIN source_ecommerce.orders o ON oi.order_id = o.order_id
            WHERE (o.order_date >= CURRENT_DATE - INTERVAL '90 days' OR o.order_date IS NULL)
               AND (o.order_status NOT IN ('Cancelled') OR o.order_status IS NULL)
            GROUP BY p.product_id, p.product_name, p.category, p.price
        ),
        inventory_summary AS (
            SELECT 
                i.product_id,
                SUM(i.quantity_on_hand) as total_stock,
                SUM(i.quantity_reserved) as total_reserved,
                SUM(i.quantity_on_hand - i.quantity_reserved) as available_stock,
                AVG(i.reorder_level) as avg_reorder_level,
                COUNT(DISTINCT i.warehouse_location) as warehouse_count
            FROM source_ecommerce.inventory i
            GROUP BY i.product_id
        )
        SELECT 
            pv.product_id,
            pv.product_name,
            pv.category,
            ROUND(pv.price, 2) as price,
            pv.order_frequency,
            pv.total_sold_90days,
            ROUND(
                CASE 
                    WHEN pv.total_sold_90days > 0 THEN pv.total_sold_90days / 90.0
                    ELSE 0
                END, 2
            ) as daily_sales_velocity,
            COALESCE(ist.total_stock, 0) as total_stock,
            COALESCE(ist.available_stock, 0) as available_stock,
            ROUND(
                CASE 
                    WHEN pv.total_sold_90days > 0 AND ist.available_stock > 0 THEN 
                        ist.available_stock / (pv.total_sold_90days / 90.0)
                    ELSE NULL
                END, 1
            ) as days_of_inventory,
            COALESCE(ist.avg_reorder_level, 0) as avg_reorder_level,
            COALESCE(ist.warehouse_count, 0) as warehouse_count,
            CASE 
                WHEN COALESCE(ist.available_stock, 0) <= 0 THEN 'Out of Stock'
                WHEN COALESCE(ist.available_stock, 0) <= COALESCE(ist.avg_reorder_level, 0) THEN 'Low Stock'
                WHEN pv.days_since_last_sale > 90 THEN 'Slow Moving'
                WHEN COALESCE(ist.available_stock, 0) > (pv.total_sold_90days * 2) AND pv.total_sold_90days > 0 THEN 'Overstock'
                ELSE 'Normal'
            END as stock_status,
            pv.days_since_last_sale,
            CASE 
                WHEN COALESCE(ist.available_stock, 0) <= 0 AND pv.total_sold_90days > 0 THEN 'URGENT: Restock immediately'
                WHEN COALESCE(ist.available_stock, 0) <= COALESCE(ist.avg_reorder_level, 0) AND pv.total_sold_90days > 0 THEN 'Reorder soon'
                WHEN COALESCE(ist.available_stock, 0) > (pv.total_sold_90days * 2) AND pv.total_sold_90days > 0 THEN 'Reduce inventory'
                WHEN pv.days_since_last_sale > 90 THEN 'Review product performance'
                ELSE 'Monitor regularly'
            END as recommendation
        FROM product_velocity pv
        LEFT JOIN inventory_summary ist ON pv.product_id = ist.product_id
        ORDER BY 
            CASE 
                WHEN COALESCE(ist.available_stock, 0) <= 0 AND pv.total_sold_90days > 0 THEN 1
                WHEN COALESCE(ist.available_stock, 0) <= COALESCE(ist.avg_reorder_level, 0) AND pv.total_sold_90days > 0 THEN 2
                ELSE 3
            END,
            pv.total_sold_90days DESC
        LIMIT 100
        """
        
        start_time = time.time()
        try:
            result = self.db_manager.execute_query(query, database='source')
            duration = time.time() - start_time
            log_performance("Inventory Optimization Analysis", duration, len(result))
            
            self.logger.info(f"Inventory optimization analysis executed successfully: {len(result)} records")
            return result
            
        except Exception as e:
            self.logger.error(f"Inventory optimization analysis failed: {str(e)}")
            raise
    
    def execute_customer_segmentation(self) -> pd.DataFrame:
        """Execute RFM customer segmentation analysis"""
        query = """
        WITH customer_rfm AS (
            SELECT 
                c.customer_id,
                CONCAT(c.first_name, ' ', c.last_name) as full_name,
                c.customer_segment,
                -- Recency: Days since last order
                COALESCE(EXTRACT(DAYS FROM (CURRENT_DATE - MAX(o.order_date))), 999) as recency_days,
                -- Frequency: Number of orders
                COUNT(DISTINCT o.order_id) as frequency,
                -- Monetary: Total spent
                COALESCE(SUM(o.order_total), 0) as monetary_value,
                COALESCE(AVG(o.order_total), 0) as avg_order_value,
                MAX(o.order_date) as last_order_date,
                MIN(o.order_date) as first_order_date
            FROM source_ecommerce.customers c
            LEFT JOIN source_ecommerce.orders o ON c.customer_id = o.customer_id
            WHERE o.order_status NOT IN ('Cancelled') OR o.order_status IS NULL
            GROUP BY c.customer_id, c.first_name, c.last_name, c.customer_segment
        ),
        rfm_scores AS (
            SELECT 
                *,
                -- RFM Scores (1-5 scale, 5 being best)
                CASE 
                    WHEN recency_days <= 30 THEN 5
                    WHEN recency_days <= 60 THEN 4
                    WHEN recency_days <= 90 THEN 3
                    WHEN recency_days <= 180 THEN 2
                    ELSE 1
                END as recency_score,
                
                CASE 
                    WHEN frequency >= 10 THEN 5
                    WHEN frequency >= 5 THEN 4
                    WHEN frequency >= 3 THEN 3
                    WHEN frequency >= 2 THEN 2
                    ELSE 1
                END as frequency_score,
                
                CASE 
                    WHEN monetary_value >= 1000 THEN 5
                    WHEN monetary_value >= 500 THEN 4
                    WHEN monetary_value >= 200 THEN 3
                    WHEN monetary_value >= 100 THEN 2
                    ELSE 1
                END as monetary_score
            FROM customer_rfm
        )
        SELECT 
            customer_id,
            full_name,
            customer_segment as original_segment,
            recency_days,
            frequency,
            ROUND(monetary_value, 2) as monetary_value,
            ROUND(avg_order_value, 2) as avg_order_value,
            recency_score,
            frequency_score,
            monetary_score,
            (recency_score + frequency_score + monetary_score) as rfm_total,
            CONCAT(recency_score, frequency_score, monetary_score) as rfm_string,
            CASE 
                WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
                WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers'
                WHEN recency_score >= 4 AND frequency_score <= 2 THEN 'New Customers'
                WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score <= 2 THEN 'Potential Loyalists'
                WHEN recency_score >= 3 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'Big Spenders'
                WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'At Risk'
                WHEN recency_score <= 2 AND frequency_score >= 2 AND monetary_score <= 2 THEN 'Cannot Lose Them'
                WHEN recency_score <= 2 AND frequency_score <= 2 THEN 'Lost Customers'
                ELSE 'Others'
            END as customer_segment_rfm,
            last_order_date,
            first_order_date
        FROM rfm_scores
        ORDER BY rfm_total DESC, monetary_value DESC
        LIMIT 100
        """
        
        start_time = time.time()
        try:
            result = self.db_manager.execute_query(query, database='source')
            duration = time.time() - start_time
            log_performance("Customer Segmentation Analysis", duration, len(result))
            
            self.logger.info(f"Customer segmentation analysis executed successfully: {len(result)} records")
            return result
            
        except Exception as e:
            self.logger.error(f"Customer segmentation analysis failed: {str(e)}")
            raise
    
    def execute_all_analytics(self) -> Dict[str, pd.DataFrame]:
        """Execute all analytics queries and return results"""
        self.logger.info("Executing all analytics queries...")
        
        results = {}
        
        try:
            results['customer_analytics'] = self.execute_customer_analytics()
            results['product_performance'] = self.execute_product_performance_analysis()
            results['sales_trends'] = self.execute_sales_trend_analysis()
            results['inventory_optimization'] = self.execute_inventory_optimization()
            results['customer_segmentation'] = self.execute_customer_segmentation()
            
            self.logger.info("All analytics queries executed successfully")
            
        except Exception as e:
            self.logger.error(f"Error executing analytics queries: {str(e)}")
            raise
        
        return results
    
    def save_results_to_csv(self, results: Dict[str, pd.DataFrame], output_dir: str):
        """Save query results to CSV files"""
        os.makedirs(output_dir, exist_ok=True)
        
        for name, df in results.items():
            file_path = os.path.join(output_dir, f'{name}_results.csv')
            df.to_csv(file_path, index=False)
            self.logger.info(f"Saved {name} results to {file_path} ({len(df)} records)")

if __name__ == "__main__":
    # Example usage
    from database_manager import DatabaseManager
    from logger_config import setup_logging
    
    setup_logging()
    
    db_manager = DatabaseManager()
    query_executor = SQLQueryExecutor(db_manager)
    
    # Execute all analytics
    results = query_executor.execute_all_analytics()
    
    # Save results
    query_executor.save_results_to_csv(results, 'data/analytics_results')
    
    print("Analytics queries completed successfully!")

