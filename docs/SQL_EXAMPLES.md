# Advanced SQL Examples and Techniques

This document provides comprehensive examples of advanced SQL techniques used in the ETL pipeline, demonstrating JOINs, CTEs, window functions, and complex analytical queries.

## Table of Contents
1. [Window Functions](#window-functions)
2. [Common Table Expressions (CTEs)](#common-table-expressions-ctes)
3. [Advanced JOIN Techniques](#advanced-join-techniques)
4. [Analytical Queries](#analytical-queries)
5. [Performance Optimization](#performance-optimization)

## Window Functions

Window functions perform calculations across a set of table rows related to the current row, without requiring GROUP BY clauses.

### Ranking Functions

#### ROW_NUMBER(), RANK(), and DENSE_RANK()

```sql
-- Customer ranking by lifetime value
SELECT 
    customer_id,
    full_name,
    lifetime_value,
    -- ROW_NUMBER: Unique sequential numbers
    ROW_NUMBER() OVER (ORDER BY lifetime_value DESC) as row_num,
    -- RANK: Same rank for ties, gaps in sequence
    RANK() OVER (ORDER BY lifetime_value DESC) as rank_with_gaps,
    -- DENSE_RANK: Same rank for ties, no gaps
    DENSE_RANK() OVER (ORDER BY lifetime_value DESC) as dense_rank
FROM customer_metrics
ORDER BY lifetime_value DESC;
```

#### NTILE() for Percentile Analysis

```sql
-- Divide customers into quartiles based on spending
SELECT 
    customer_id,
    full_name,
    lifetime_value,
    NTILE(4) OVER (ORDER BY lifetime_value DESC) as spending_quartile,
    NTILE(10) OVER (ORDER BY lifetime_value DESC) as spending_decile,
    CASE 
        WHEN NTILE(4) OVER (ORDER BY lifetime_value DESC) = 1 THEN 'Top 25%'
        WHEN NTILE(4) OVER (ORDER BY lifetime_value DESC) = 2 THEN 'Second 25%'
        WHEN NTILE(4) OVER (ORDER BY lifetime_value DESC) = 3 THEN 'Third 25%'
        ELSE 'Bottom 25%'
    END as customer_tier
FROM customer_metrics;
```

### Aggregate Window Functions

#### Running Totals and Cumulative Calculations

```sql
-- Daily sales with running totals and moving averages
SELECT 
    sale_date,
    daily_sales,
    -- Running total from beginning
    SUM(daily_sales) OVER (
        ORDER BY sale_date 
        ROWS UNBOUNDED PRECEDING
    ) as running_total,
    -- 7-day moving average
    AVG(daily_sales) OVER (
        ORDER BY sale_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as seven_day_avg,
    -- 30-day moving sum
    SUM(daily_sales) OVER (
        ORDER BY sale_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as thirty_day_sum,
    -- Percentage of total sales
    daily_sales * 100.0 / SUM(daily_sales) OVER () as pct_of_total
FROM daily_sales_summary
ORDER BY sale_date;
```

#### Partitioned Window Functions

```sql
-- Sales analysis by category with partitioned windows
SELECT 
    category,
    product_name,
    monthly_sales,
    sale_month,
    -- Rank within each category
    RANK() OVER (
        PARTITION BY category 
        ORDER BY monthly_sales DESC
    ) as category_rank,
    -- Running total within each category
    SUM(monthly_sales) OVER (
        PARTITION BY category 
        ORDER BY sale_month 
        ROWS UNBOUNDED PRECEDING
    ) as category_running_total,
    -- Percentage of category sales
    monthly_sales * 100.0 / SUM(monthly_sales) OVER (
        PARTITION BY category
    ) as pct_of_category_sales
FROM product_monthly_sales
ORDER BY category, monthly_sales DESC;
```

### LAG and LEAD Functions

#### Time Series Analysis

```sql
-- Month-over-month and year-over-year growth analysis
SELECT 
    year,
    month,
    monthly_revenue,
    -- Previous month comparison
    LAG(monthly_revenue) OVER (ORDER BY year, month) as prev_month_revenue,
    monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY year, month) as mom_change,
    ROUND(
        (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY year, month)) * 100.0 / 
        NULLIF(LAG(monthly_revenue) OVER (ORDER BY year, month), 0), 2
    ) as mom_growth_pct,
    -- Same month previous year comparison
    LAG(monthly_revenue, 12) OVER (ORDER BY year, month) as same_month_prev_year,
    ROUND(
        (monthly_revenue - LAG(monthly_revenue, 12) OVER (ORDER BY year, month)) * 100.0 / 
        NULLIF(LAG(monthly_revenue, 12) OVER (ORDER BY year, month), 0), 2
    ) as yoy_growth_pct,
    -- Next month preview (for forecasting validation)
    LEAD(monthly_revenue) OVER (ORDER BY year, month) as next_month_revenue
FROM monthly_revenue_summary
ORDER BY year, month;
```

#### Customer Behavior Analysis

```sql
-- Customer purchase patterns with LAG/LEAD
SELECT 
    customer_id,
    order_date,
    order_total,
    -- Days since last order
    order_date - LAG(order_date) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
    ) as days_since_last_order,
    -- Order value compared to previous order
    order_total - LAG(order_total) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
    ) as order_value_change,
    -- Average time between orders (looking ahead)
    LEAD(order_date) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
    ) - order_date as days_to_next_order,
    -- Customer order sequence number
    ROW_NUMBER() OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
    ) as order_sequence
FROM orders
WHERE order_status = 'Completed'
ORDER BY customer_id, order_date;
```

### FIRST_VALUE and LAST_VALUE

```sql
-- Product lifecycle analysis
SELECT 
    product_id,
    product_name,
    sale_date,
    daily_sales,
    -- First and last sale dates for the product
    FIRST_VALUE(sale_date) OVER (
        PARTITION BY product_id 
        ORDER BY sale_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as first_sale_date,
    LAST_VALUE(sale_date) OVER (
        PARTITION BY product_id 
        ORDER BY sale_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as last_sale_date,
    -- Peak sales day and amount
    FIRST_VALUE(daily_sales) OVER (
        PARTITION BY product_id 
        ORDER BY daily_sales DESC 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as peak_daily_sales,
    FIRST_VALUE(sale_date) OVER (
        PARTITION BY product_id 
        ORDER BY daily_sales DESC 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as peak_sales_date
FROM product_daily_sales
ORDER BY product_id, sale_date;
```

## Common Table Expressions (CTEs)

CTEs provide a way to create temporary named result sets that improve query readability and enable complex logic.

### Simple CTEs

#### Customer Segmentation

```sql
-- RFM Analysis using CTEs
WITH customer_rfm AS (
    -- Calculate RFM metrics for each customer
    SELECT 
        c.customer_id,
        c.full_name,
        -- Recency: Days since last order
        COALESCE(
            EXTRACT(DAYS FROM (CURRENT_DATE - MAX(o.order_date))), 
            999
        ) as recency_days,
        -- Frequency: Number of orders
        COUNT(o.order_id) as frequency,
        -- Monetary: Total amount spent
        COALESCE(SUM(o.order_total), 0) as monetary_value
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id 
        AND o.order_status = 'Completed'
    GROUP BY c.customer_id, c.full_name
),
rfm_scores AS (
    -- Convert RFM metrics to scores (1-5 scale)
    SELECT 
        customer_id,
        full_name,
        recency_days,
        frequency,
        monetary_value,
        -- Recency score (lower days = higher score)
        CASE 
            WHEN recency_days <= 30 THEN 5
            WHEN recency_days <= 60 THEN 4
            WHEN recency_days <= 90 THEN 3
            WHEN recency_days <= 180 THEN 2
            ELSE 1
        END as recency_score,
        -- Frequency score
        CASE 
            WHEN frequency >= 10 THEN 5
            WHEN frequency >= 5 THEN 4
            WHEN frequency >= 3 THEN 3
            WHEN frequency >= 2 THEN 2
            ELSE 1
        END as frequency_score,
        -- Monetary score
        CASE 
            WHEN monetary_value >= 1000 THEN 5
            WHEN monetary_value >= 500 THEN 4
            WHEN monetary_value >= 200 THEN 3
            WHEN monetary_value >= 100 THEN 2
            ELSE 1
        END as monetary_score
    FROM customer_rfm
),
customer_segments AS (
    -- Assign customer segments based on RFM scores
    SELECT 
        customer_id,
        full_name,
        recency_score,
        frequency_score,
        monetary_score,
        CONCAT(recency_score, frequency_score, monetary_score) as rfm_string,
        CASE 
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 
                THEN 'Champions'
            WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 
                THEN 'Loyal Customers'
            WHEN recency_score >= 4 AND frequency_score <= 2 
                THEN 'New Customers'
            WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score <= 2 
                THEN 'Potential Loyalists'
            WHEN recency_score >= 3 AND frequency_score <= 2 AND monetary_score >= 3 
                THEN 'Big Spenders'
            WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 
                THEN 'At Risk'
            WHEN recency_score <= 2 AND frequency_score >= 2 AND monetary_score <= 2 
                THEN 'Cannot Lose Them'
            WHEN recency_score <= 2 AND frequency_score <= 2 
                THEN 'Lost Customers'
            ELSE 'Others'
        END as customer_segment
    FROM rfm_scores
)
-- Final result with segment analysis
SELECT 
    customer_segment,
    COUNT(*) as customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
    ROUND(AVG(recency_score), 2) as avg_recency_score,
    ROUND(AVG(frequency_score), 2) as avg_frequency_score,
    ROUND(AVG(monetary_score), 2) as avg_monetary_score
FROM customer_segments
GROUP BY customer_segment
ORDER BY customer_count DESC;
```

### Recursive CTEs

#### Organizational Hierarchy

```sql
-- Employee hierarchy traversal
WITH RECURSIVE employee_hierarchy AS (
    -- Base case: Top-level managers (no supervisor)
    SELECT 
        employee_id,
        employee_name,
        supervisor_id,
        job_title,
        0 as level,
        employee_name as hierarchy_path,
        ARRAY[employee_id] as path_array
    FROM employees
    WHERE supervisor_id IS NULL
    
    UNION ALL
    
    -- Recursive case: Employees with supervisors
    SELECT 
        e.employee_id,
        e.employee_name,
        e.supervisor_id,
        e.job_title,
        eh.level + 1,
        eh.hierarchy_path || ' > ' || e.employee_name,
        eh.path_array || e.employee_id
    FROM employees e
    JOIN employee_hierarchy eh ON e.supervisor_id = eh.employee_id
    WHERE NOT e.employee_id = ANY(eh.path_array)  -- Prevent cycles
)
SELECT 
    employee_id,
    employee_name,
    job_title,
    level,
    hierarchy_path,
    -- Count of direct reports
    (SELECT COUNT(*) FROM employees WHERE supervisor_id = eh.employee_id) as direct_reports,
    -- Count of all subordinates
    (SELECT COUNT(*) FROM employee_hierarchy WHERE hierarchy_path LIKE eh.hierarchy_path || '%' 
     AND employee_id != eh.employee_id) as total_subordinates
FROM employee_hierarchy eh
ORDER BY level, employee_name;
```

#### Product Category Hierarchy

```sql
-- Product category tree with sales rollup
WITH RECURSIVE category_tree AS (
    -- Base case: Root categories
    SELECT 
        category_id,
        category_name,
        parent_category_id,
        0 as level,
        category_name as full_path,
        ARRAY[category_id] as path_ids
    FROM product_categories
    WHERE parent_category_id IS NULL
    
    UNION ALL
    
    -- Recursive case: Child categories
    SELECT 
        pc.category_id,
        pc.category_name,
        pc.parent_category_id,
        ct.level + 1,
        ct.full_path || ' > ' || pc.category_name,
        ct.path_ids || pc.category_id
    FROM product_categories pc
    JOIN category_tree ct ON pc.parent_category_id = ct.category_id
),
category_sales AS (
    -- Calculate sales for each category level
    SELECT 
        ct.category_id,
        ct.category_name,
        ct.level,
        ct.full_path,
        -- Direct sales (products directly in this category)
        COALESCE(SUM(CASE WHEN p.category_id = ct.category_id THEN oi.total_price END), 0) as direct_sales,
        -- Total sales (including subcategories)
        COALESCE(SUM(oi.total_price), 0) as total_sales,
        COUNT(DISTINCT p.product_id) as product_count
    FROM category_tree ct
    LEFT JOIN products p ON p.category_id = ANY(ct.path_ids || ct.category_id)
    LEFT JOIN order_items oi ON p.product_id = oi.product_id
    LEFT JOIN orders o ON oi.order_id = o.order_id AND o.order_status = 'Completed'
    GROUP BY ct.category_id, ct.category_name, ct.level, ct.full_path
)
SELECT 
    category_id,
    category_name,
    level,
    full_path,
    direct_sales,
    total_sales,
    product_count,
    ROUND(total_sales * 100.0 / SUM(total_sales) OVER (), 2) as pct_of_total_sales
FROM category_sales
ORDER BY level, total_sales DESC;
```

### Multiple CTEs for Complex Analysis

#### Cohort Analysis

```sql
-- Customer cohort retention analysis
WITH customer_cohorts AS (
    -- Assign customers to cohorts based on first purchase month
    SELECT 
        customer_id,
        DATE_TRUNC('month', MIN(order_date)) as cohort_month
    FROM orders
    WHERE order_status = 'Completed'
    GROUP BY customer_id
),
customer_activities AS (
    -- Track customer activity by month
    SELECT 
        cc.customer_id,
        cc.cohort_month,
        DATE_TRUNC('month', o.order_date) as activity_month,
        SUM(o.order_total) as monthly_revenue
    FROM customer_cohorts cc
    JOIN orders o ON cc.customer_id = o.customer_id
    WHERE o.order_status = 'Completed'
    GROUP BY cc.customer_id, cc.cohort_month, DATE_TRUNC('month', o.order_date)
),
cohort_data AS (
    -- Calculate cohort metrics
    SELECT 
        cohort_month,
        activity_month,
        EXTRACT(YEAR FROM AGE(activity_month, cohort_month)) * 12 + 
        EXTRACT(MONTH FROM AGE(activity_month, cohort_month)) as period_number,
        COUNT(DISTINCT customer_id) as active_customers,
        SUM(monthly_revenue) as cohort_revenue
    FROM customer_activities
    GROUP BY cohort_month, activity_month
),
cohort_sizes AS (
    -- Get initial cohort sizes
    SELECT 
        cohort_month,
        COUNT(DISTINCT customer_id) as cohort_size
    FROM customer_cohorts
    GROUP BY cohort_month
)
SELECT 
    cd.cohort_month,
    cd.period_number,
    cd.active_customers,
    cs.cohort_size,
    ROUND(cd.active_customers * 100.0 / cs.cohort_size, 2) as retention_rate,
    cd.cohort_revenue,
    ROUND(cd.cohort_revenue / cd.active_customers, 2) as revenue_per_customer,
    -- Cumulative metrics
    SUM(cd.active_customers) OVER (
        PARTITION BY cd.cohort_month 
        ORDER BY cd.period_number
    ) as cumulative_customers,
    SUM(cd.cohort_revenue) OVER (
        PARTITION BY cd.cohort_month 
        ORDER BY cd.period_number
    ) as cumulative_revenue
FROM cohort_data cd
JOIN cohort_sizes cs ON cd.cohort_month = cs.cohort_month
WHERE cd.period_number <= 12  -- First 12 months
ORDER BY cd.cohort_month, cd.period_number;
```

## Advanced JOIN Techniques

### Self-Joins

#### Finding Related Records

```sql
-- Find customers who made multiple orders on the same day
SELECT 
    o1.customer_id,
    o1.order_date,
    COUNT(*) as orders_same_day,
    STRING_AGG(o1.order_id::text, ', ') as order_ids,
    SUM(o1.order_total) as total_amount
FROM orders o1
JOIN orders o2 ON o1.customer_id = o2.customer_id 
    AND o1.order_date::date = o2.order_date::date
    AND o1.order_id != o2.order_id
WHERE o1.order_status = 'Completed'
GROUP BY o1.customer_id, o1.order_date
HAVING COUNT(*) > 1
ORDER BY orders_same_day DESC, total_amount DESC;
```

#### Sequential Analysis

```sql
-- Analyze customer order patterns (consecutive orders)
SELECT 
    o1.customer_id,
    o1.order_id as first_order,
    o2.order_id as next_order,
    o1.order_date as first_order_date,
    o2.order_date as next_order_date,
    o2.order_date - o1.order_date as days_between_orders,
    o1.order_total as first_order_amount,
    o2.order_total as next_order_amount,
    o2.order_total - o1.order_total as amount_change,
    CASE 
        WHEN o2.order_total > o1.order_total THEN 'Increased'
        WHEN o2.order_total < o1.order_total THEN 'Decreased'
        ELSE 'Same'
    END as spending_trend
FROM orders o1
JOIN orders o2 ON o1.customer_id = o2.customer_id
WHERE o1.order_status = 'Completed' 
    AND o2.order_status = 'Completed'
    AND o2.order_date > o1.order_date
    AND NOT EXISTS (
        -- Ensure o2 is the immediate next order
        SELECT 1 FROM orders o3
        WHERE o3.customer_id = o1.customer_id
            AND o3.order_date > o1.order_date
            AND o3.order_date < o2.order_date
            AND o3.order_status = 'Completed'
    )
ORDER BY o1.customer_id, o1.order_date;
```

### Cross Joins for Combinations

#### Product Affinity Analysis

```sql
-- Market basket analysis: Products frequently bought together
SELECT 
    p1.product_name as product_a,
    p2.product_name as product_b,
    COUNT(*) as times_bought_together,
    COUNT(*) * 100.0 / (
        SELECT COUNT(DISTINCT order_id) FROM order_items
    ) as basket_penetration_pct,
    -- Calculate lift (how much more likely to buy together vs independently)
    COUNT(*) * 1.0 / (
        (SELECT COUNT(DISTINCT order_id) FROM order_items oi_a WHERE oi_a.product_id = oi1.product_id) *
        (SELECT COUNT(DISTINCT order_id) FROM order_items oi_b WHERE oi_b.product_id = oi2.product_id) /
        (SELECT COUNT(DISTINCT order_id) FROM order_items)
    ) as lift
FROM order_items oi1
JOIN order_items oi2 ON oi1.order_id = oi2.order_id 
    AND oi1.product_id < oi2.product_id  -- Avoid duplicates and self-joins
JOIN products p1 ON oi1.product_id = p1.product_id
JOIN products p2 ON oi2.product_id = p2.product_id
GROUP BY oi1.product_id, oi2.product_id, p1.product_name, p2.product_name
HAVING COUNT(*) >= 10  -- Minimum support threshold
ORDER BY times_bought_together DESC, lift DESC
LIMIT 20;
```

### Lateral Joins

#### Top N per Group

```sql
-- Top 3 selling products per category
SELECT 
    c.category_name,
    top_products.product_name,
    top_products.total_sales,
    top_products.rank_in_category
FROM product_categories c
CROSS JOIN LATERAL (
    SELECT 
        p.product_name,
        SUM(oi.total_price) as total_sales,
        RANK() OVER (ORDER BY SUM(oi.total_price) DESC) as rank_in_category
    FROM products p
    JOIN order_items oi ON p.product_id = oi.product_id
    JOIN orders o ON oi.order_id = o.order_id
    WHERE p.category_id = c.category_id
        AND o.order_status = 'Completed'
    GROUP BY p.product_id, p.product_name
    ORDER BY total_sales DESC
    LIMIT 3
) top_products
ORDER BY c.category_name, top_products.total_sales DESC;
```

#### Dynamic Calculations

```sql
-- Customer analysis with dynamic date ranges
SELECT 
    c.customer_id,
    c.full_name,
    c.registration_date,
    recent_activity.orders_last_30_days,
    recent_activity.revenue_last_30_days,
    lifetime_activity.total_orders,
    lifetime_activity.lifetime_revenue
FROM customers c
CROSS JOIN LATERAL (
    -- Recent activity (last 30 days)
    SELECT 
        COUNT(*) as orders_last_30_days,
        COALESCE(SUM(order_total), 0) as revenue_last_30_days
    FROM orders o
    WHERE o.customer_id = c.customer_id
        AND o.order_date >= CURRENT_DATE - INTERVAL '30 days'
        AND o.order_status = 'Completed'
) recent_activity
CROSS JOIN LATERAL (
    -- Lifetime activity
    SELECT 
        COUNT(*) as total_orders,
        COALESCE(SUM(order_total), 0) as lifetime_revenue
    FROM orders o
    WHERE o.customer_id = c.customer_id
        AND o.order_status = 'Completed'
) lifetime_activity
WHERE recent_activity.orders_last_30_days > 0 
    OR lifetime_activity.total_orders > 5
ORDER BY recent_activity.revenue_last_30_days DESC;
```

## Analytical Queries

### Time Series Analysis

#### Sales Trend Analysis with Seasonality

```sql
-- Comprehensive sales trend analysis
WITH daily_sales AS (
    SELECT 
        o.order_date::date as sale_date,
        EXTRACT(YEAR FROM o.order_date) as year,
        EXTRACT(MONTH FROM o.order_date) as month,
        EXTRACT(DOW FROM o.order_date) as day_of_week,
        EXTRACT(QUARTER FROM o.order_date) as quarter,
        COUNT(DISTINCT o.order_id) as order_count,
        COUNT(DISTINCT o.customer_id) as unique_customers,
        SUM(o.order_total) as daily_revenue,
        AVG(o.order_total) as avg_order_value
    FROM orders o
    WHERE o.order_status = 'Completed'
    GROUP BY o.order_date::date
),
sales_with_trends AS (
    SELECT 
        *,
        -- Moving averages
        AVG(daily_revenue) OVER (
            ORDER BY sale_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as ma_7_day,
        AVG(daily_revenue) OVER (
            ORDER BY sale_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as ma_30_day,
        -- Year-over-year comparison
        LAG(daily_revenue, 365) OVER (ORDER BY sale_date) as same_day_prev_year,
        -- Week-over-week comparison
        LAG(daily_revenue, 7) OVER (ORDER BY sale_date) as same_day_prev_week,
        -- Seasonal indicators
        AVG(daily_revenue) OVER (
            PARTITION BY EXTRACT(DOW FROM sale_date)
        ) as avg_for_day_of_week,
        AVG(daily_revenue) OVER (
            PARTITION BY EXTRACT(MONTH FROM sale_date)
        ) as avg_for_month
    FROM daily_sales
)
SELECT 
    sale_date,
    year,
    month,
    CASE day_of_week
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END as day_name,
    daily_revenue,
    ROUND(ma_7_day, 2) as seven_day_avg,
    ROUND(ma_30_day, 2) as thirty_day_avg,
    -- Growth calculations
    ROUND(
        CASE 
            WHEN same_day_prev_year > 0 THEN 
                (daily_revenue - same_day_prev_year) * 100.0 / same_day_prev_year
            ELSE NULL
        END, 2
    ) as yoy_growth_pct,
    ROUND(
        CASE 
            WHEN same_day_prev_week > 0 THEN 
                (daily_revenue - same_day_prev_week) * 100.0 / same_day_prev_week
            ELSE NULL
        END, 2
    ) as wow_growth_pct,
    -- Seasonality indicators
    ROUND(daily_revenue / NULLIF(avg_for_day_of_week, 0), 2) as day_of_week_index,
    ROUND(daily_revenue / NULLIF(avg_for_month, 0), 2) as monthly_index,
    -- Performance categories
    CASE 
        WHEN daily_revenue > ma_30_day * 1.2 THEN 'Exceptional'
        WHEN daily_revenue > ma_30_day * 1.1 THEN 'Above Average'
        WHEN daily_revenue > ma_30_day * 0.9 THEN 'Average'
        WHEN daily_revenue > ma_30_day * 0.8 THEN 'Below Average'
        ELSE 'Poor'
    END as performance_category
FROM sales_with_trends
WHERE sale_date >= CURRENT_DATE - INTERVAL '90 days'
ORDER BY sale_date DESC;
```

### Customer Lifetime Value Analysis

```sql
-- Comprehensive customer lifetime value analysis
WITH customer_orders AS (
    SELECT 
        c.customer_id,
        c.full_name,
        c.registration_date,
        c.customer_segment,
        COUNT(o.order_id) as total_orders,
        SUM(o.order_total) as lifetime_revenue,
        AVG(o.order_total) as avg_order_value,
        MIN(o.order_date) as first_order_date,
        MAX(o.order_date) as last_order_date,
        EXTRACT(DAYS FROM (MAX(o.order_date) - MIN(o.order_date))) as customer_lifespan_days
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id 
        AND o.order_status = 'Completed'
    GROUP BY c.customer_id, c.full_name, c.registration_date, c.customer_segment
),
customer_metrics AS (
    SELECT 
        *,
        -- Calculate purchase frequency
        CASE 
            WHEN customer_lifespan_days > 0 THEN 
                total_orders * 365.0 / customer_lifespan_days
            ELSE total_orders
        END as annual_purchase_frequency,
        -- Calculate customer value scores
        NTILE(5) OVER (ORDER BY lifetime_revenue) as revenue_quintile,
        NTILE(5) OVER (ORDER BY total_orders) as frequency_quintile,
        NTILE(5) OVER (ORDER BY avg_order_value) as aov_quintile,
        -- Recency calculation
        EXTRACT(DAYS FROM (CURRENT_DATE - last_order_date)) as days_since_last_order
    FROM customer_orders
),
clv_analysis AS (
    SELECT 
        *,
        -- Predicted CLV (simplified model)
        CASE 
            WHEN annual_purchase_frequency > 0 AND days_since_last_order <= 365 THEN
                avg_order_value * annual_purchase_frequency * 
                CASE customer_segment
                    WHEN 'VIP' THEN 3.0
                    WHEN 'Premium' THEN 2.5
                    WHEN 'Regular' THEN 2.0
                    ELSE 1.5
                END
            ELSE lifetime_revenue
        END as predicted_clv,
        -- Customer status
        CASE 
            WHEN total_orders = 0 THEN 'Never Purchased'
            WHEN days_since_last_order <= 30 THEN 'Active'
            WHEN days_since_last_order <= 90 THEN 'At Risk'
            WHEN days_since_last_order <= 365 THEN 'Dormant'
            ELSE 'Lost'
        END as customer_status,
        -- Value tier
        CASE 
            WHEN revenue_quintile = 5 THEN 'High Value'
            WHEN revenue_quintile >= 3 THEN 'Medium Value'
            ELSE 'Low Value'
        END as value_tier
    FROM customer_metrics
)
SELECT 
    customer_id,
    full_name,
    customer_segment,
    customer_status,
    value_tier,
    total_orders,
    ROUND(lifetime_revenue, 2) as lifetime_revenue,
    ROUND(avg_order_value, 2) as avg_order_value,
    ROUND(annual_purchase_frequency, 2) as annual_purchase_frequency,
    ROUND(predicted_clv, 2) as predicted_clv,
    days_since_last_order,
    first_order_date,
    last_order_date,
    customer_lifespan_days,
    -- Ranking within segment
    RANK() OVER (
        PARTITION BY customer_segment 
        ORDER BY predicted_clv DESC
    ) as segment_clv_rank,
    -- Overall ranking
    RANK() OVER (ORDER BY predicted_clv DESC) as overall_clv_rank
FROM clv_analysis
WHERE total_orders > 0
ORDER BY predicted_clv DESC;
```

## Performance Optimization

### Index Optimization

```sql
-- Create optimized indexes for common query patterns

-- Composite index for order analysis
CREATE INDEX idx_orders_customer_date_status 
ON orders(customer_id, order_date, order_status);

-- Partial index for active products only
CREATE INDEX idx_products_active_category 
ON products(category_id, price) 
WHERE is_active = true;

-- Covering index to avoid table lookups
CREATE INDEX idx_order_items_covering 
ON order_items(order_id) 
INCLUDE (product_id, quantity, unit_price, total_price);

-- Functional index for case-insensitive searches
CREATE INDEX idx_customers_email_lower 
ON customers(LOWER(email));

-- Index for time-based queries
CREATE INDEX idx_orders_date_trunc_month 
ON orders(DATE_TRUNC('month', order_date));
```

### Query Optimization Examples

#### Before and After Optimization

```sql
-- BEFORE: Inefficient correlated subquery
SELECT 
    customer_id,
    order_total,
    (SELECT AVG(order_total) 
     FROM orders o2 
     WHERE o2.customer_id = o1.customer_id) as customer_avg
FROM orders o1
WHERE order_total > (
    SELECT AVG(order_total) 
    FROM orders o2 
    WHERE o2.customer_id = o1.customer_id
);

-- AFTER: Optimized with window function
SELECT 
    customer_id,
    order_total,
    customer_avg
FROM (
    SELECT 
        customer_id,
        order_total,
        AVG(order_total) OVER (PARTITION BY customer_id) as customer_avg
    FROM orders
) t
WHERE order_total > customer_avg;
```

#### Materialized Views for Performance

```sql
-- Create materialized view for frequently accessed aggregations
CREATE MATERIALIZED VIEW mv_customer_summary AS
SELECT 
    c.customer_id,
    c.full_name,
    c.customer_segment,
    COUNT(o.order_id) as total_orders,
    COALESCE(SUM(o.order_total), 0) as lifetime_value,
    COALESCE(AVG(o.order_total), 0) as avg_order_value,
    MAX(o.order_date) as last_order_date,
    EXTRACT(DAYS FROM (CURRENT_DATE - MAX(o.order_date))) as days_since_last_order
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id 
    AND o.order_status = 'Completed'
GROUP BY c.customer_id, c.full_name, c.customer_segment;

-- Create indexes on materialized view
CREATE INDEX idx_mv_customer_summary_segment 
ON mv_customer_summary(customer_segment);

CREATE INDEX idx_mv_customer_summary_lifetime_value 
ON mv_customer_summary(lifetime_value DESC);

-- Refresh materialized view (schedule this regularly)
REFRESH MATERIALIZED VIEW mv_customer_summary;
```

This comprehensive collection of SQL examples demonstrates the advanced techniques used throughout the ETL pipeline, providing practical implementations of window functions, CTEs, complex joins, and analytical queries that form the foundation of modern data analytics and business intelligence systems.

