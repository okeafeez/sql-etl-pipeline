-- Complex SQL Queries for ETL Pipeline
-- Demonstrates JOINs, CTEs, and Window Functions

-- =====================================================
-- 1. CUSTOMER ANALYTICS WITH WINDOW FUNCTIONS
-- =====================================================

-- Customer lifetime value and ranking with window functions
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.full_name,
        c.customer_segment,
        c.registration_date,
        COUNT(o.order_id) as total_orders,
        SUM(o.order_total) as lifetime_value,
        AVG(o.order_total) as avg_order_value,
        MAX(o.order_date) as last_order_date,
        MIN(o.order_date) as first_order_date
    FROM dw_analytics.dim_customers c
    LEFT JOIN source_ecommerce.orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.full_name, c.customer_segment, c.registration_date
),
customer_rankings AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (ORDER BY lifetime_value DESC) as ltv_rank,
        RANK() OVER (PARTITION BY customer_segment ORDER BY lifetime_value DESC) as segment_rank,
        NTILE(10) OVER (ORDER BY lifetime_value DESC) as ltv_decile,
        LAG(lifetime_value) OVER (ORDER BY registration_date) as prev_customer_ltv,
        LEAD(lifetime_value) OVER (ORDER BY registration_date) as next_customer_ltv,
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
    lifetime_value,
    avg_order_value,
    ltv_rank,
    segment_rank,
    ltv_decile,
    CASE 
        WHEN ltv_decile <= 2 THEN 'Top 20%'
        WHEN ltv_decile <= 5 THEN 'Middle 30%'
        ELSE 'Bottom 50%'
    END as customer_tier,
    rolling_avg_ltv,
    EXTRACT(DAYS FROM (last_order_date - first_order_date)) as customer_lifespan_days
FROM customer_rankings
ORDER BY ltv_rank;

-- =====================================================
-- 2. PRODUCT PERFORMANCE ANALYSIS WITH COMPLEX JOINS
-- =====================================================

-- Product performance analysis with multiple joins and aggregations
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
        SUM(oi.quantity) as total_quantity_sold,
        SUM(oi.total_price) as total_revenue,
        SUM(oi.quantity * p.cost) as total_cost,
        AVG(oi.quantity) as avg_quantity_per_order,
        MAX(o.order_date) as last_sold_date,
        MIN(o.order_date) as first_sold_date
    FROM source_ecommerce.products p
    LEFT JOIN source_ecommerce.order_items oi ON p.product_id = oi.product_id
    LEFT JOIN source_ecommerce.orders o ON oi.order_id = o.order_id
    WHERE p.is_active = TRUE
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
),
inventory_status AS (
    SELECT 
        i.product_id,
        SUM(i.quantity_on_hand) as total_stock,
        SUM(i.quantity_reserved) as total_reserved,
        COUNT(DISTINCT i.warehouse_location) as warehouse_count,
        AVG(i.reorder_level) as avg_reorder_level
    FROM source_ecommerce.inventory i
    GROUP BY i.product_id
)
SELECT 
    ps.product_id,
    ps.product_name,
    ps.category,
    ps.subcategory,
    ps.brand,
    ps.price,
    ps.profit_per_unit,
    ps.times_ordered,
    ps.total_quantity_sold,
    ps.total_revenue,
    (ps.total_revenue - (ps.total_quantity_sold * ps.cost)) as total_profit,
    ps.avg_quantity_per_order,
    cm.category_total_revenue,
    (ps.total_revenue / NULLIF(cm.category_total_revenue, 0) * 100) as category_revenue_share,
    ist.total_stock,
    ist.total_reserved,
    ist.warehouse_count,
    CASE 
        WHEN ps.total_quantity_sold = 0 THEN 'No Sales'
        WHEN ist.total_stock <= ist.avg_reorder_level THEN 'Low Stock'
        WHEN ps.last_sold_date < CURRENT_DATE - INTERVAL '30 days' THEN 'Slow Moving'
        ELSE 'Active'
    END as product_status,
    EXTRACT(DAYS FROM (ps.last_sold_date - ps.first_sold_date)) as sales_period_days
FROM product_sales ps
JOIN category_metrics cm ON ps.category = cm.category
LEFT JOIN inventory_status ist ON ps.product_id = ist.product_id
ORDER BY ps.total_revenue DESC;

-- =====================================================
-- 3. SALES TREND ANALYSIS WITH WINDOW FUNCTIONS
-- =====================================================

-- Monthly sales trends with year-over-year comparison
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
        (total_revenue - LAG(total_revenue) OVER (ORDER BY year, month)) / 
            NULLIF(LAG(total_revenue) OVER (ORDER BY year, month), 0) * 100 as mom_growth_rate,
        -- Year-over-year comparison
        LAG(total_revenue, 12) OVER (ORDER BY year, month) as same_month_prev_year,
        (total_revenue - LAG(total_revenue, 12) OVER (ORDER BY year, month)) / 
            NULLIF(LAG(total_revenue, 12) OVER (ORDER BY year, month), 0) * 100 as yoy_growth_rate,
        -- Rolling averages
        AVG(total_revenue) OVER (
            ORDER BY year, month 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as rolling_3month_avg,
        AVG(total_revenue) OVER (
            ORDER BY year, month 
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) as rolling_6month_avg,
        -- Cumulative metrics
        SUM(total_revenue) OVER (
            PARTITION BY year 
            ORDER BY month 
            ROWS UNBOUNDED PRECEDING
        ) as ytd_revenue,
        -- Ranking
        RANK() OVER (ORDER BY total_revenue DESC) as revenue_rank,
        DENSE_RANK() OVER (PARTITION BY year ORDER BY total_revenue DESC) as monthly_rank_in_year
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
    ROUND(yoy_growth_rate, 2) as yoy_growth_rate,
    ROUND(rolling_3month_avg, 2) as rolling_3month_avg,
    ROUND(rolling_6month_avg, 2) as rolling_6month_avg,
    ROUND(ytd_revenue, 2) as ytd_revenue,
    revenue_rank,
    monthly_rank_in_year,
    CASE 
        WHEN mom_growth_rate > 10 THEN 'High Growth'
        WHEN mom_growth_rate > 0 THEN 'Growth'
        WHEN mom_growth_rate > -10 THEN 'Stable'
        ELSE 'Declining'
    END as growth_category
FROM sales_with_trends
ORDER BY year DESC, month DESC;

-- =====================================================
-- 4. CUSTOMER COHORT ANALYSIS
-- =====================================================

-- Customer cohort analysis to understand retention
WITH customer_cohorts AS (
    SELECT 
        c.customer_id,
        DATE_TRUNC('month', c.registration_date) as cohort_month,
        DATE_TRUNC('month', o.order_date) as order_month,
        o.order_total
    FROM source_ecommerce.customers c
    JOIN source_ecommerce.orders o ON c.customer_id = o.customer_id
    WHERE o.order_status NOT IN ('Cancelled')
),
cohort_data AS (
    SELECT 
        cohort_month,
        order_month,
        EXTRACT(YEAR FROM age(order_month, cohort_month)) * 12 + 
        EXTRACT(MONTH FROM age(order_month, cohort_month)) as period_number,
        COUNT(DISTINCT customer_id) as customers,
        SUM(order_total) as revenue
    FROM customer_cohorts
    GROUP BY cohort_month, order_month
),
cohort_sizes AS (
    SELECT 
        cohort_month,
        COUNT(DISTINCT customer_id) as cohort_size
    FROM customer_cohorts
    WHERE cohort_month = order_month  -- First purchase month
    GROUP BY cohort_month
)
SELECT 
    cd.cohort_month,
    cd.period_number,
    cd.customers,
    cs.cohort_size,
    ROUND(cd.customers::DECIMAL / cs.cohort_size * 100, 2) as retention_rate,
    cd.revenue,
    ROUND(cd.revenue / cd.customers, 2) as revenue_per_customer,
    -- Cumulative metrics
    SUM(cd.customers) OVER (
        PARTITION BY cd.cohort_month 
        ORDER BY cd.period_number 
        ROWS UNBOUNDED PRECEDING
    ) as cumulative_customers,
    SUM(cd.revenue) OVER (
        PARTITION BY cd.cohort_month 
        ORDER BY cd.period_number 
        ROWS UNBOUNDED PRECEDING
    ) as cumulative_revenue
FROM cohort_data cd
JOIN cohort_sizes cs ON cd.cohort_month = cs.cohort_month
ORDER BY cd.cohort_month, cd.period_number;

-- =====================================================
-- 5. INVENTORY OPTIMIZATION ANALYSIS
-- =====================================================

-- Inventory optimization with sales velocity and stock levels
WITH product_velocity AS (
    SELECT 
        p.product_id,
        p.product_name,
        p.category,
        p.price,
        COUNT(oi.order_item_id) as order_frequency,
        SUM(oi.quantity) as total_sold_90days,
        AVG(oi.quantity) as avg_quantity_per_order,
        MAX(o.order_date) as last_sale_date,
        EXTRACT(DAYS FROM (CURRENT_DATE - MAX(o.order_date))) as days_since_last_sale
    FROM source_ecommerce.products p
    LEFT JOIN source_ecommerce.order_items oi ON p.product_id = oi.product_id
    LEFT JOIN source_ecommerce.orders o ON oi.order_id = o.order_id
    WHERE o.order_date >= CURRENT_DATE - INTERVAL '90 days'
       OR o.order_date IS NULL
    GROUP BY p.product_id, p.product_name, p.category, p.price
),
inventory_summary AS (
    SELECT 
        i.product_id,
        SUM(i.quantity_on_hand) as total_stock,
        SUM(i.quantity_reserved) as total_reserved,
        SUM(i.quantity_on_hand - i.quantity_reserved) as available_stock,
        AVG(i.reorder_level) as avg_reorder_level,
        COUNT(DISTINCT i.warehouse_location) as warehouse_count,
        STRING_AGG(DISTINCT i.warehouse_location, ', ') as warehouse_locations
    FROM source_ecommerce.inventory i
    GROUP BY i.product_id
),
stock_analysis AS (
    SELECT 
        pv.*,
        ist.total_stock,
        ist.total_reserved,
        ist.available_stock,
        ist.avg_reorder_level,
        ist.warehouse_count,
        ist.warehouse_locations,
        -- Calculate sales velocity (units per day)
        CASE 
            WHEN pv.total_sold_90days > 0 THEN pv.total_sold_90days / 90.0
            ELSE 0
        END as daily_sales_velocity,
        -- Calculate days of inventory
        CASE 
            WHEN pv.total_sold_90days > 0 THEN 
                ist.available_stock / (pv.total_sold_90days / 90.0)
            ELSE NULL
        END as days_of_inventory,
        -- Stock status
        CASE 
            WHEN ist.available_stock <= 0 THEN 'Out of Stock'
            WHEN ist.available_stock <= ist.avg_reorder_level THEN 'Low Stock'
            WHEN pv.days_since_last_sale > 90 THEN 'Slow Moving'
            WHEN ist.available_stock > (pv.total_sold_90days * 2) THEN 'Overstock'
            ELSE 'Normal'
        END as stock_status
    FROM product_velocity pv
    LEFT JOIN inventory_summary ist ON pv.product_id = ist.product_id
)
SELECT 
    product_id,
    product_name,
    category,
    price,
    order_frequency,
    total_sold_90days,
    ROUND(daily_sales_velocity, 2) as daily_sales_velocity,
    total_stock,
    available_stock,
    ROUND(days_of_inventory, 1) as days_of_inventory,
    avg_reorder_level,
    warehouse_count,
    warehouse_locations,
    stock_status,
    days_since_last_sale,
    -- Recommendations
    CASE 
        WHEN stock_status = 'Out of Stock' THEN 'URGENT: Restock immediately'
        WHEN stock_status = 'Low Stock' THEN 'Reorder soon'
        WHEN stock_status = 'Overstock' THEN 'Reduce inventory, consider promotion'
        WHEN stock_status = 'Slow Moving' THEN 'Review product performance'
        ELSE 'Monitor regularly'
    END as recommendation,
    -- Priority scoring (1-10, 10 being highest priority)
    CASE 
        WHEN stock_status = 'Out of Stock' AND daily_sales_velocity > 1 THEN 10
        WHEN stock_status = 'Low Stock' AND daily_sales_velocity > 0.5 THEN 8
        WHEN stock_status = 'Overstock' THEN 6
        WHEN stock_status = 'Slow Moving' THEN 4
        ELSE 5
    END as priority_score
FROM stock_analysis
ORDER BY priority_score DESC, daily_sales_velocity DESC;

-- =====================================================
-- 6. ADVANCED CUSTOMER SEGMENTATION
-- =====================================================

-- RFM Analysis (Recency, Frequency, Monetary) for customer segmentation
WITH customer_rfm AS (
    SELECT 
        c.customer_id,
        c.full_name,
        c.customer_segment,
        -- Recency: Days since last order
        EXTRACT(DAYS FROM (CURRENT_DATE - MAX(o.order_date))) as recency_days,
        -- Frequency: Number of orders
        COUNT(DISTINCT o.order_id) as frequency,
        -- Monetary: Total spent
        SUM(o.order_total) as monetary_value,
        AVG(o.order_total) as avg_order_value,
        MAX(o.order_date) as last_order_date,
        MIN(o.order_date) as first_order_date
    FROM source_ecommerce.customers c
    LEFT JOIN source_ecommerce.orders o ON c.customer_id = o.customer_id
    WHERE o.order_status NOT IN ('Cancelled') OR o.order_status IS NULL
    GROUP BY c.customer_id, c.full_name, c.customer_segment
),
rfm_scores AS (
    SELECT 
        *,
        -- RFM Scores (1-5 scale, 5 being best)
        CASE 
            WHEN recency_days IS NULL THEN 1
            WHEN recency_days <= 30 THEN 5
            WHEN recency_days <= 60 THEN 4
            WHEN recency_days <= 90 THEN 3
            WHEN recency_days <= 180 THEN 2
            ELSE 1
        END as recency_score,
        
        CASE 
            WHEN frequency IS NULL OR frequency = 0 THEN 1
            WHEN frequency >= 10 THEN 5
            WHEN frequency >= 5 THEN 4
            WHEN frequency >= 3 THEN 3
            WHEN frequency >= 2 THEN 2
            ELSE 1
        END as frequency_score,
        
        CASE 
            WHEN monetary_value IS NULL OR monetary_value = 0 THEN 1
            WHEN monetary_value >= 1000 THEN 5
            WHEN monetary_value >= 500 THEN 4
            WHEN monetary_value >= 200 THEN 3
            WHEN monetary_value >= 100 THEN 2
            ELSE 1
        END as monetary_score
    FROM customer_rfm
),
rfm_segments AS (
    SELECT 
        *,
        (recency_score + frequency_score + monetary_score) as rfm_total,
        CONCAT(recency_score, frequency_score, monetary_score) as rfm_string,
        -- Customer segments based on RFM scores
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
        END as customer_segment_rfm
    FROM rfm_scores
)
SELECT 
    customer_id,
    full_name,
    customer_segment as original_segment,
    customer_segment_rfm,
    recency_days,
    frequency,
    ROUND(monetary_value, 2) as monetary_value,
    ROUND(avg_order_value, 2) as avg_order_value,
    recency_score,
    frequency_score,
    monetary_score,
    rfm_total,
    rfm_string,
    last_order_date,
    first_order_date,
    EXTRACT(DAYS FROM (last_order_date - first_order_date)) as customer_lifespan_days,
    -- Recommended actions
    CASE 
        WHEN customer_segment_rfm = 'Champions' THEN 'Reward and upsell premium products'
        WHEN customer_segment_rfm = 'Loyal Customers' THEN 'Maintain engagement with personalized offers'
        WHEN customer_segment_rfm = 'New Customers' THEN 'Onboard with welcome series and education'
        WHEN customer_segment_rfm = 'Potential Loyalists' THEN 'Increase purchase frequency with targeted campaigns'
        WHEN customer_segment_rfm = 'Big Spenders' THEN 'Engage with high-value product recommendations'
        WHEN customer_segment_rfm = 'At Risk' THEN 'Win-back campaign with special offers'
        WHEN customer_segment_rfm = 'Cannot Lose Them' THEN 'Urgent retention efforts'
        WHEN customer_segment_rfm = 'Lost Customers' THEN 'Aggressive win-back or remove from active marketing'
        ELSE 'Standard marketing approach'
    END as recommended_action
FROM rfm_segments
ORDER BY rfm_total DESC, monetary_value DESC;

