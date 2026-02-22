/*
===============================================================================
Customer Report
===============================================================================
Purpose:
    - This report consolidates key customer metrics and behaviors

Highlights:
    1. Gathers essential fields such as names, ages, and transaction details.
    2. Segments customers into categories (VIP, Regular, New) and age groups.
    3. Aggregates customer-level metrics:
        - total orders
        - total sales
        - total quantity purchased
        - total products
        - lifespan (in months)
    4. Calculates valuable KPIs:
        - recency (months since last order)
        - average order value
        - average monthly spend
===============================================================================
*/
DROP VIEW IF EXISTS gold.report_customers;

CREATE VIEW gold.report_customers AS
-- 1) Base Query: Joins fact sales and dimension tables and extracts basic fields
WITH base_query AS (
    SELECT
        f.order_number,
        f.product_key,
        f.order_date,
        f.sales,
        f.quantity,
        c.customer_key,
        c.customer_number,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        EXTRACT(YEAR FROM AGE(c.birthdate)) AS age
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
        ON f.customer_key = c.customer_key
    WHERE f.order_date IS NOT NULL
),

-- 2) Customer Aggregations
customer_aggregation AS (
    SELECT
        customer_key,
        customer_number,
        customer_name,
        age,
        COUNT(DISTINCT order_number) AS total_orders,
        SUM(sales) AS total_sales,
        SUM(quantity) AS total_quantity,
        COUNT(DISTINCT product_key) AS total_products,
        MAX(order_date) AS last_order_date,
        -- Correctly calculates total months in PostgreSQL (Years * 12 + Months)
        (EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12) +
            EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS lifespan
    FROM base_query
    GROUP BY customer_key, customer_number, customer_name, age
)

-- 3) Final Select: Computes derived metrics, segments, and cleans output
SELECT
    customer_key,
    customer_number,
    customer_name,
    age,
    -- Grouping customers by age brackets
    CASE
        WHEN age < 20 THEN 'Under 20'
        WHEN age < 30 THEN '20 - 29'
        WHEN age < 40 THEN '30 - 39'
        WHEN age < 50 THEN '40 - 49'
        ELSE '50 and above'
    END AS age_group,
    -- Segmenting customers based on loyalty (lifespan) and spending (total_sales)
    CASE
        WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
        WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
        ELSE 'New'
    END AS customer_segment,
    last_order_date,
    -- Calculates recency in months from the current date
    (EXTRACT(YEAR FROM AGE(now(), last_order_date)) * 12) +
        EXTRACT(MONTH FROM AGE(now(), last_order_date)) AS recency_months,
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    lifespan,
    -- Computes Average Order Value (AOV) safely by avoiding division by zero
    ROUND(total_sales / NULLIF(total_orders, 0), 2) AS avg_order_value,
    -- Computes Average Monthly Spend (adds +1 to avoid division by zero for new customers)
    ROUND(total_sales / (lifespan + 1), 2) AS avg_monthly_spend
FROM customer_aggregation;
