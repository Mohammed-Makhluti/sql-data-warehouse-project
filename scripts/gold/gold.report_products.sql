/*
===============================================================================
Product Report
===============================================================================
Purpose:
    - This report consolidates key product metrics and behaviors.

Highlights:
    1. Gathers essential fields such as product name, category, subcategory, and cost.
    2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
    3. Aggregates product-level metrics:
        - total orders
        - total sales
        - total quantity sold
        - total customers (unique)
        - lifespan (in months)
    4. Calculates valuable KPIs:
        - recency (months since last sale)
        - average order revenue (AOR)
        - average monthly revenue
===============================================================================
*/
DROP VIEW IF EXISTS gold.report_products;
CREATE VIEW gold.report_products AS
-- 1) Base Query: Retrieves core columns from fact_sales and dim_products
WITH base_query AS (
    SELECT
        f.order_number,
        f.order_date,
        f.customer_key,
        f.sales,
        f.quantity,
        p.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.cost
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
),
-- 2) Product Aggregations: Summarizes key metrics at the product level

Product_aggregations AS(
    SELECT
        product_key,
        product_name,
        category,
        subcategory,
        cost,
        -- Added + 1 to avoid a 0 lifespan for products sold within the same month
        ((EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12) +
            EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date)))) + 1 AS lifespan_months,
        MAX(order_date) AS last_sale_date,
        COUNT(DISTINCT order_number) AS total_orders,
        COUNT(DISTINCT customer_key) AS total_customers,
        SUM(sales) AS total_sales,
        SUM(quantity) AS total_quantity,
        -- More accurate: Total Sales divided by Total Quantity
        ROUND(SUM(sales)::NUMERIC / NULLIF(SUM(quantity), 0), 1) AS avg_selling_price
    FROM base_query
    GROUP BY
        product_key,
        product_name,
        category,
        subcategory,
        cost
)
SELECT
    product_key,
    product_name,
    category,
    subcategory,
    cost,
    last_sale_date,
    (EXTRACT(YEAR FROM AGE(now(), last_sale_date)) * 12) +
        EXTRACT(MONTH FROM AGE(now(), last_sale_date)) AS recency_months,
    CASE
        WHEN total_sales > 50000 THEN 'High_Performer'
        WHEN total_sales >= 10000 THEN 'Mid_range'
        ELSE 'Low_Performer'
    END AS product_segment,
    lifespan_months,
    total_orders,
    total_sales,
    total_quantity,
    total_customers,
    avg_selling_price,
    ROUND(total_sales * 1.0 / NULLIF(total_orders, 0), 2) AS avg_order_value,
    ROUND(total_sales * 1.0 / NULLIF(lifespan_months, 0), 2) AS avg_monthly_revenue
FROM Product_aggregations;
