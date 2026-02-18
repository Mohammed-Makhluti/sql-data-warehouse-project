-- =============================================================================
-- Project: Data Warehouse Analytics (Gold Layer)
-- Description: This script contains key business queries to explore data, 
--              calculate metrics, and perform Magnitude Analysis.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Metadata Exploration
-- -----------------------------------------------------------------------------
-- List all tables in the warehouse
SELECT * FROM information_schema.tables WHERE table_schema IN ('bronze', 'silver', 'gold');

-- Explore columns of the main dimension table
SELECT * FROM information_schema.columns 
WHERE table_name = 'dim_customers' AND table_schema = 'gold';

-- -----------------------------------------------------------------------------
-- 2. Key Business Metrics (Executive Summary)
-- -----------------------------------------------------------------------------
-- High-level overview of total revenue, quantity, orders, products, and customers
SELECT 'Total Revenue' AS measure_name, SUM(sales) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity) FROM gold.fact_sales
UNION ALL
SELECT 'Average Price', ROUND(AVG(price), 2) FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Orders', COUNT(DISTINCT order_number) FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Products', COUNT(DISTINCT product_number) FROM gold.dim_products
UNION ALL
SELECT 'Total Nr. Customers', COUNT(DISTINCT customer_number) FROM gold.dim_customers
UNION ALL
SELECT 'Avg Order Value', SUM(sales) / COUNT(DISTINCT order_number) FROM gold.fact_sales;

-- -----------------------------------------------------------------------------
-- 3. Magnitude Analysis (Customer & Product Distributions)
-- -----------------------------------------------------------------------------
-- Customers distribution by Country
SELECT country, COUNT(DISTINCT customer_number) AS total_customers
FROM gold.dim_customers GROUP BY country ORDER BY total_customers DESC;

-- Customers distribution by Gender
SELECT gender, COUNT(DISTINCT customer_number) AS total_customers
FROM gold.dim_customers GROUP BY gender ORDER BY total_customers DESC;

-- Category Performance: Total Revenue per Category
SELECT pr.category, SUM(f.sales) AS total_revenue
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_products AS pr ON f.product_key = pr.product_key
GROUP BY pr.category ORDER BY total_revenue DESC;

-- -----------------------------------------------------------------------------
-- 4. Ranking & Performance (Top/Bottom Analysis)
-- -----------------------------------------------------------------------------
-- Top 5 performing products by Revenue
SELECT pr.product_name, SUM(f.sales) AS total_revenue
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_products AS pr ON f.product_key = pr.product_key
GROUP BY pr.product_name ORDER BY total_revenue DESC LIMIT 5;

-- Bottom 5 products by Revenue (Potential for removal or promotion)
SELECT pr.product_name, SUM(f.sales) AS total_revenue
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_products AS pr ON f.product_key = pr.product_key
GROUP BY pr.product_name ORDER BY total_revenue ASC LIMIT 5;

-- Top 5 subcategories by Revenue
SELECT pr.subcategory, SUM(f.sales) AS total_revenue
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_products AS pr ON f.product_key = pr.product_key
GROUP BY pr.subcategory ORDER BY total_revenue DESC LIMIT 5;

-- -----------------------------------------------------------------------------
-- 5. Advanced Ranking using Window Functions
-- -----------------------------------------------------------------------------
-- Ranking products using ROW_NUMBER() for precise analysis
SELECT * FROM (
    SELECT 
        pr.product_name, 
        SUM(f.sales) AS total_revenue,
        ROW_NUMBER() OVER (ORDER BY SUM(f.sales) ASC) AS rank_products
    FROM gold.fact_sales AS f
    LEFT JOIN gold.dim_products AS pr ON f.product_key = pr.product_key
    GROUP BY pr.product_name
) t WHERE rank_products <= 5;
