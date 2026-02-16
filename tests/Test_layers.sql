-- =============================================================================
-- 1. Bronze Layer Verification
-- =============================================================================

-- Check Total Row Counts for all tables
SELECT 'bronze.crm_cust_info' AS table_name, COUNT(*) AS row_count FROM bronze.crm_cust_info
UNION ALL
SELECT 'bronze.crm_prd_info', COUNT(*) FROM bronze.crm_prd_info
UNION ALL
SELECT 'bronze.crm_sales_details', COUNT(*) FROM bronze.crm_sales_details
UNION ALL
SELECT 'bronze.erp_cus_az12', COUNT(*) FROM bronze.erp_cus_az12
UNION ALL
SELECT 'bronze.erp_loc_a101', COUNT(*) FROM bronze.erp_loc_a101
UNION ALL
SELECT 'bronze.erp_px_cat_g1v2', COUNT(*) FROM bronze.erp_px_cat_g1v2;

-- Check for NULLs in Primary Keys
SELECT 
    (SELECT COUNT(*) FROM bronze.crm_cust_info WHERE cst_id IS NULL) AS null_ids_crm,
    (SELECT COUNT(*) FROM bronze.crm_sales_details WHERE sls_ord_num IS NULL) AS null_orders_sales;


-- =============================================================================
-- 2. Silver Layer Verification
-- =============================================================================

-- Check Row Counts (After cleaning and de-duplication)
SELECT 'silver.crm_cust_info' AS table_name, COUNT(*) AS row_count FROM silver.crm_cust_info
UNION ALL
SELECT 'silver.crm_sales_details', COUNT(*) FROM silver.crm_sales_details;

-- Check for invalid genders (Data Standardization)
SELECT DISTINCT cst_gndr FROM silver.crm_cust_info;

-- Check for invalid country names (Data Standardization)
SELECT DISTINCT cntry FROM silver.erp_loc_a101;

-- Logical Range Check for Birth Dates
SELECT COUNT(*) AS invalid_dates 
FROM silver.erp_cus_az12 
WHERE bdate > CURRENT_DATE OR bdate < '1900-01-01';

-- =============================================================================
-- 3. Gold Layer (Reporting) Verification
-- =============================================================================

-- 1. Referential Integrity: Check for orphaned sales
SELECT COUNT(*) AS orphaned_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

-- 2. Business Logic: Performance by Country
SELECT
    c.country,
    SUM(f.sales) AS total_revenue,
    COUNT(f.order_number) AS total_orders,
    SUM(f.quantity) AS total_items_sold
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.country
ORDER BY total_revenue DESC;

-- 3. Business Logic: Product Category Analysis
SELECT
    p.category,
    p.subcategory,
    SUM(f.sales) AS revenue,
    ROUND(AVG(f.price), 2) AS avg_unit_price
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY p.category, p.subcategory
ORDER BY revenue DESC;
