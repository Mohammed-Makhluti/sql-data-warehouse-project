-- =============================================================================
-- Data Loading Verification Script (Bronze Layer)
-- =============================================================================
-- For Call procedure code:
CALL bronze.load_bronze_layer();
-- =============================================================================

-- 1. Check Row Counts for all tables
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

-- 2. Sample Check: View top 5 rows from Sales to ensure data alignment
SELECT * FROM bronze.crm_sales_details LIMIT 5;

-- 3. Check for NULLs in Primary Keys (Quick Quality Check)
SELECT
    (SELECT COUNT(*) FROM bronze.crm_cust_info WHERE cst_id IS NULL) AS null_ids_crm,
    (SELECT COUNT(*) FROM bronze.crm_sales_details WHERE sls_ord_num IS NULL) AS null_orders_sales;

-- 4. Print Any Table:

SELECT * FROM bronze.crm_prd_info LIMIT 10;
SELECT * FROM bronze.crm_sales_details LIMIT 10;
SELECT * FROM bronze.crm_sales_details LIMIT 10;
SELECT * FROM bronze.erp_cus_az12 LIMIT 10;
SELECT * FROM bronze.erp_loc_a101 LIMIT 10;
SELECT * FROM bronze.erp_px_cat_g1v2 LIMIT 10;
