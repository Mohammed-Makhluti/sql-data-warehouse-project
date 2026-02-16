/*
===============================================================================
Stored Procedure: Load silver Layer (Source -> silver)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'silver' schema from external CSV files.
    It performs the following actions:
    - Truncates the silver tables before loading data.
    - Uses the `COPY` command to load data from CSV Files to silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL silver.load_silver();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_batch_start_time TIMESTAMP;
    v_batch_end_time TIMESTAMP;
BEGIN
    -- Main Try block equivalent in PostgreSQL
    BEGIN
        v_batch_start_time := clock_timestamp();
        RAISE NOTICE '================================================';
        RAISE NOTICE 'Loading silver Layer';
        RAISE NOTICE '================================================';

        RAISE NOTICE '------------------------------------------------';
        RAISE NOTICE 'Loading CRM Tables';
        RAISE NOTICE '------------------------------------------------';

-- =======================================================================
        -- 1. Load crm_cust_info
        v_start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
-- -----------------------------------------------------------------------
        INSERT INTO silver.crm_cust_info(cst_id, cst_key, cst_firstname, cst_lastname, cst_material_status, cst_gndr, cst_create_date)
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
        CASE
            WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last -- For delete Duplicate
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;
-- -----------------------------------------------------------------------
        v_end_time := clock_timestamp();
        RAISE NOTICE '>> Load Duration: %', v_end_time - v_start_time;
        RAISE NOTICE '>> -------------';

-- =======================================================================

        -- 2. Load crm_prd_info
        v_start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
-- -----------------------------------------------------------------------
        INSERT INTO silver.crm_prd_info(prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
        SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_Id,
        SUBSTRING(prd_key,7) AS prd_key,
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        prd_start_dt::DATE,
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info;
-- -----------------------------------------------------------------------
        v_end_time := clock_timestamp();
        RAISE NOTICE '>> Load Duration: %', v_end_time - v_start_time;
        RAISE NOTICE '>> -------------';

-- =======================================================================

        -- 3. Load crm_sales_details
        v_start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
-- -----------------------------------------------------------------------
        INSERT INTO silver.crm_sales_details(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE
                WHEN sls_order_dt <= 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
                ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
            END sls_order_dt,
            CASE
                WHEN sls_ship_dt <= 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
                ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
            END sls_ship_dt,
            CASE
                WHEN sls_due_dt <= 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
                ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
            END sls_due_dt,
            CASE
                WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
                    THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END sls_sales,
            sls_quantity,
            CASE
                WHEN sls_price <= 0 OR sls_price IS NULL
                    THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END sls_price
        FROM bronze.crm_sales_details;
-- -----------------------------------------------------------------------
        v_end_time := clock_timestamp();
        RAISE NOTICE '>> Load Duration: %', v_end_time - v_start_time;
        RAISE NOTICE '>> -------------';

        RAISE NOTICE '------------------------------------------------';
        RAISE NOTICE 'Loading ERP Tables';
        RAISE NOTICE '------------------------------------------------';
-- =======================================================================

        -- 4. Load erp_loc_a101
        v_start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;
        RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';
-- -----------------------------------------------------------------------
        INSERT INTO silver.erp_loc_a101(cid, cntry)
        SELECT
            REPLACE(cid,'-','') AS cid,
            CASE
                WHEN UPPER(TRIM(cntry)) IN ('US', 'USA', 'United States') THEN 'United States'
                WHEN UPPER(TRIM(cntry)) IN ('DE', 'Germany') THEN 'Germany'
                WHEN UPPER(TRIM(cntry)) = '' OR UPPER(TRIM(cntry)) IS NULL THEN 'N\A'
                ELSE cntry
            END AS cntry
        FROM bronze.erp_loc_a101;
-- -----------------------------------------------------------------------
        v_end_time := clock_timestamp();
        RAISE NOTICE '>> Load Duration: %', v_end_time - v_start_time;
        RAISE NOTICE '>> -------------';

-- =======================================================================

        -- 5. Load erp_cus_az12
        v_start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: silver.erp_cus_az12';
        TRUNCATE TABLE silver.erp_cus_az12;
        RAISE NOTICE '>> Inserting Data Into: silver.erp_cus_az12';
-- -----------------------------------------------------------------------
        INSERT INTO silver.erp_cus_az12(cid, bdate, gen)
        SELECT
            CASE
                WHEN cid ILIKE 'NAS%' THEN SUBSTRING(cid FROM 4)
                ELSE cid
            END AS cid,
            CASE
                WHEN bdate < '1900-01-01' OR bdate > CURRENT_DATE THEN NULL
                ELSE bdate
            END AS bdate,
            CASE
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cus_az12;
-- -----------------------------------------------------------------------
        v_end_time := clock_timestamp();
        RAISE NOTICE '>> Load Duration: %', v_end_time - v_start_time;
        RAISE NOTICE '>> -------------';

-- =======================================================================

        -- 6. Load erp_px_cat_g1v2
        v_start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
-- -----------------------------------------------------------------------
        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT
            ID,
            CAT,
            SUBCAT,
            MAINTENANCE
        FROM bronze.erp_px_cat_g1v2;
-- -----------------------------------------------------------------------
        v_end_time := clock_timestamp();
        RAISE NOTICE '>> Load Duration: %', v_end_time - v_start_time;
        RAISE NOTICE '>> -------------';

        v_batch_end_time := clock_timestamp();
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'Loading silver Layer is Completed';
        RAISE NOTICE '   - Total Load Duration: %', v_batch_end_time - v_batch_start_time;
        RAISE NOTICE '==========================================';

-- =======================================================================

    -- Catch block equivalent in PostgreSQL
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING silver LAYER';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error Code: %', SQLSTATE;
        RAISE NOTICE '==========================================';
    END;
END;
$$;
