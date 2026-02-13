/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from CSV Files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
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
        RAISE NOTICE 'Loading Bronze Layer';
        RAISE NOTICE '================================================';

        RAISE NOTICE '------------------------------------------------';
        RAISE NOTICE 'Loading CRM Tables';
        RAISE NOTICE '------------------------------------------------';

        -- 1. Load crm_cust_info
        v_start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
        COPY bronze.crm_cust_info
        FROM 'C:/Users/mmakh/OneDrive/Desktop/sql-data-warehouse-project-main/datasets/source_crm/cust_info.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER ',');
        v_end_time := clock_timestamp();
        RAISE NOTICE '>> Load Duration: %', v_end_time - v_start_time;
        RAISE NOTICE '>> -------------';

        -- 2. Load crm_prd_info
        v_start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
        COPY bronze.crm_prd_info
        FROM 'C:/Users/mmakh/OneDrive/Desktop/sql-data-warehouse-project-main/datasets/source_crm/prd_info.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER ',');
        v_end_time := clock_timestamp();
        RAISE NOTICE '>> Load Duration: %', v_end_time - v_start_time;
        RAISE NOTICE '>> -------------';

        -- 3. Load crm_sales_details
        v_start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
        COPY bronze.crm_sales_details
        FROM 'C:/Users/mmakh/OneDrive/Desktop/sql-data-warehouse-project-main/datasets/source_crm/sales_details.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER ',');
        v_end_time := clock_timestamp();
        RAISE NOTICE '>> Load Duration: %', v_end_time - v_start_time;
        RAISE NOTICE '>> -------------';

        RAISE NOTICE '------------------------------------------------';
        RAISE NOTICE 'Loading ERP Tables';
        RAISE NOTICE '------------------------------------------------';

        -- 4. Load erp_loc_a101
        v_start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
        COPY bronze.erp_loc_a101
        FROM 'C:/Users/mmakh/OneDrive/Desktop/sql-data-warehouse-project-main/datasets/source_erp/LOC_A101.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER ',');
        v_end_time := clock_timestamp();
        RAISE NOTICE '>> Load Duration: %', v_end_time - v_start_time;
        RAISE NOTICE '>> -------------';

        -- 5. Load erp_cus_az12
        v_start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: bronze.erp_cus_az12';
        TRUNCATE TABLE bronze.erp_cus_az12;
        RAISE NOTICE '>> Inserting Data Into: bronze.erp_cus_az12';
        COPY bronze.erp_cus_az12
        FROM 'C:/Users/mmakh/OneDrive/Desktop/sql-data-warehouse-project-main/datasets/source_erp/CUST_AZ12.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER ',');
        v_end_time := clock_timestamp();
        RAISE NOTICE '>> Load Duration: %', v_end_time - v_start_time;
        RAISE NOTICE '>> -------------';

        -- 6. Load erp_px_cat_g1v2
        v_start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        COPY bronze.erp_px_cat_g1v2
        FROM 'C:/Users/mmakh/OneDrive/Desktop/sql-data-warehouse-project-main/datasets/source_erp/PX_CAT_G1V2.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER ',');
        v_end_time := clock_timestamp();
        RAISE NOTICE '>> Load Duration: %', v_end_time - v_start_time;
        RAISE NOTICE '>> -------------';

        v_batch_end_time := clock_timestamp();
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'Loading Bronze Layer is Completed';
        RAISE NOTICE '   - Total Load Duration: %', v_batch_end_time - v_batch_start_time;
        RAISE NOTICE '==========================================';

    -- Catch block equivalent in PostgreSQL
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error Code: %', SQLSTATE;
        RAISE NOTICE '==========================================';
    END;
END;
$$;
