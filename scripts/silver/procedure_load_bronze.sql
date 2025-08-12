/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
    SET NOCOUNT ON;

    DECLARE @BatchStartTime DATETIME = GETDATE();
    DECLARE @BatchEndTime DATETIME;
    DECLARE @StepStartTime DATETIME;
    DECLARE @StepEndTime DATETIME;
    DECLARE @StepRunTime VARCHAR(50);

    PRINT '=====================================================';
    PRINT '>>> Starting load_silver process at ' + CONVERT(VARCHAR, @BatchStartTime, 120);
    PRINT '=====================================================';

    BEGIN TRY
        --------------------------------------------------------------------------------
        -- silver.crm_cust_info
        --------------------------------------------------------------------------------
        SET @StepStartTime = GETDATE();
        PRINT '>> Truncating: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname, cst_material_status, cst_gndr, cst_create_date
        )
        SELECT cst_id, 
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname)  AS cst_lastname,
            CASE 
                WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_material_status,    
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t 
        WHERE flag_last = 1;

        SET @StepEndTime = GETDATE();
        SET @StepRunTime = CAST(DATEDIFF(SECOND, @StepStartTime, @StepEndTime) AS VARCHAR) + ' sec';
        PRINT '>> silver.crm_cust_info loaded in ' + @StepRunTime;

        --------------------------------------------------------------------------------
        -- silver.crm_prd_info
        --------------------------------------------------------------------------------
        SET @StepStartTime = GETDATE();
        PRINT '>> Truncating: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
        )
        SELECT prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info;

        SET @StepEndTime = GETDATE();
        SET @StepRunTime = CAST(DATEDIFF(SECOND, @StepStartTime, @StepEndTime) AS VARCHAR) + ' sec';
        PRINT '>> silver.crm_prd_info loaded in ' + @StepRunTime;

        --------------------------------------------------------------------------------
        -- silver.crm_sales_details
        --------------------------------------------------------------------------------
        SET @StepStartTime = GETDATE();
        PRINT '>> Truncating: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
        )
        SELECT sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN LEN(sls_order_dt) != 8 OR sls_order_dt = 0 THEN NULL
                 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END,
            CASE WHEN LEN(sls_ship_dt) != 8 OR sls_ship_dt = 0 THEN NULL
                 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END,
            CASE WHEN LEN(sls_due_dt) != 8 OR sls_due_dt = 0 THEN NULL
                 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END,
            CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * sls_price
                 THEN sls_quantity * ABS(sls_price)
                 ELSE sls_sales END,
            sls_quantity,
            CASE WHEN sls_price <= 0 OR sls_price IS NULL
                 THEN sls_sales / NULLIF(sls_quantity, 0)
                 ELSE sls_price END
        FROM bronze.crm_sales_details;

        SET @StepEndTime = GETDATE();
        SET @StepRunTime = CAST(DATEDIFF(SECOND, @StepStartTime, @StepEndTime) AS VARCHAR) + ' sec';
        PRINT '>> silver.crm_sales_details loaded in ' + @StepRunTime;

        --------------------------------------------------------------------------------
        -- silver.erp_cust_az12
        --------------------------------------------------------------------------------
        SET @StepStartTime = GETDATE();
        PRINT '>> Truncating: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END,
               CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
               CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                    WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                    ELSE 'n/a' END
        FROM bronze.erp_cust_az12;

        SET @StepEndTime = GETDATE();
        SET @StepRunTime = CAST(DATEDIFF(SECOND, @StepStartTime, @StepEndTime) AS VARCHAR) + ' sec';
        PRINT '>> silver.erp_cust_az12 loaded in ' + @StepRunTime;

        --------------------------------------------------------------------------------
        -- silver.erp_loc_a101
        --------------------------------------------------------------------------------
        SET @StepStartTime = GETDATE();
        PRINT '>> Truncating: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT REPLACE(cid, '-', ''),
               CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                    WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
                    WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                    ELSE TRIM(cntry) END
        FROM bronze.erp_loc_a101;

        SET @StepEndTime = GETDATE();
        SET @StepRunTime = CAST(DATEDIFF(SECOND, @StepStartTime, @StepEndTime) AS VARCHAR) + ' sec';
        PRINT '>> silver.erp_loc_a101 loaded in ' + @StepRunTime;

        --------------------------------------------------------------------------------
        -- silver.erp_px_cat_g1v2
        --------------------------------------------------------------------------------
        SET @StepStartTime = GETDATE();
        PRINT '>> Truncating: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT id, cat, subcat, maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @StepEndTime = GETDATE();
        SET @StepRunTime = CAST(DATEDIFF(SECOND, @StepStartTime, @StepEndTime) AS VARCHAR) + ' sec';
        PRINT '>> silver.erp_px_cat_g1v2 loaded in ' + @StepRunTime;

        --------------------------------------------------------------------------------
        -- End Batch
        --------------------------------------------------------------------------------
        SET @BatchEndTime = GETDATE();
        PRINT '=====================================================';
        PRINT '>>> load_silver completed successfully at ' + CONVERT(VARCHAR, @BatchEndTime, 120);
        PRINT '>>> Total Runtime: ' + CAST(DATEDIFF(SECOND, @BatchStartTime, @BatchEndTime) AS VARCHAR) + ' sec';
        PRINT '=====================================================';
    END TRY
    BEGIN CATCH
        PRINT '!!! ERROR OCCURRED in load_silver !!!';
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR);
    END CATCH
END;

exec silver.load_silver
