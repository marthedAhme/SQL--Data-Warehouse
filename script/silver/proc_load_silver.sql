/* ===============================================================
   Procedure Name : silver.load_silver
   Layer          : Silver Layer
   Type           : ETL Transformation Procedure

   Description:
   This procedure loads and transforms data from the Bronze layer
   into the Silver layer.

   Processing Logic:
   - Truncates existing Silver tables to ensure clean reload.
   - Applies data cleansing (TRIM, NULL handling, default values).
   - Standardizes categorical values (Gender, Marital Status, Country).
   - Fixes invalid dates and converts DateKey (YYYYMMDD) to DATE.
   - Removes duplicates using ROW_NUMBER().
   - Handles negative values using ABS().
   - Applies basic data quality validation rules.
   - Prepares structured, conformed data for the Gold layer.

   Scope:
   - CRM tables
   - ERP tables

   Error Handling:
   - TRY/CATCH block implemented.
   - Logs error message, number, and state if failure occurs.

   Execution:
   EXEC silver.load_silver

   =============================================================== */

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN 
	DECLARE 
		@start_time DATETIME,
		@end_time DATETIME, 
		@start_batch DATETIME, 
		@end_batch DATETIME
		BEGIN TRY
			SET @start_batch = GETDATE();
			PRINT '===============================';
			PRINT 'Loading Silver Layer';
			PRINT '===============================';


			PRINT '-----------------------------------';
			PRINT 'Loading CRM Tables';
			PRINT '-----------------------------------';

			-- Cleansing and Insert data into crm_cust_info
			SET  @start_time = GETDATE();
			PRINT '>> Truncate Table: silver.crm_cust_info';
			TRUNCATE TABLE silver.crm_cust_info
			PRINT '>> Insert the data into: silver.crm_cust_info';
			INSERT INTO silver.crm_cust_info (
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date)
			SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				ELSE 'Unknown'
			END AS cst_marital_status,
			CASE
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				ELSE 'Unknown'
			END AS cst_gndr,
			cst_create_date
			FROM
			(
			SELECT * ,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date) AS last_flage
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
			) t 
			WHERE last_flage = 1;

			-- Cleansing and Insert data into crm_prd_info
			SET  @end_time = GETDATE();
			PRINT '>> Loading Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
			PRINT '---------------';

			SET  @end_time = GETDATE();
			PRINT '>> Truncate Table: silver.crm_prd_info';
			TRUNCATE TABLE silver.crm_prd_info
			PRINT '>> Insert the data into: silver.crm_prd_info';
			INSERT INTO silver.crm_prd_info(
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt)
			SELECT 
				prd_id,
				REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
				SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
				prd_nm,
				COALESCE(prd_cost,0) AS prd_cost,
				CASE UPPER(TRIM(prd_line))
					WHEN  'M' THEN 'Mountain'
					WHEN  'R' THEN 'Road'
					WHEN  'S' THEN 'Other Sales'
					WHEN  'R' THEN 'Touring'
					ELSE 'Unknown'
				END AS prd_line,
				CAST(prd_start_dt AS DATE) AS prd_start_dt,
				CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt
			FROM bronze.crm_prd_info;

			SET  @end_time = GETDATE();
			PRINT '>> Loading Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
			PRINT '---------------';

			-- Cleansing and Insert data into crm_sales_details

			SET  @end_time = GETDATE();
			PRINT '>> Truncate Table: silver.crm_sales_details';
			TRUNCATE TABLE silver.crm_sales_details
			PRINT '>> Insert the data into: silver.crm_sales_details';
			INSERT INTO silver.crm_sales_details (
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				sls_order_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_price)
			SELECT 
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				CASE 
					WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
					ELSE CONVERT(DATE,CAST(sls_order_dt AS VARCHAR(8)),112)
				END AS sls_order_dt,

				CASE 
					WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
					ELSE CONVERT(DATE,CAST(sls_ship_dt AS VARCHAR(8)),112)
				END AS sls_ship_dt,

				CASE 
					WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
					ELSE CONVERT(DATE,CAST(sls_due_dt AS VARCHAR(8)),112)
				END AS sls_due_dt,

				CASE 
					WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
						THEN sls_quantity * ABS(sls_price)
					ELSE sls_sales
				END AS sls_sales,

				ABS(NULLIF(sls_quantity,0)) AS sls_quantity,

				CASE
					WHEN sls_price IS NULL OR sls_price <= 0 
						THEN sls_sales / NULLIF(sls_quantity, 0)
					ELSE sls_price
				END AS sls_price
			FROM bronze.crm_sales_details;

			SET  @end_time = GETDATE();
			PRINT '>> Loading Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
			PRINT '---------------';

			-- Cleansing and Insert data into erp_cust_az12

			PRINT '-----------------------------------';
			PRINT 'Loading ERP Tables';
			PRINT '-----------------------------------';

			SET  @end_time = GETDATE();
			PRINT '>> Truncate Table: silver.erp_cust_az12'
			TRUNCATE TABLE silver.erp_cust_az12
			PRINT '>> Insert the data into: silver.erp_cust_az12'
			INSERT INTO silver.erp_cust_az12(
				cid,
				bdate,
				gen)

			SELECT 
				CASE
					WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
					ELSE cid
				END AS cid,

				CASE WHEN bdate > GETDATE() THEN NULL
					ELSE bdate
				END AS bdate,

				CASE 
					WHEN  UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
					WHEN  UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
					ELSE 'Unknown'
				END AS gen
			FROM bronze.erp_cust_az12; 

			SET  @end_time = GETDATE();
			PRINT '>> Loading Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
			PRINT '---------------';

			-- Cleansing and Insert data in erp_loc_a101
			SET  @end_time = GETDATE();
			PRINT '>> Truncate Table: silver.erp_loc_a101'
			TRUNCATE TABLE silver.erp_loc_a101
			PRINT '>> Insert the data into: silver.erp_loc_a101'
			INSERT INTO silver.erp_loc_a101(
				cid,
				cntry)
			SELECT 
				REPLACE(cid,'-','') AS cid,
				CASE 
					WHEN TRIM(cntry) IN ('USA','US') THEN 'United States'
					WHEN TRIM(cntry) = 'UK' THEN 'United Kingdom'
					WHEN TRIM(cntry) = 'DE' THEN 'Germany'
					WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unknown'
					ELSE TRIM(cntry)
				END AS cntry
			FROM bronze.erp_loc_a101;

			SET  @end_time = GETDATE();
			PRINT '>> Loading Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
			PRINT '---------------';

			-- Cleansing and Insert data in erp_px_cat_g1v2
			SET  @end_time = GETDATE();
			PRINT '>> Truncate Table: silver.erp_px_cat_g1v2'
			TRUNCATE TABLE silver.erp_px_cat_g1v2
			PRINT '>> Insert the data into: silver.erp_px_cat_g1v2'
			INSERT INTO silver.erp_px_cat_g1v2(
				id,
				cat,
				subcat,
				maintenance)
			SELECT 
				id,
				cat,
				subcat,
				maintenance
			FROM bronze.erp_px_cat_g1v2;

			SET  @end_time = GETDATE();
			PRINT '>> Loading Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
			PRINT '---------------';

			SET @end_batch = GETDATE();
			PRINT '======================================='
			PRINT ' Loading Silver Layer is Completed';
			PRINT '>> Total Loading Duration: ' + CAST(DATEDIFF(second, @start_batch, @end_batch) AS NVARCHAR) + 'seconds';
			PRINT '======================================='

		END TRY

		BEGIN CATCH

			PRINT '=============================================================';
			PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
			PRINT '============================================================='

	END CATCH

END;

EXEC silver.load_silver
