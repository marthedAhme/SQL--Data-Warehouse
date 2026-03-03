/* ============================================================
   Silver Layer – Cleansed & Standardized Tables Initialization

   Purpose:
   - Recreate refined tables for CRM and ERP source systems.
   - Existing tables are dropped to ensure a clean structure.
   - Tables store cleansed, standardized, and transformed data.
   - Basic data quality rules are applied (null handling, mapping, formatting).
   - Data is structured and prepared for downstream integration
     into the Gold analytical layer.

   Notes:
   - No complex aggregations are performed at this stage.
   - Business logic is partially applied.
   - Data is conformed across multiple sources where applicable.
   ============================================================ */

---------------------------
-- CRM: Customer Information
---------------------------

IF OBJECT_ID ('silver.crm_cust_info','U') IS NOT NULL 
	DROP TABLE silver.crm_cust_info

Go
  
CREATE TABLE silver.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE,
	dwh_create_date DATETIME DEFAULT GETDATE()

);

Go
---------------------------
-- CRM: Product Information
---------------------------

IF OBJECT_ID ('silver.crm_prd_info','U') IS NOT NULL 
	DROP TABLE silver.crm_prd_info

Go
  
CREATE TABLE silver.crm_prd_info (
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME DEFAULT GETDATE()
);

Go
---------------------------
-- CRM: Sales Details (Transactional Data)
---------------------------

IF OBJECT_ID ('silver.crm_sales_details','U') IS NOT NULL 
	DROP TABLE silver.crm_sales_details

Go
  
CREATE TABLE silver.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME DEFAULT GETDATE()
);

Go

---------------------------
-- ERP: Customer Demographics
---------------------------

IF OBJECT_ID ('silver.erp_cust_az12','U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12

Go

CREATE TABLE silver.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50),
	dwh_create_date DATETIME DEFAULT GETDATE()
);

Go

---------------------------
-- ERP: Customer Location
---------------------------

IF OBJECT_ID ('silver.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101

Go

CREATE TABLE silver.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_create_date DATETIME DEFAULT GETDATE()
);

Go
---------------------------
-- ERP: Product Category
---------------------------

IF OBJECT_ID ('silver.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2

Go
  
CREATE TABLE silver.erp_px_cat_g1v2 (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_create_date DATETIME DEFAULT GETDATE()
);

