/* ============================================================
   Bronze Layer – Source Tables Initialization
   Purpose:
   - Recreate raw staging tables for CRM and ERP source systems.
   - Existing tables are dropped to ensure a clean structure.
   - Tables store untransformed source data (no constraints/indexes).
   ============================================================ */

---------------------------
-- CRM: Customer Information
---------------------------

IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL 
	DROP TABLE bronze.crm_cust_info

Go
  
CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE

);

Go
---------------------------
-- CRM: Product Information
---------------------------

IF OBJECT_ID ('bronze.crm_prd_info','U') IS NOT NULL 
	DROP TABLE bronze.crm_prd_info

Go
  
CREATE TABLE bronze.crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);

Go
---------------------------
-- CRM: Sales Details (Transactional Data)
---------------------------

IF OBJECT_ID ('bronze.crm_sales_details','U') IS NOT NULL 
	DROP TABLE bronze.crm_sales_details

Go
  
CREATE TABLE bronze.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

Go

---------------------------
-- ERP: Customer Demographics
---------------------------

IF OBJECT_ID ('bronze.erp_cust_az12','U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12

Go

CREATE TABLE bronze.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
);

Go

---------------------------
-- ERP: Customer Location
---------------------------

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101

Go

CREATE TABLE bronze.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);

Go
---------------------------
-- ERP: Product Category
---------------------------

IF OBJECT_ID ('bronze.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2

Go
  
CREATE TABLE bronze.erp_px_cat_g1v2 (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
);
