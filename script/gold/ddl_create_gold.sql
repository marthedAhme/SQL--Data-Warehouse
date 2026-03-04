/* ============================================================  
   Gold Layer – Business-Ready Analytical Tables Initialization  

   Purpose:  
   - Recreate final analytical tables for reporting and BI consumption.  
   - Existing tables are dropped to ensure a clean and optimized structure.  
   - Tables store aggregated, integrated, and business-ready data.  
   - Advanced business logic and calculations are fully applied.  
   - Data is modeled using dimensional techniques (Fact & Dimension tables).  
   - Optimized for high-performance querying and analytical workloads.  

   Notes:  
   - Data is sourced from the Silver layer (cleansed & standardized data).  
   - Aggregations, KPIs, and derived metrics are implemented at this stage.  
   - Designed to support dashboards, reports, and executive decision-making.  
   - Structure typically follows Star Schema or other analytical modeling approaches.  
   ============================================================ */

---------------------------
-- Create Dimension: dim_customer (view)
---------------------------
IF OBJECT_ID ('gold.dim_customers','V') IS NOT NULL
	DROP VIEW gold.dim_customers
GO

CREATE VIEW gold.dim_customers AS 

SELECT

	ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	cl.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'Unkown' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen,'Unkown')
	END AS gender,
	ca.bdate AS birthday,
	ci.cst_create_date AS create_date

FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 cl
	ON ci.cst_key = cl.cid;

GO

---------------------------
-- Create Dimension: dim_customer (view)
---------------------------
IF OBJECT_ID ('gold.dim_products','V') IS NOT NULL
	DROP VIEW gold.dim_products
GO

CREATE VIEW gold.dim_products AS 
SELECT 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategorty,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL -- Filter out all Historical date

GO

---------------------------
-- Create Fact: fact_sales (view)
---------------------------
IF OBJECT_ID ('gold.fact_sales','V') IS NOT NULL
	DROP VIEW gold.fact_sales
GO

CREATE VIEW gold.fact_sales AS 
SELECT 
	sd.sls_ord_num AS order_number,
	dp.product_key,
	dc.customer_id,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quanitiy,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers dc
ON sd.sls_cust_id = dc.customer_id
LEFT JOIN gold.dim_products dp
ON sd.sls_prd_key = dp.product_number
