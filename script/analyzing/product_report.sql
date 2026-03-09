/*
=============================================================================================
Product Report
=============================================================================================
Purpose:
	- This report consolidates key product metrics and behaviors.

Highlights:
	1. Gather essential fields such as product name, caterogy,subcategory, and cost.
	2. segments products by revenue to identify High-Performers, Mid-Performers, or Low-performers.
	3. Aggregates products - level metrics:
		- total orders
		- total sales
		- total quantity sold
		- total customers (unique)
		-lifespan (in months)
	4. Calculates valuable KIPs:
		- recency (month since last order)
		- average order revenue
		- average monthly revenue
=======================================================================================
*/

CREATE OR ALTER VIEW gold.product_report AS
WITH base_query AS (
/*-------------------------------------------------------------------------------------
1) Base Query: Retrieves core columns from tables
---------------------------------------------------------------------------------------*/
	SELECT 
		s.order_number,
		p.product_key,
		p.product_number,
		s.customer_id,
		p.product_name,
		p.category,
		p.subcategorty,
		p.cost,
		s.order_date,
		s.quanitiy,
		s.sales_amount
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_products p
		ON s.product_key = p.product_key
	WHERE s.order_date IS NOT NULL
)

, aggergate_product AS (
/*-------------------------------------------------------------------------------------
2) Products Aggregation: Summarize Key metrics at the product level
---------------------------------------------------------------------------------------*/
	SELECT 
		product_key,
		product_number,
		product_name,
		category,
		subcategorty,
		cost,
		MAX(order_date) AS last_order_date,
		COUNT(DISTINCT order_number) AS total_order,
		COUNT(product_key) AS total_products,
		SUM(sales_amount) AS total_sales,
		SUM(quanitiy) AS total_quantity,
		COUNT(DISTINCT customer_id) AS total_customers,
		DATEDIFF(MONTH,MIN(order_date),MAX(order_date)) AS lifespan,
		ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quanitiy,0)),1) AS avg_selling_price
	FROM base_query
	GROUP BY 
		product_key,
		product_number,
		product_name,
		category,
		subcategorty,
		cost
)

SELECT 
		product_key,
		product_number,
		product_name,
		category,
		subcategorty,
		cost,
		last_order_date,
		lifespan,
		DATEDIFF(MONTH,last_order_date,GETDATE()) AS recency,
		CASE 
			WHEN lifespan >= 12 AND total_sales > 50000 THEN 'High-performers'
			WHEN lifespan >= 12 AND total_sales <= 10000 THEN 'Mid-performers'
			ELSE 'Low-performers'
		END AS product_segment,
		total_customers,
		total_sales,
		total_order,
		total_products,
		total_quantity,
		avg_selling_price,
		-- Average Order Revenue (AOR)
		CASE WHEN total_order = 0 THEN 0
			 ELSE total_sales/total_order
		END AS AOR,
		-- Average monthly spend (AMS)
		CASE WHEN lifespan = 0 THEN total_sales
			 ELSE total_sales/lifespan
		END AS AMS
FROM aggergate_product

