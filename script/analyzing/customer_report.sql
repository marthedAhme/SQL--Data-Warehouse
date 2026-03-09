/*
=====================================================================================
Customer Report
=====================================================================================

Purpose:
	- This report consolidates key customer metrics and behaviors

Highlights:
	1. Gather essential fields such as name, age, and transcation details.
	2. segments customers into categories (VIP, Regular, New) and age groups.
	3. Aggregates customers - level metrics:
		- total orders
		- total sales
		- total quantity purchased
		- total products
		-lifespan (in months)
	4. Calculates valuable KIPs:
		- recency (month since last order)
		- average order value
		- average monthly spend
=======================================================================================
*/

CREATE VIEW gold.report_customers AS
WITH base_query AS (
/*-------------------------------------------------------------------------------------
1) Base Query: Retrieves core columns from tables
---------------------------------------------------------------------------------------*/
	SELECT 
		s.order_number,
		s.product_key,
		s.order_date,
		s.sales_amount,
		s.quanitiy,
		c.customer_key,
		c.customer_number,
		CONCAT(c.first_name,' ',c.last_name) AS customer_name,
		DATEDIFF(YEAR,c.birthday,GETDATE()) AS age
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_customers c
		ON s.customer_id = c.customer_id
	WHERE s.order_date  IS NOT NULL
)

,customer_aggregation AS (
/*-------------------------------------------------------------------------------------
2) Customer Aggregation: Summarize Key metrics at the customer level
---------------------------------------------------------------------------------------*/
	SELECT 
		customer_key,
		customer_number,
		customer_name,
		age,
		COUNT(DISTINCT order_number) AS total_orders,
		SUM(sales_amount) AS total_sales,
		SUM(quanitiy) AS total_quanitiy,
		COUNT(DISTINCT product_key) AS total_products,
		MAX(order_date) AS last_order_date,
		DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan

	FROM base_query
	GROUP BY 
		customer_key,
		customer_number,
		customer_name,
		age
)

SELECT 
	customer_key,
	customer_number,
	customer_name,
	age,
	CASE 
		WHEN age < 20 THEN 'Under 20'
		WHEN age BETWEEN 20 AND 29 THEN '20-29'
		WHEN age BETWEEN 30 AND 39 THEN '30-39'
		WHEN age BETWEEN 40 AND 49 THEN '40-49'
		ELSE '50 and above'
	END AS age_group,
	CASE
		WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
		WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
		ELSE 'New'
	END AS customer_segment,
	total_orders,
	last_order_date,
	DATEDIFF(MONTH,last_order_date,GETDATE()) AS recency,
	total_sales,
	total_quanitiy,
	total_products,
	lifespan,
	-- Compuate average order value (AVO)
	CASE WHEN total_orders = 0 THEN 0
		 ELSE total_sales/total_orders 
	END AS AVO,
	-- Compuate average monthly spend
	CASE WHEN lifespan = 0 THEN total_sales
		 ELSE total_sales/lifespan
	END AS avg_month_spend
FROM customer_aggregation

