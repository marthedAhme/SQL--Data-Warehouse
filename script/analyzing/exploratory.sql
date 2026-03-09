/*
============================
EXPLORATION OF THE DATABASE
============================
*/
USE DataWarehouse;
GO
-- Explore all objects in the database

SELECT * FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'gold';

-- Explore all columns in the database

-- For dim_customers
SELECT * FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_customers';

-- For dim_products
SELECT * FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_products';

-- For fact sales
SELECT * FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'fact_sales';

/*
============================
EXPLORATION DIMENSIONS
============================
*/

-- For dim_customers Table
-- country
SELECT DISTINCT country FROM gold.dim_customers;

-- marital status
SELECT DISTINCT marital_status FROM gold.dim_customers;

-- gender
SELECT DISTINCT gender FROM gold.dim_customers;

-- For dim_products

-- category, sub_categorty and product_name
SELECT DISTINCT category, subcategorty, product_name FROM gold.dim_products
ORDER BY 1,2,3;

-- product line
SELECT DISTINCT product_line FROM gold.dim_products;


/*
============================
EXPLORATION DATE
============================
*/

-- birthday
SELECT 
	MIN(birthday) AS earliest_date,
	DATEDIFF(YEAR,MIN(birthday),GETDATE()) AS oldest_age,
	MAX(birthday) AS latest_date,
	DATEDIFF(YEAR,MAX(birthday),GETDATE()) AS youngest_age
FROM gold.dim_customers;
-- Create date
SELECT 
	MIN(create_date) AS earliest_date,
	MAX(create_date) AS latest_date
FROM gold.dim_customers;
-- start date
SELECT 
	MIN(start_date) AS earliest_date,
	MAX(start_date) AS latest_date
FROM gold.dim_products;

-- Order date
SELECT 
	MIN(order_date) AS first_order_date,
	MAX(order_date) AS last_order_date,
	DATEDIFF(YEAR,MIN(order_date),MAX(order_date)) AS order_range_years
FROM gold.fact_sales;
-- shipping date
SELECT 
	MIN(shipping_date) AS first_shipping_date,
	MAX(shipping_date) AS last_shipping_date,
	DATEDIFF(YEAR,MIN(shipping_date),MAX(shipping_date)) AS shipping_ragne_years
FROM gold.fact_sales;
-- duration date
SELECT 
	MIN(due_date) AS earliest_date,
	MAX(due_date) AS latest_date,
	DATEDIFF(YEAR,MIN(due_date),MAX(due_date)) AS due_range_years
FROM gold.fact_sales;


/*
============================
EXPLORATION DATE
============================
*/

/* 
 Total Sales
 Items are sold
 Average silling price
 Total number of Orders
 Total number of product
 Total number of customer
 Total number of customer has order
*/


SELECT 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quanitiy' AS measure_name, SUM(quanitiy) AS measure_value FROM gold.fact_sales
UNION All
SELECT 'Average Price' AS measure_name, AVG(price) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Orders' AS measure_name, COUNT(DISTINCT order_number) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Products' AS measure_name, COUNT(DISTINCT product_name) AS measure_value FROM gold.dim_products
UNION ALL
SELECT 'Total Nr. Customers' AS measure_name, COUNT(customer_key) AS measure_value FROM gold.dim_customers
UNION ALL
SELECT 'Total Nr. Customer Has Order' AS measure_name, COUNT(DISTINCT customer_id) AS measure_value FROM gold.fact_sales;


/*
================================
MAGNITUDE ANALYZING
================================
*/

-- Total customers by countries

SELECT 
	country,
	COUNT(customer_id) AS total_customers
FROM gold.dim_customers
GROUP BY country
ORDER BY total_customers DESC;

-- Total customers by gender
SELECT 
	gender,
	COUNT(customer_id) AS total_customers
FROM gold.dim_customers
GROUP BY gender
ORDER BY total_customers DESC;

-- Total product by category
SELECT 
	category,
	COUNT(product_key) AS total_products
FROM gold.dim_products
GROUP BY category
ORDER BY total_products DESC;

-- Average cost in each category
SELECT 
	category,
	AVG(cost) AS average_cost
FROM gold.dim_products
GROUP BY category
ORDER BY average_cost DESC;

-- Total revenue for each category
SELECT 
	p.category,
	SUM(s.sales_amount) AS total_revenue
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON s.product_key = p.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;

-- Total revenue for each customers
SELECT 
	c.customer_key,
	c.first_name,
	c.last_name,
	SUM(s.sales_amount) AS total_revenue
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_id = c.customer_id
GROUP BY
	c.customer_key,
	c.first_name,
	c.last_name
ORDER BY total_revenue DESC;
 
-- Distribution of sold items across countries
SELECT 
	c.country,
	SUM(s.quanitiy) AS total_sold_items
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_id = c.customer_id
GROUP BY c.country
ORDER BY total_sold_items DESC;

/*
================================
RANKING ANALYZING
================================
*/

-- Which 5 Products generate the highest revenue

SELECT TOP 5
	p.product_name,
	SUM(s.sales_amount) AS total_revenue
FROM gold.fact_sales s
	LEFT JOIN gold.dim_products p
	ON s.product_key = p.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC;

-- anther way
SELECT 
	product_name,
	total_revenue
FROM (
	SELECT 
		p.product_name,
		SUM(s.sales_amount) AS total_revenue,
		RANK() OVER (ORDER BY SUM(s.sales_amount) DESC) AS rank_products
	FROM gold.fact_sales s
		LEFT JOIN gold.dim_products p
		ON s.product_key = p.product_key
	GROUP BY p.product_name
) t

WHERE rank_products <= 5
-- What are the 5 worst-performing product in terms sales

SELECT TOP 5
	p.product_name,
	SUM(s.sales_amount) AS total_revenue
FROM gold.fact_sales s
	LEFT JOIN gold.dim_products p
	ON s.product_key = p.product_key
GROUP BY p.product_name
ORDER BY total_revenue ;

-- The top 10 customers who have the highest revenue
SELECT TOP 10
	c.customer_key,
	c.first_name,
	c.last_name,
	SUM(s.sales_amount) AS total_revenue
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_id = c.customer_id
GROUP BY
	c.customer_key,
	c.first_name,
	c.last_name
ORDER BY total_revenue DESC;

-- The 3 customers with the fewest orders placed

SELECT TOP 3
	c.customer_key,
	c.first_name,
	c.last_name,
	COUNT(DISTINCT order_number) AS total_orders
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_id = c.customer_id
GROUP BY
	c.customer_key,
	c.first_name,
	c.last_name
ORDER BY total_orders;
