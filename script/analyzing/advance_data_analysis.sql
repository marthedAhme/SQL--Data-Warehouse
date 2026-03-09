/*
====================================
CHANGE OVER TIME
====================================
*/
-- Change Over Year
SELECT 
	YEAR(order_date) AS Year,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_id) AS total_customers,
	COUNT(quanitiy) AS total_quanitiy
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY YEAR(order_date)

-- Change Over Month
SELECT
	YEAR(order_date) AS Year,
	DATENAME(Month,order_date) AS Month,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_id) AS total_customers,
	SUM(quanitiy) AS total_quanitiy
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY 
	YEAR(order_date),
	MONTH(order_date),
	DATENAME(MONTH,order_date)
ORDER BY 
	YEAR(order_date),
	MONTH(order_date)

-- Another format
SELECT
	FORMAT(order_date,'yyyy-MMM') AS order_date,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_id) AS total_customers,
	SUM(quanitiy) AS total_quanitiy
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY 
	YEAR(order_date),
    MONTH(order_date),
	FORMAT(order_date,'yyyy-MMM')
ORDER BY
    YEAR(order_date),
    MONTH(order_date)


/*
====================================
CUMULATIVE ANALYSIS
====================================
*/
SELECT
	order_month,
	total_sales,
	SUM(total_sales) OVER (ORDER BY order_month) AS running_sales,
	AVG(avg_price) OVER (ORDER BY order_month) AS moving_average
FROM (
SELECT
	YEAR(order_date) AS order_month,
	SUM(sales_amount) AS total_sales,
	AVG(price) AS avg_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY 
	YEAR(order_date)
)t

/*
====================================
PERFORMANCE ANALYSIS
====================================
*/
	WITH yearly_product_sales AS (
	SELECT 
		YEAR(s.order_date) AS order_year,
		p.product_name,
		SUM(s.sales_amount) AS current_sales
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_products p
	ON s.product_key = p.product_key
	WHERE s.order_date IS NOT NULL
	GROUP BY 
		YEAR(s.order_date),
		p.product_name
)

SELECT 
	order_year,
	product_name,
	current_sales,
	AVG(current_sales) OVER(PARTITION BY product_name) AS avg_sales,
	current_sales - AVG(current_sales) OVER(PARTITION BY product_name ) AS different_sales,
	CASE 
		WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name )  > 0 THEN  'Above Average'
		WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name )  < 0 THEN  'Below Average'
		ELSE 'Average'
	END AS avg_change,
	-- Year Over Year
	LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year)  AS py_salse,
	current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS diff_py,
		CASE 
		WHEN current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) > 0 THEN  'Increase'
		WHEN current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) < 0 THEN  'Decrease'
		ELSE 'Average'
	END AS avg_change

FROM yearly_product_sales
ORDER BY product_name, order_year

/*
====================================
PART TO WHOLE ANALYSIS
====================================
*/
SELECT 
	country,
	total_sales,
	CONCAT(ROUND((CAST(total_sales AS FLOAT) * 100/ SUM(total_sales) OVER ()),2),'%')AS percent_sales
FROM (
SELECT 
	c.country,
	SUM(s.sales_amount) AS total_sales
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_id = c.customer_id
GROUP BY c.country)
t
ORDER BY total_sales DESC;


WITH category_sales AS (
SELECT	
	p.category,
	SUM(s.sales_amount) AS total_sales
FROM gold.fact_sales s
LEFT JOIN gold.dim_products	p
ON s.product_key = p.product_key
GROUP BY p.category
)

SELECT 
	category,
	total_sales,
	SUM(total_sales) OVER() AS overall_sales,
	CONCAT(ROUND((CAST(total_sales AS FLOAT)/SUM(total_sales) OVER() * 100),2),'%') AS percent_sales
FROM category_sales
ORDER BY total_sales DESC;

/*
====================================
DATA SEGMENTATION ANALYSIS
====================================
*/
SELECT 
	segment,
	segmentations,
	CONCAT(ROUND((CAST(segmentations AS FLOAT)/SUM(segmentations) OVER () * 100),2),'%') AS percent_cost

FROM (
SELECT 
	COUNT(product_key) AS segmentations,
	CASE 
		WHEN cost < 100 THEN 'Below 100'
		WHEN cost BETWEEN 100 AND 500 THEN '500-100'
		WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
		ELSE 'Above 1000'
	END AS segment
FROM gold.dim_products
	GROUP BY 
		CASE 
			WHEN cost < 100 THEN 'Below 100'
			WHEN cost BETWEEN 100 AND 500 THEN '500-100'
			WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
			ELSE 'Above 1000'
		END ) t
ORDER BY percent_cost DESC;
----
-- CTE
WITH customer_spending AS (
	SELECT 
		c.customer_key,
		SUM(s.sales_amount) AS total_spending,
		MIN(s.order_date) AS first_order,
		MAX(s.order_date) AS last_order,
		DATEDIFF(MONTH,MIN(order_date), MAX(order_date)) AS lifespan
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_customers c
	ON s.customer_id = c.customer_id
	GROUP BY c.customer_key
)

SELECT 
	customer_segment,
	COUNT(customer_key) AS total_customers
FROM (
	SELECT 
		customer_key,
		CASE 
			WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
			WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
			ELSE 'New'
		END AS customer_segment
	FROM customer_spending
)t
GROUP BY customer_segment
ORDER BY total_customers DESC;




