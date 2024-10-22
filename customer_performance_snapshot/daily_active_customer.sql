WITH cte_sales_settled AS (
	SELECT DISTINCT 
		dd.`year`,
		dd.`month`, 
		dd.week,
		dd.eoweek,
		sr.order_date,
		sr.settle_date,
		sr.order_no,
		customer.customer_code,
		product.sku_group_code,
		product.product_code,
		sr.quantity,
		sr.nsr
	FROM {{ ref('int_sales_settled') }} sr 
	LEFT JOIN {{ ref('dim_product') }} product
		ON sr.product_code_sid = product.product_code_sid 
	LEFT JOIN {{ ref('relationship_customer_vendor') }} customer_vendor
		ON sr.rel_customer_vendor_code = customer_vendor.rel_customer_vendor_code
	LEFT JOIN {{ ref('dim_customer') }} customer
		ON customer_vendor.customer_code_sid = customer.customer_code_sid
	LEFT JOIN {{ ref('dim_date') }} dd
		ON sr.order_date = dd.d_date 
),
	cte_SOL_cumulative_sr AS (
	SELECT DISTINCT 
		sol.d_date,
		sol.customer_code,
		sr.sku_group_code,
		CASE
			WHEN SUM(sr.quantity) OVER(PARTITION BY sol.d_date,sol.customer_code,sr.sku_group_code) >= con.qty_condition THEN 1
			ELSE 0 
		END AS is_active 
	FROM {{ ref('daily_service_customer') }} sol
	LEFT JOIN cte_sales_settled sr
		ON sol.customer_code = sr.customer_code
		AND sol.year = sr.year
		AND sol.month = sr.month
		AND sol.d_date >= sr.order_date
	LEFT JOIN {{ ref('dim_qty_condition') }} con
		ON sol.d_date BETWEEN con.start_date AND con.end_date
	WHERE 1=1
		AND sr.customer_code IS NOT NULL
		AND (
			(sol.is_last_week = 0 AND sol.eoweek >= sr.settle_date) 
			OR sol.is_last_week = 1
		)		
)
SELECT DISTINCT 
	d_date,
    customer_code,
	1 as is_active
FROM cte_SOL_cumulative_sr 
WHERE 
	is_active = 1