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
		product.consumer_sku,
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
	LEFT JOIN {{ ref('dim_date_SOL') }} dd
		ON sr.order_date = dd.d_date 
),
	cte_SOL_cumulative_sr AS (
	SELECT DISTINCT 
		sol.d_date,
		sol.SOL_date,
		sol.customer_code,
		sr.product_code,
        SUM(sr.quantity) AS quantity_cumulative,
        SUM(sr.nsr) AS nsr_cumulative
	FROM {{ ref('daily_service_customer') }} sol
	LEFT JOIN cte_sales_settled sr
		ON sol.customer_code = sr.customer_code
		AND sol.year = sr.year
		AND sol.month = sr.month
		AND sol.d_date >= sr.order_date
	WHERE 1=1
		AND sr.customer_code IS NOT NULL
		AND (
			(sol.is_last_week = 0 AND sol.eoweek >= sr.settle_date) 
			OR sol.is_last_week = 1
		)		
    GROUP BY 
        sol.d_date,
		sol.SOL_date,
		sol.customer_code,
		sr.product_code
)
SELECT 
	cte_SOL_cumulative_sr.d_date AS cumulative_date,
	rel.rel_code,
	product.product_code_sid,
	cte_SOL_cumulative_sr.quantity_cumulative,
	cte_SOL_cumulative_sr.nsr_cumulative
FROM cte_SOL_cumulative_sr
LEFT JOIN {{ ref('dim_customer') }} customer
    ON cte_SOL_cumulative_sr.customer_code = customer.customer_code
    AND cte_SOL_cumulative_sr.SOL_date BETWEEN  customer.__start_date AND customer.__end_date
LEFT JOIN {{ ref('relationship') }} rel
	ON customer.customer_code_sid = rel.customer_code_sid
	AND cte_SOL_cumulative_sr.SOL_date BETWEEN  rel.__start_date AND rel.__end_date
LEFT JOIN {{ ref('dim_product') }} product
	ON cte_SOL_cumulative_sr.product_code = product.product_code
	and cte_SOL_cumulative_sr.d_date BETWEEN  product.__start_date AND product.__end_date