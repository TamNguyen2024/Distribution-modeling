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
	LEFT JOIN {{ ref('dim_date_SOL') }} dd
		ON sr.order_date = dd.d_date 
),
	cte_SOL_cumulative_sr AS (
	SELECT DISTINCT 
		sol.year,
		sol.month,
		sol.week,
		sol.d_date,
		sol.ranking_code,
		sol.channel_code,
		sol.region_code,
		sol.route_code,
		sol.customer_code,
		assessment.assessment_segment_code,
		assessment.assessment_group_code,
		sr.sku_group,
		SUM(sr.quantity) OVER(PARTITION BY sol.d_date,sol.customer_code,sr.sku_group) AS qty_con_sku,
		CASE
			WHEN SUM(sr.quantity) OVER(PARTITION BY sol.d_date,sol.customer_code,sr.sku_group) >= assessment.min_qty_ea_mix_bysku THEN 1
			ELSE 0 
		END AS is_pass_con_sku,
		assessment.min_qty_ea_mix AS min_qty_assessment
	FROM {{ ref('daily_service_customer') }} sol
	LEFT JOIN cte_sales_settled sr
		ON sol.customer_code = sr.customer_code
		AND sol.year = sr.year
		AND sol.month = sr.month
		AND sol.d_date >= sr.order_date
	LEFT JOIN {{ ref('assessment_condition_mapping') }} assessment
		ON sol.region_code = assessment.sales_region_code
		AND sol.channel_code = assessment.channel_code
		AND sol.ranking_code = assessment.ranking_code
		AND sr.sku_group = assessment.sku_group
		AND sol.d_date BETWEEN assessment.start_date AND assessment.end_date
	WHERE 1=1
		AND sr.customer_code IS NOT NULL
		AND assessment.sku_group IS NOT NULL
		AND (
			(sol.is_last_week = 0 AND sol.eoweek >= sr.settle_date) 
			OR sol.is_last_week = 1
		)		
),
	cte_is_pass_assessment AS (
	SELECT *
	FROM (
		SELECT DISTINCT 
			year,
			month,
			week,
			d_date,
			ranking_code,
			channel_code,
			region_code,
			route_code,
			customer_code,
			assessment_segment_code,
			assessment_group_code,
			CASE
				WHEN SUM(qty_con_sku) OVER(PARTITION BY d_date,customer_code,assessment_group_code) >= min_qty_assessment THEN 1
				ELSE 0 
			END	AS is_pass_assessment
		FROM cte_SOL_cumulative_sr
		WHERE is_pass_con_sku = 1
	) temp_cte
	WHERE is_pass_assessment = 1
),
	cte_count_is_pass_assessment AS (
	SELECT DISTINCT 
		year,
		month,
		week,
		d_date,
		ranking_code,
		channel_code,
		region_code,
		route_code,
		customer_code,
		assessment_segment_code,
		COUNT(assessment_group_code) OVER(PARTITION BY d_date,customer_code,assessment_segment_code) AS count_assessment_pass
	FROM cte_is_pass_assessment
),
	cte_is_pass_segment AS (
	SELECT 
		cte_count_is_pass_assessment.year,
		cte_count_is_pass_assessment.month,
		cte_count_is_pass_assessment.week,
		cte_count_is_pass_assessment.d_date,
		cte_count_is_pass_assessment.ranking_code,
		cte_count_is_pass_assessment.channel_code,
		cte_count_is_pass_assessment.region_code,
		cte_count_is_pass_assessment.route_code,
		cte_count_is_pass_assessment.customer_code,
		cte_count_is_pass_assessment.assessment_segment_code,
		CASE 
			WHEN COUNT(DISTINCT assessment.assessment_group_code) = cte_count_is_pass_assessment.count_assessment_pass THEN 1
			ELSE 0
		END	AS is_pass_segment
	FROM cte_count_is_pass_assessment
	LEFT JOIN {{ ref('assessment_condition_mapping') }} assessment
		ON cte_count_is_pass_assessment.region_code = assessment.sales_region_code
		AND cte_count_is_pass_assessment.channel_code = assessment.channel_code
		AND cte_count_is_pass_assessment.ranking_code = assessment.ranking_code
		AND cte_count_is_pass_assessment.assessment_segment_code = assessment.assessment_segment_code
		AND cte_count_is_pass_assessment.d_date BETWEEN assessment.start_date AND assessment.end_date
	GROUP BY 
		cte_count_is_pass_assessment.year,
		cte_count_is_pass_assessment.month,
		cte_count_is_pass_assessment.week,
		cte_count_is_pass_assessment.d_date,
		cte_count_is_pass_assessment.ranking_code,
		cte_count_is_pass_assessment.channel_code,
		cte_count_is_pass_assessment.region_code,
		cte_count_is_pass_assessment.route_code,
		cte_count_is_pass_assessment.customer_code,
		cte_count_is_pass_assessment.assessment_segment_code,
		cte_count_is_pass_assessment.count_assessment_pass
)
SELECT DISTINCT 
    year,
    month,
    week,
    d_date,
    ranking_code,
    channel_code,
    region_code,
    route_code,
    customer_code,
    assessment_segment_code AS segment_pass,
	is_pass_segment,
	CASE 
		WHEN assessment_segment_code = 'COR' THEN 1
		ELSE 0 
	END AS is_achieve_cor,
	CASE 
		WHEN assessment_segment_code = 'REG' THEN 1
		ELSE 0 
	END AS is_achieve_reg,
	CASE 
		WHEN assessment_segment_code = 'STR' THEN 1
		ELSE 0 
	END AS is_achieve_str,
	CASE 
		WHEN assessment_segment_code = 'EDR' THEN 1
		ELSE 0 
	END AS is_achieve_edr
FROM cte_is_pass_segment
WHERE is_pass_segment = 1 