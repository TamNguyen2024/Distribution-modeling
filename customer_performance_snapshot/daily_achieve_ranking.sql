WITH cte_count_is_pass_segment AS (
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
		COUNT(segment_pass) OVER(PARTITION BY d_date,customer_code) AS count_segment_pass
	FROM {{ ref('daily_achieve_segment') }}
),
	cte_is_pass_ranking AS (
	SELECT 
		cte_count_is_pass_segment.year,
		cte_count_is_pass_segment.month,
		cte_count_is_pass_segment.week,
		cte_count_is_pass_segment.d_date,
		cte_count_is_pass_segment.ranking_code,
		cte_count_is_pass_segment.channel_code,
		cte_count_is_pass_segment.region_code,
		cte_count_is_pass_segment.route_code,
		cte_count_is_pass_segment.customer_code,
		CASE 
			WHEN COUNT(DISTINCT assessment.assessment_segment_code) = cte_count_is_pass_segment.count_segment_pass THEN 1
			ELSE 0
		END	AS is_pass_ranking
	FROM cte_count_is_pass_segment
	LEFT JOIN {{ ref('assessment_condition_mapping') }} assessment
		ON cte_count_is_pass_segment.region_code = assessment.sales_region_code
		AND cte_count_is_pass_segment.channel_code = assessment.channel_code
		AND cte_count_is_pass_segment.ranking_code = assessment.ranking_code
		AND cte_count_is_pass_segment.d_date BETWEEN assessment.start_date AND assessment.end_date
	GROUP BY 
		cte_count_is_pass_segment.year,
		cte_count_is_pass_segment.month,
		cte_count_is_pass_segment.week,
		cte_count_is_pass_segment.d_date,
		cte_count_is_pass_segment.ranking_code,
		cte_count_is_pass_segment.channel_code,
		cte_count_is_pass_segment.region_code,
		cte_count_is_pass_segment.route_code,
		cte_count_is_pass_segment.customer_code,
		cte_count_is_pass_segment.count_segment_pass
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
	is_pass_ranking
FROM cte_is_pass_ranking
WHERE is_pass_ranking = 1 