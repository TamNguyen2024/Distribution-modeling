SELECT DISTINCT
	dd.`year`,
	dd.`month`,
	dd.week,
	dd.eoweek,
	dd.is_last_week,
	dd.d_date,
	dd.SOL_date,
	customer.region_code,
	customer.ranking_code,
	channel.channel_code,
	customer.customer_code,
	vendor.vendor_code,
	route.route_code,
	1 AS is_service
FROM {{ ref('fact_visit_plan') }} visit_plan 
LEFT JOIN  {{ ref('relationship') }} rel
	ON visit_plan.rel_code = rel.rel_code
LEFT JOIN  {{ ref('dim_route') }} route 
	ON rel.route_code_sid = route.route_code_sid 
LEFT JOIN {{ ref('dim_vendor') }} vendor
	ON rel.vendor_code_sid = vendor.vendor_code_sid 
LEFT JOIN {{ ref('dim_customer') }} customer
	ON rel.customer_code_sid = customer.customer_code_sid 
LEFT JOIN {{ ref('dim_subchannel') }} subchannel
	ON subchannel.sub_channel_code = customer.sub_channel_code
JOIN {{ ref('dim_channel') }}  channel 
	ON subchannel.channel_code = channel.channel_code 
LEFT JOIN {{ ref('dim_ranking') }} ranking 
	ON customer.ranking_code = ranking.ranking_code 
LEFT JOIN {{ ref('dim_date_SOL') }} dd
	ON visit_plan.d_date = dd.SOL_date 
WHERE visit_plan.active = '1'
	AND customer.isactive = '1'
	AND subchannel.sub_channel_code IS NOT NULL 
	AND ranking.ranking_code IS NOT NULL
	AND route.route_code_sid IS NOT NULL    AND route.is_active = 'Yes'
	AND vendor.vendor_code_sid IS NOT NULL
	AND dd.SOL_date IS NOT NULL

