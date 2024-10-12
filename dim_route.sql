WITH cte_date_rn AS(
    SELECT DISTINCT
	    year,
	    month,
	    week,
	    eoweek,
	    d_date,
	    CASE 
	      WHEN (month IN (1,2,4,5,7,8,10,11)) AND week IN (3,4) THEN 3
	      WHEN (month IN (3,6,9,12)) AND week IN (4,5) THEN 4
	      ELSE 1
	    END AS map_week
    FROM {{ ref('dim_date') }}
),
cte_dim_route as (
	SELECT DISTINCT
	  oc.route_id AS route_code,
	  dd.d_date,
	  dd.eoweek,
	  oc.`is_using` AS is_active,
	  oc.channel,
	  oc.target_new_customer,
	  mar.region_code AS region_code,
	  CONCAT(
  		COALESCE(oc.`is_using`,"Blank"),
  		COALESCE(oc.channel,"Blank"),
  		COALESCE(oc.target_new_customer,"Blank"),
  		COALESCE(mar.region_code,"Blank")
	  ) as concat_key
	FROM cte_date_rn dd
	LEFT JOIN {{ source('stg_sap', 'org_chart') }} oc 
	  ON dd.year = oc.year
	  AND dd.month = oc.month
	  AND dd.map_week = oc.week
	LEFT JOIN {{ source('stg_sap', 'mapping_area_region') }} mar 
	  ON oc.region = mar.region_short_name 
	WHERE oc.route_id IS NOT NULL 
),
cte_pre_concat_key as (
	select 
		*,
		LAG(concat_key) OVER(PARTITION BY route_code ORDER BY d_date ASC) as pre_concat_key
	from cte_dim_route
),
cte_changing_indicator as (
	select 
		*,
		CASE 
			WHEN (pre_concat_key <> concat_key OR pre_concat_key IS NULL) THEN 1
			ELSE 0
		END as changing_indicator	
	from cte_pre_concat_key
),
cte_changing_indicator_cumulative as (
	SELECT 
		*,
		SUM(changing_indicator) OVER(PARTITION BY route_code ORDER BY d_date) as changing_indicator_cumsum
	FROM cte_changing_indicator
),
cte_dim_route_final as (
	SELECT 
		route_code,
		is_active,
		channel,
		target_new_customer,
		region_code,
		MIN(d_date) as __start_date,
		MAX(d_date) as __end_date
	FROM cte_changing_indicator_cumulative
	GROUP BY 
		route_code,
		is_active,
		channel,
		target_new_customer,
		region_code,
		changing_indicator_cumsum
)
SELECT 
	CONCAT(route_code,"|",DATE_FORMAT(__start_date,'yyyyMMdd'),"|",DATE_FORMAT(__end_date,'yyyyMMdd') ) as route_code_sid,
	cte_dim_route_final.*,	
	CASE 
		WHEN __end_date >= CURRENT_DATE() THEN 1
		ELSE 0
	END AS __is_active	
FROM cte_dim_route_final 



