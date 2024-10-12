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
cte_dim_sr as(
	SELECT DISTINCT
		dd.d_date,
		dd.eoweek,
		oc.sales_rep_code AS sr_code,
		oc.sales_rep_name AS sr_name,
		oc.email as email_sr,
		dts.tsm_code_sid,
		drs.route_code_sid,
		CONCAT(oc.sales_rep_name,oc.email,dts.tsm_code_sid,drs.route_code_sid) as concat_key
	FROM cte_date_rn dd
	LEFT JOIN {{ source('stg_sap','org_chart') }} oc
	  ON dd.year = oc.year
	  AND dd.month = oc.month
	  AND dd.map_week = oc.week
	LEFT JOIN {{ ref('dim_tsm') }} dts
	  ON oc.tsm_code = dts.tsm_code
	  AND dd.d_date >= dts.__start_date AND dd.d_date <= dts.__end_date
	LEFT JOIN {{ ref('dim_route') }} drs 
	  ON oc.route_id_pace = drs.route_code
	  AND dd.d_date >= drs.__start_date AND dd.d_date <= drs.__end_date
	WHERE oc.sales_rep_code IS NOT NULL 
),
cte_pre_sr as(
	SELECT 
		*,
		LAG(concat_key) OVER(PARTITION BY sr_code ORDER BY d_date) as pre_sr_name
	FROM cte_dim_sr 
),
cte_changing_indicator as (
	SELECT 
		*,
		CASE 
			WHEN (pre_sr_name IS NULL) OR (pre_sr_name <> concat_key) THEN 1
			ELSE 0
		END as changing_indicator		
	FROM cte_pre_sr
),
cte_changing_indicator_cumulative as (
	SELECT 
		*,
		SUM(changing_indicator) OVER(PARTITION BY sr_code ORDER BY d_date) as changing_indicator_cumulative
	FROM cte_changing_indicator
),
cte_sr_final as (
	SELECT 
		sr_code,
		sr_name,
		email_sr,
		tsm_code_sid,
		route_code_sid,
		MIN(d_date) as __start_date,
		MAX(d_date) as __end_date
	FROM cte_changing_indicator_cumulative
	GROUP BY
		sr_code,
		sr_name,
		email_sr,
		tsm_code_sid,
		route_code_sid,
		changing_indicator_cumulative
)
SELECT 
	CONCAT(cte_sr_final.sr_code,"|",DATE_FORMAT(__start_date,'yyyyMMdd'),"|",DATE_FORMAT(__end_date,'yyyyMMdd')) AS sr_code_sid,
	cte_sr_final.*,	
	CASE 
		WHEN __end_date >= CURRENT_DATE() THEN 1
		ELSE 0
	END AS __is_active	
FROM cte_sr_final 