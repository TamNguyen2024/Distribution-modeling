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
cte_dim_tsm as(
	SELECT DISTINCT
		dd.d_date,
		dd.eoweek,
		oc.tsm_code,
		oc.tsm_name,
		das.asm_code_sid,
		CONCAT(oc.tsm_name,das.asm_code_sid) AS concat_key
	FROM cte_date_rn dd
	LEFT JOIN {{ source('stg_sap', 'org_chart') }} oc
	  ON dd.year = oc.year
	  AND dd.month = oc.month
	  AND dd.map_week = oc.week
    LEFT JOIN {{ ref('dim_asm') }} das
	  ON oc.asm_code = das.asm_code
	  AND dd.d_date >= das.__start_date AND dd.d_date <= das.__end_date
	WHERE oc.tsm_code IS NOT NULL 
),
cte_pre_tsm as(
	SELECT 
		*,
		LAG(concat_key) OVER(PARTITION BY tsm_code ORDER BY d_date) AS pre_tsm_name
	FROM cte_dim_tsm 
),
cte_changing_indicator as (
	SELECT 
		*,
		CASE 
			WHEN (pre_tsm_name IS NULL) OR (pre_tsm_name <> concat_key) THEN 1
			ELSE 0
		END AS changing_indicator		
	FROM cte_pre_tsm
),
cte_changing_indicator_cumulative as (
	SELECT 
		*,
		SUM(changing_indicator) OVER(PARTITION BY tsm_code ORDER BY d_date) as changing_indicator_cumulative
	FROM cte_changing_indicator
),
cte_tsm_final as (
	SELECT 
		tsm_code,
		tsm_name,
		asm_code_sid,
		MIN(d_date) as __start_date,
		MAX(d_date) as __end_date
	FROM cte_changing_indicator_cumulative
	GROUP BY 
		tsm_code,
		tsm_name,
		asm_code_sid,
		changing_indicator_cumulative
)
SELECT 
	CONCAT(tsm_code,"|",DATE_FORMAT(__start_date,'yyyyMMdd'),"|",DATE_FORMAT(__end_date,'yyyyMMdd')) as tsm_code_sid,
	cte_tsm_final.*,
	CASE 
		WHEN __end_date >= CURRENT_DATE() THEN 1
		ELSE 0
	END AS __is_active	
FROM cte_tsm_final 