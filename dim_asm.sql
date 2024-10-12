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
cte_dim_asm as(
	SELECT DISTINCT
		dd.d_date,
		dd.eoweek,
		oc.asm_code,
		oc.asm_name
	FROM cte_date_rn dd
	LEFT JOIN {{ source('stg_sap', 'org_chart') }} oc
	  ON dd.year = oc.year
	  AND dd.month = oc.month
	  AND dd.map_week = oc.week
	WHERE oc.asm_code IS NOT NULL 
),
cte_pre_asm as(
	SELECT 
		*,
		LAG(asm_name) OVER(PARTITION BY asm_code ORDER BY d_date) as pre_asm_name
	FROM cte_dim_asm 
),
cte_changing_indicator as (
	SELECT 
		*,
		CASE 
			WHEN (pre_asm_name IS NULL) OR (pre_asm_name <> asm_name) THEN 1
			ELSE 0
		END as changing_indicator		
	FROM cte_pre_asm
),
cte_changing_indicator_cumulative as (
	SELECT 
		*,
		SUM(changing_indicator) OVER(PARTITION BY asm_code ORDER BY d_date) as changing_indicator_cumulative
	FROM cte_changing_indicator
),
cte_asm_final as (
	SELECT 
		asm_code,
		asm_name,
		MIN(d_date) as __start_date,
		MAX(d_date) as __end_date
	FROM cte_changing_indicator_cumulative
	GROUP BY 
		asm_code,
		asm_name,
		changing_indicator_cumulative
)
SELECT 
	CONCAT(asm_code,"|",DATE_FORMAT(__start_date,'yyyyMMdd'),"|",DATE_FORMAT(__end_date,'yyyyMMdd')) as asm_code_sid,
	cte_asm_final.*,
	CASE 
		WHEN __end_date >= CURRENT_DATE() THEN 1
		ELSE 0
	END as __is_active	
FROM cte_asm_final 








