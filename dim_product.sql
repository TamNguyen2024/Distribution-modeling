WITH cte_dim_products AS (
	SELECT DISTINCT
		sku.sku_code AS d_product_code,
		sku.sku_description AS product_name,
		sku.sku_group AS sku_group_code,
		sku.sku_category AS product_brand_name,
		cg.sku_category_group AS product_brand_group_name,
		sku.units_of_measure AS packtype,
		sku.pack_size AS packsize,
		sku.conversion AS bottle_convert,
		CONCAT(
			COALESCE(sku.sku_description,'Blank'),
			COALESCE(sku.sku_group,'Blank'),
			COALESCE(sku.sku_category,'Blank'),
			COALESCE(cg.sku_category_group,'Blank'),
			COALESCE(sku.units_of_measure,'Blank'),
			COALESCE(sku.pack_size,'Blank'),
			COALESCE(sku.conversion,'Blank')        	
		) AS concat_key,
		__export_date AS d_date 
	FROM {{ source('stg_sap','sku_master') }} sku
	LEFT JOIN {{ source('stg_sap', 'sku_category_group') }} cg 
		ON sku.sku_category = cg.sku_category
),
cte_pre_concat_key as(
	SELECT 
		*,
		LAG(concat_key) OVER(PARTITION BY d_product_code ORDER BY d_date) as pre_concat_key
	FROM cte_dim_products 
),
cte_changing_indicator as (
	SELECT 
		*,
		CASE 
			WHEN (pre_concat_key IS NULL) OR (pre_concat_key <> concat_key) THEN 1
			ELSE 0
		END as changing_indicator,
		ROW_NUMBER() OVER(PARTITION BY d_product_code ORDER BY d_date) AS rn
	-- d_date is duplicated, so use rn to order
	FROM cte_pre_concat_key
),
cte_changing_indicator_cumulative as (
	SELECT 
		*,
		SUM(changing_indicator) OVER(PARTITION BY d_product_code ORDER BY rn) as changing_indicator_cumulative
	FROM cte_changing_indicator
),
cte_product_final as (
	SELECT 
		d_product_code,
		product_name,
		sku_group_code,
		product_brand_name,
		product_brand_group_name,
		packtype,
		packsize,
		bottle_convert,
		MIN(d_date) as __start_date,
		CASE
			WHEN MAX(d_date) = CURRENT_DATE() - 1 THEN '2099-12-31'
			ELSE MAX(d_date)
		END AS __end_date -- Since latest uploaded date for product master is N-1
	FROM cte_changing_indicator_cumulative
	GROUP BY
		d_product_code,
		product_name,
		sku_group_code,
		product_brand_name,
		product_brand_group_name,
		packtype,
		packsize,
		bottle_convert,
		changing_indicator_cumulative
)
SELECT
    CONCAT(d_product_code,"|",DATE_FORMAT(__start_date,'yyyyMMdd'),"|",DATE_FORMAT(__end_date,'yyyyMMdd')) AS product_code_sid,
    d_product_code AS product_code,
	cte_product_final.* EXCEPT(d_product_code),
	CASE 
		WHEN __end_date >= CURRENT_DATE() THEN 1
		ELSE 0
	END AS __is_active	
FROM cte_product_final 



