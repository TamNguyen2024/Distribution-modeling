WITH cte_date_descartes_SOL AS (
    SELECT DISTINCT
        dd.year,
        dd.month,
        dd.week,
        dd.eoweek,
        dd.d_date,
        dd.SOL_date,
        sol.customer_code
    FROM {{ ref('dim_date') }} dd
    LEFT JOIN {{ ref('daily_service_customer') }} sol
),
    cte_assessment_set AS (
    SELECT DISTINCT 
        cte_date_descartes_SOL.year,
        cte_date_descartes_SOL.month,
        cte_date_descartes_SOL.week,
        cte_date_descartes_SOL.eoweek,
        cte_date_descartes_SOL.d_date,
        cte_date_descartes_SOL.SOL_date, 
        cte_date_descartes_SOL.customer_code,
        COALESCE(sol.is_service,0) AS is_service_customer,
        COALESCE(aol.is_active,0) AS is_active_customer,
        COALESCE(achieve_segment.is_pass_segment,0) AS is_achieve_segment_customer,
        COALESCE(achieve_ranking.is_pass_ranking,0) AS is_achieve_ranking_customer,
        COALESCE(SUM(achieve_segment.is_achieve_cor),0) AS is_achieve_cor_customer,
        COALESCE(SUM(achieve_segment.is_achieve_reg),0) AS is_achieve_reg_customer,
        COALESCE(SUM(achieve_segment.is_achieve_str),0) AS is_achieve_str_customer,
        COALESCE(SUM(achieve_segment.is_achieve_edr),0) AS is_achieve_edr_customer
    FROM cte_date_descartes_SOL
    LEFT JOIN {{ ref('daily_service_customer') }} sol
        ON cte_date_descartes_SOL.d_date = sol.d_date
        AND cte_date_descartes_SOL.customer_code = sol.customer_code
    LEFT JOIN {{ ref('daily_active_customer') }} aol
        ON cte_date_descartes_SOL.d_date = aol.d_date
        AND cte_date_descartes_SOL.customer_code = aol.customer_code
    LEFT JOIN {{ ref('daily_achieve_segment') }} achieve_segment
        ON cte_date_descartes_SOL.d_date = achieve_segment.d_date
        AND cte_date_descartes_SOL.customer_code = achieve_segment.customer_code
    LEFT JOIN {{ ref('daily_achieve_ranking') }} achieve_ranking
        ON cte_date_descartes_SOL.d_date = achieve_ranking.d_date
        AND cte_date_descartes_SOL.customer_code = achieve_ranking.customer_code
    GROUP BY 
        cte_date_descartes_SOL.year,
        cte_date_descartes_SOL.month,
        cte_date_descartes_SOL.week,
        cte_date_descartes_SOL.eoweek,
        cte_date_descartes_SOL.d_date,
        cte_date_descartes_SOL.SOL_date,
        cte_date_descartes_SOL.customer_code,
        sol.is_service,
        aol.is_active,
        achieve_segment.is_pass_segment,
        achieve_ranking.is_pass_ranking
),
    cte_sales_settled_aggregation AS (
	SELECT DISTINCT 
		dd.`year`,
		dd.`month`, 
		dd.week,
		dd.eoweek,
		sr.order_date,
		customer.customer_code,
		SUM(sr.quantity) AS quantity,
		SUM(sr.nsr) AS nsr
	FROM {{ ref('int_sales_settled') }} sr 
	LEFT JOIN {{ ref('relationship_customer_vendor') }} customer_vendor
		ON sr.rel_customer_vendor_code = customer_vendor.rel_customer_vendor_code
	LEFT JOIN {{ ref('dim_customer') }} customer
		ON customer_vendor.customer_code_sid = customer.customer_code_sid
	LEFT JOIN {{ ref('dim_date_SOL') }} dd
		ON sr.order_date = dd.d_date 
    GROUP BY 
        dd.`year`,
		dd.`month`, 
		dd.week,
		dd.eoweek,
		sr.order_date,
		customer.customer_code
)
SELECT DISTINCT 
    cte_assessment_set.d_date,
    cte_assessment_set.SOL_date,
    customer.customer_code_sid,
    cte_assessment_set.customer_code,
    cte_assessment_set.is_service_customer,
    cte_assessment_set.is_active_customer,
    cte_assessment_set.is_achieve_ranking_customer,
    cte_assessment_set.is_achieve_cor_customer,
    cte_assessment_set.is_achieve_reg_customer,
    cte_assessment_set.is_achieve_str_customer,
    cte_assessment_set.is_achieve_edr_customer,
    COALESCE( SUM(cte_sales_settled_aggregation.quantity_ea),0 ) AS mtd_qty,
    COALESCE( SUM(cte_sales_settled_aggregation.nsr),0 ) AS mtd_nsr
FROM cte_assessment_set
LEFT JOIN {{ ref('dim_customer') }} customer  
    ON cte_assessment_set.customer_code = customer.customer_code
    AND cte_assessment_set.SOL_date BETWEEN customer.__start_date AND customer.__end_date 
LEFT JOIN cte_sales_settled_aggregation
    ON cte_assessment_set.customer_code = cte_sales_settled_aggregation.customer_code
    AND cte_assessment_set.year = cte_sales_settled_aggregation.year
    AND cte_assessment_set.month = cte_sales_settled_aggregation.month
    AND cte_assessment_set.d_date >= cte_sales_settled_aggregation.order_date
GROUP BY 
    cte_assessment_set.d_date,
    cte_assessment_set.SOL_date,
    customer.customer_code_sid,
    cte_assessment_set.customer_code,
    cte_assessment_set.is_service_customer,
    cte_assessment_set.is_active_customer,
    cte_assessment_set.is_achieve_ranking_customer,
    cte_assessment_set.is_achieve_cor_customer,
    cte_assessment_set.is_achieve_reg_customer,
    cte_assessment_set.is_achieve_str_customer,
    cte_assessment_set.is_achieve_edr_customer

