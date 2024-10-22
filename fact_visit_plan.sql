WITH cte_customer AS (
    SELECT DISTINCT 
        cc.userid AS customer_code,
        cc.username AS customer_name,
        cc.country,
        cc.region AS region_code,
        cc.subchannel AS sub_channel_code,
        cc.ranking AS ranking_code,
        cc.routeid AS route_code,
        cc.housenumber AS house_number,
        cc.street,
        cc.suburb,
        cc.city, 
        cc.longitude,
        cc.latitude,
        cc.isactive AS isactive,
        to_date(cc.createdate, 'yyyyMMdd') AS create_date,
        to_date(cc.changedate, 'yyyyMMdd') AS change_date,
        cc.__active AS __is_active,
        cc.`__start_date`,
        CASE
            WHEN cc.`__end_date` IS NULL THEN '2099-12-31'
            ELSE cc.`__end_date`
        END AS `__end_date`
    FROM {{ source('stg_sap','user_changelog') }} cc
    WHERE cc.primaryindicator = "1"
        AND cc.country = "VN"
        AND cc.is_customer = "1"
),
	cte_visit_plan_cleaning AS (
	SELECT visit_plan.*
	FROM {{ source('stg_pace', 'visit_plan_append') }} visit_plan
	LEFT JOIN cte_customer customer 
		ON CONCAT('00',visit_plan.customerid) = customer.customer_code 
		AND visit_plan.__export_date BETWEEN customer.__start_date AND customer.__end_date
		AND visit_plan.routeid = customer.route_code		
	WHERE 
		customer.customer_code IS NOT NULL
),
	cte_visit_plan AS (
	SELECT DISTINCT
	    CONCAT("00",visit_plan.customerid) AS customer_code,
	    visit_plan.routeid AS route_code,
	    visit_plan.frequency,
	    visit_plan.starting_week_visit,
	    visit_plan.visitfriday,
	    visit_plan.visitsaturday,
	    visit_plan.visitsunday,
	    visit_plan.visitmonday,
	    visit_plan.visittuesday,
	    visit_plan.visitwednesday,
	    visit_plan.visitthursday,
	    visit_plan.active,
	    visit_plan.__export_date AS d_date
	FROM cte_visit_plan_cleaning visit_plan
	WHERE 
	    visit_plan.visitfriday = "1" 
	    OR visit_plan.visitmonday = "1" 
	    OR visit_plan.visitsaturday = "1" 
	    OR visit_plan.visitsunday = "1" 
	    OR visit_plan.visitthursday = "1" 
	    OR visit_plan.visittuesday = "1" 
	    OR visit_plan.visitwednesday = "1"
)
SELECT 
    visit_plan.d_date,
	rel.rel_code
    visit_plan.frequency,
    visit_plan.starting_week_visit,
    visit_plan.visitfriday,
    visit_plan.visitsaturday,
    visit_plan.visitsunday,
    visit_plan.visitmonday,
    visit_plan.visittuesday,
    visit_plan.visitwednesday,
    visit_plan.visitthursday,
    visit_plan.active
FROM cte_visit_plan visit_plan
LEFT JOIN {{ ref('dim_route') }} route 
	ON visit_plan.route_code = route.route_code 
	AND visit_plan.d_date >= route.__start_date AND visit_plan.d_date <= route.__end_date
LEFT JOIN {{ ref('dim_customer') }} customer 
	ON visit_plan.customer_code = customer.customer_code 
	AND visit_plan.d_date >= customer.__start_date AND visit_plan.d_date <= customer.__end_date
LEFT JOIN {{ ref('relationship') }} rel
	ON customer.customer_code_sid = rel.customer_code_sid
	AND route.route_code_sid = rel.route_code_sid
	AND visit_plan.d_date BETWEEN rel.__start_date AND rel.__end_date





