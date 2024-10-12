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
        AND cc.isactive = "1" -- take active customer only 
),
    cte_customer_vendor_rel AS (
    SELECT DISTINCT
        CONCAT(customer.customer_code,'|',date_format(customer.__start_date,'yyyyMMdd'),'|',date_format(customer.__end_date,'yyyyMMdd')) AS customer_code_sid,
        customer.customer_code,
        vendor.vendor_code_sid, 
        vendor.vendor_code,
        CASE
            WHEN customer.__start_date > vendor.__start_date THEN customer.__start_date
            ELSE vendor.__start_date
        END as __start_date,
        CASE
            WHEN customer.__end_date < vendor.__end_date THEN customer.__end_date
            ELSE vendor.__end_date
        END as __end_date
    FROM cte_customer customer
    LEFT JOIN {{ ref('dim_vendor') }} vendor
        ON customer.vendor_code = vendor.vendor_code
        AND (
            customer.__start_date BETWEEN vendor.__start_date AND vendor.__end_date
            OR 
            vendor.__start_date BETWEEN customer.__start_date AND customer.__end_date
        )
    WHERE vendor.__start_date IS NOT NULL
)
SELECT DISTINCT 
    CONCAT(customer_code,'|',vendor_code,'|',date_format(__start_date,'yyyyMMdd'),'|',date_format(__end_date,'yyyyMMdd')) AS rel_customer_vendor_code,
    customer_code_sid,
    vendor_code_sid,
    __start_date,
    __end_date,
    CASE
        WHEN __end_date >= CURRENT_DATE() THEN 1
        ELSE 0
    END AS __is_active
FROM cte_customer_vendor_rel