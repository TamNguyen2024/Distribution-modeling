WITH customer AS (
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
)
SELECT
    CONCAT(customer_code,'|',date_format(__start_date,'yyyyMMdd'),'|',date_format(__end_date,'yyyyMMdd')) AS customer_code_sid,
    customer_code,
    customer_name,
    sub_channel_code,
    ranking_code,
    region_code,
    country,
    street,
    suburb,
    city,
    house_number,
    longitude,
    latitude,
    isactive,
    create_date,
    change_date,
    __is_active,
    __start_date,
    __end_date
FROM customer

