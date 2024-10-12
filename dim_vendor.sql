WITH vendor AS (
    SELECT DISTINCT 
        cc.userid AS vendor_code,
        cc.username AS vendor_name,
        cc.country,
        cc.region AS region_code,
        cc.province AS province_code, 
        cc.routeid AS route_code,
        cc.keyaccount AS key_account,
        cc.housenumber AS house_number,
        cc.street,
        cc.suburb,
        cc.city, 
        cc.category,
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
        AND cc.is_vendor = "1"
)
SELECT
    CONCAT(vendor_code,'|',date_format(__start_date,'yyyyMMdd'),'|',date_format(__end_date,'yyyyMMdd')) AS vendor_code_sid,
    vendor_code,
    vendor_name,
    street,
    suburb,
    city,
    region_code,
    country,
    key_account,
    house_number,
    category,
    longitude,
    latitude,
    isactive,
    create_date,
    change_date,
    __is_active,
    __start_date,
    __end_date
FROM vendor





