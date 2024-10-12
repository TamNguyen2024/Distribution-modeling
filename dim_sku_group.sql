SELECT DISTINCT 
    sku_group_code,
    sku_group_name,
    flavor,
    brand,
    category 
FROM {{ source('stg_sap', 'sku_group') }}


