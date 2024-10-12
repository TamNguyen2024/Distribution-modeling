SELECT DISTINCT
    area_code,
    area_name 
FROM {{ source('stg_sap', 'mapping_area_region') }}
