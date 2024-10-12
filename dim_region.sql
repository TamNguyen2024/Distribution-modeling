SELECT DISTINCT
    region_code,
    lo.hierarchy_name as region_name,
    region_short_name,
    region_full_name,
    area_code 
FROM {{ source('stg_sap', 'mapping_area_region') }} mar
LEFT JOIN {{ source('stg_sap', 'location_hierarchy') }} lo
    ON mar.region_code = lo.hierarchy_code 
