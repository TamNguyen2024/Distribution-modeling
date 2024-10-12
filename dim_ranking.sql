SELECT DISTINCT
    ranking_code, 
    ranking_name 
FROM {{ source('stg_sap', 'customer_ranking') }}