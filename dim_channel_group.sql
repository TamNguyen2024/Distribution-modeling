SELECT DISTINCT 
    channel_group_code,
    channel_group_name 
FROM {{ source('stg_sap', 'channel') }}