SELECT DISTINCT
    channel_code,
    channel_name,
    channel_group_code 
FROM {{ source('stg_sap', 'channel') }}