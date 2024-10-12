SELECT DISTINCT
    sub_channel_code,
    sub_channel_description as sub_channel_name,
    trade_channel_description as trade_channel_name,
    channel as channel_code
FROM {{ source('stg_sap','channel_code_mapping') }}
ORDER BY 
    sub_channel_code,
    trade_channel_description,
    channel