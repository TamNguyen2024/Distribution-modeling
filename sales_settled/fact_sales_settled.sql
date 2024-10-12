SELECT DISTINCT
    rel_customer_vendor_code,
    rel_customer_route_code,
    order_no,
    invoice_code,
    order_date,
    settle_date,
    invoice_date
FROM {{ ref('int_sales_settled') }}
