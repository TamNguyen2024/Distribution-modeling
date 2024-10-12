WITH latest_status_sales_data (
    SELECT *
    FROM 
    (
        SELECT 
            sales_data.order_code AS order_no,
            sales_data.customer_code,
            sales_data.vendor_code,
            TO_DATE(sales_data.order_date, 'dd-MM-yyyy') AS order_date,
            TO_DATE(sales_data.settlement_date, 'dd-MM-yyyy') AS settle_date,
            sales_data.invoice_code,
            TO_DATE(sales_data.invoice_date, 'dd-MM-yyyy') AS invoice_date,    
            sales_data.sku_code AS product_code,
            sales_data.quantity_sold AS quantity,
            sales_data.net_revenue AS nsr,
            sales_data.isgift AS is_gift,
            sales_data.__export_date,
            ROW_NUMBER() OVER (PARTITION BY customer_code,vendor_code, order_code, sku_code, isgift, quantity_sold ORDER BY __export_date DESC ) as rn
        FROM {{ source ('stg_dms', 'sales_data') }} AS sales_data
    ) AS ranked_orders
    WHERE rn = 1
)
SELECT DISTINCT
    order_no,
    customer_vendor.rel_customer_vendor_code,
    customer_route.rel_customer_route_code,
    sales_data.order_no,
    sales_data.order_date,
    sales_data.settle_date,
    sales_data.invoice_code,
    sales_data.invoice_date,    
    product.product_code_sid,
    sales_data.quantity,
    sales_data.nsr,
    sales_data.is_gift
FROM latest_status_sales_data sales_data
LEFT JOIN {{ ref('dim_customer') }} customer
    ON sales_data.customer_code = customer.customer_code
    AND sales_data.order_date BETWEEN customer.__start_date AND customer.__end_date 
LEFT JOIN {{ ref('dim_product') }} product
    ON sales_data.product_code = product.product_code
    AND sales_data.order_date BETWEEN product.__start_date AND product.__end_date 
LEFT JOIN {{ ref('relationship_customer_vendor') }} customer_vendor
    ON customer.customer_code_sid = customer_vendor.customer_code_sid
    AND sales_data.order_date BETWEEN customer_vendor.__start_date AND customer_vendor.__end_date
LEFT JOIN {{ ref('relationship_customer_route') }} customer_route
    ON customer.customer_code_sid = customer_route.customer_code_sid
    AND sales_data.order_date BETWEEN customer_route.__start_date AND customer_route.__end_date