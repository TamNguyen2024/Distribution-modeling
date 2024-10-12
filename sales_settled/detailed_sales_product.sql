SELECT 
	order_no,
	product_code_sid,
	quantity,
	nsr,
	is_gift 
FROM {{ ref('int_sales_settled') }}



