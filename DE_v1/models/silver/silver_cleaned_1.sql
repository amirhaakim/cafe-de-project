-- source already uses snake_case column names
SELECT 
    transaction_id      AS transaction_id,
    item                AS item,
    quantity            AS quantity,
    price_per_unit      AS price_per_unit,
    total_spent         AS total_spent,
    payment_method      AS payment_method,
    location            AS location,
    transaction_date    AS transaction_date
FROM {{ ref('bronze_cafe_sales') }}
