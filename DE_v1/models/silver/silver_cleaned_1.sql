-- rename column headers to be in snake case
SELECT 
    "transaction id"      AS transaction_id,
    "item"                AS item,
    "quantity"            AS quantity,
    "price per unit"      AS price_per_unit,
    "total spent"         AS total_spent,
    "payment method"      AS payment_method,
    "location"            AS location,
    "transaction date"    AS transaction_date
FROM {{ ref('bronze_cafe_sales') }}