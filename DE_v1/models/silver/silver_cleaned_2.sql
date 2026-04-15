-- changing data types into the appropriate data types
SELECT
    transaction_id,
    CASE WHEN item IN ('ERROR', 'UNKNOWN') THEN NULL ELSE item END::text AS item,
    CASE WHEN quantity IN ('ERROR', 'UNKNOWN') THEN NULL ELSE quantity END::INTEGER AS quantity,
    CASE WHEN price_per_unit IN ('ERROR', 'UNKNOWN') THEN NULL ELSE price_per_unit END::NUMERIC as price_per_unit,
    CASE WHEN total_spent IN ('ERROR', 'UNKNOWN') THEN NULL ELSE total_spent END::NUMERIC as total_spent,
    CASE WHEN payment_method IN ('ERROR', 'UNKNOWN') THEN NULL ELSE payment_method END::text as payment_method,
    CASE WHEN "location" IN ('ERROR', 'UNKNOWN') THEN NULL ELSE "location" END::text as "location",
    CASE WHEN transaction_date IN ('ERROR', 'UNKNOWN') THEN NULL ELSE transaction_date END::DATE as transaction_date
FROM {{ref('silver_cleaned_1')}}