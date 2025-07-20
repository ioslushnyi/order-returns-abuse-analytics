-- models/intermediate/int_sku_returns.sql
SELECT
    sku,
    COUNT(*) AS total_returns,
    COUNTIF(is_fraud) AS fraudulent_returns,
    SAFE_DIVIDE(COUNTIF(is_fraud), COUNT(*)) AS fraud_ratio
FROM {{ ref('stg_returns') }}
GROUP BY sku
HAVING fraudulent_returns > 0
ORDER BY fraud_ratio DESC
