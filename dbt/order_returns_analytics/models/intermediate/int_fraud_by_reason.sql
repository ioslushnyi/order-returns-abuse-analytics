-- models/intermediate/int_fraud_by_reason.sql
SELECT
    return_reason,
    COUNT(*) AS total_returns,
    COUNTIF(is_fraud) AS fraudulent_returns,
    SAFE_DIVIDE(COUNTIF(is_fraud), COUNT(*)) AS fraud_rate
FROM {{ ref('stg_returns') }}
GROUP BY return_reason
ORDER BY fraud_rate DESC
