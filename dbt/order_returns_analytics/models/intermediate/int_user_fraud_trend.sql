-- models/intermediate/int_user_fraud_trend.sql

SELECT
    user_id,
    DATE_TRUNC(return_date, MONTH) AS return_month,
    COUNT(*) AS total_returns,
    COUNTIF(is_fraud) AS fraudulent_returns,
    SAFE_DIVIDE(COUNTIF(is_fraud), COUNT(*)) AS fraud_ratio
FROM {{ ref('stg_returns') }}
GROUP BY user_id, return_month
ORDER BY user_id, return_month
