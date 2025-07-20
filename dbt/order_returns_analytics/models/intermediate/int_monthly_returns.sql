-- models/intermediate/int_daily_returns.sql

SELECT
    DATE_TRUNC(return_date, MONTH) AS return_month,
    COUNT(*) AS total_returns,
    COUNTIF(is_fraud) AS fraudulent_returns,
    SAFE_DIVIDE(COUNTIF(is_fraud), COUNT(*)) AS fraud_rate
FROM {{ ref('stg_returns') }}
GROUP BY return_month
ORDER BY return_month
