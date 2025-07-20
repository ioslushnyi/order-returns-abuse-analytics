-- models/fraud_summary.sql

SELECT
    COUNT(*) AS total_returns,
    COUNTIF(is_fraud) AS total_frauds,
    SAFE_DIVIDE(COUNTIF(is_fraud), COUNT(*)) AS fraud_rate
FROM {{ ref('stg_returns') }}
