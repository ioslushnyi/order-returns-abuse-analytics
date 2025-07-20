-- models/intermidiate/int_user_returns.sql

SELECT
    user_id,
    COUNT(*) AS total_returns,
    COUNTIF(is_fraud) AS fraudulent_returns,
    SAFE_DIVIDE(COUNTIF(is_fraud), COUNT(*)) AS fraud_ratio
FROM {{ ref('stg_returns') }}
GROUP BY user_id
