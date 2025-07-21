-- models/intermediate/int_users_with_rising_fraud.sql

WITH trend AS (
  SELECT
    user_id,
    return_month,
    total_returns,
    fraudulent_returns,
    fraud_ratio,
    COALESCE(fraud_ratio - LAG(fraud_ratio) OVER (PARTITION BY user_id ORDER BY return_month), fraud_ratio) AS delta
  FROM {{ ref('int_user_fraud_trend') }}
)

SELECT *
FROM trend
WHERE delta > 0
