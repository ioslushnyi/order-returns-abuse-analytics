WITH trend AS (
  SELECT
    user_id,
    return_month,
    total_returns,
    fraudulent_returns,
    fraud_ratio,
    fraud_ratio - LAG(fraud_ratio) OVER (PARTITION BY user_id ORDER BY return_month) AS delta
  FROM {{ ref('int_user_fraud_trend') }}
)

SELECT *
FROM trend
WHERE delta > 0
