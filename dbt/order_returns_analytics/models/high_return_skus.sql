-- models/high_return_skus.sql

WITH sku_aggregates AS (
  SELECT
    sku,
    COUNT(*) AS total_returns,
    COUNTIF(is_fraud = TRUE) AS fraudulent_returns
  FROM {{ ref('stg_returns') }}
  GROUP BY sku
)

SELECT
  *,
  total_returns > 20 AS is_high_return_sku
FROM sku_aggregates
ORDER BY total_returns DESC
