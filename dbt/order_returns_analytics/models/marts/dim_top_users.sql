-- models/marts/dim_top_users.sql

-- How to use it in Looker
-- Filter: “Select top users by risk level”

-- Visual: Leaderboard table

-- Drilldown: Link dim_top_users.user_id → user-level return history

WITH ranked_users AS (
  SELECT
    user_id,
    total_returns,
    fraudulent_returns,
    fraud_ratio,
    RANK() OVER (ORDER BY fraud_ratio DESC, fraudulent_returns DESC) AS fraud_rank,
    CASE
      WHEN fraud_ratio >= 0.75 THEN 'high risk'
      WHEN fraud_ratio >= 0.3 THEN 'medium risk'
      WHEN fraud_ratio > 0 THEN 'low risk'
      ELSE 'no fraud'
    END AS risk_level
  FROM {{ ref('int_user_returns') }}
  WHERE fraudulent_returns >= 2
)

SELECT *
FROM ranked_users
WHERE fraud_rank <= 50
