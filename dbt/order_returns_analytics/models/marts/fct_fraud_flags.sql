-- models/marts/fct_fraud_flags.sql

SELECT
    r.return_id,
    r.user_id,
    r.sku,
    r.return_date,
    r.return_reason,
    r.return_condition,
    r.refund_amount,
    r.is_fraud,
    u.total_returns AS user_total_returns,
    u.fraud_ratio AS user_fraud_ratio,
    s.fraud_ratio AS sku_fraud_ratio
FROM {{ ref('stg_returns') }} r
LEFT JOIN {{ ref('int_user_returns') }} u USING (user_id)
LEFT JOIN {{ ref('int_sku_returns') }} s USING (sku)
