-- models/marts/fct_refund_loss_summary.sql

SELECT
    ROUND(SUM(CASE WHEN is_fraud THEN refund_amount ELSE 0 END), 2) AS total_refund_loss,
    ROUND(SUM(refund_amount), 2) AS total_refunded,
    SAFE_DIVIDE(
        SUM(CASE WHEN is_fraud THEN refund_amount ELSE 0 END),
        SUM(refund_amount)
    ) AS refund_loss_rate
FROM {{ ref('fct_fraud_flags') }}
