-- models/staging/stg_returns.sql

SELECT
    return_id,
    order_id,
    user_id,
    sku,
    category,
    brand,
    launch_date,
    order_date,
    return_date,
    return_days,
    return_reason,
    return_condition,
    refund_amount,
    is_fraud
FROM `order-returns-abuse-detection.order_returns.cleaned_returns`