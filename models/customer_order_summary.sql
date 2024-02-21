WITH customer_orders AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        COUNT(o.order_id) AS number_of_orders,
        SUM(o.amount) AS total_order_amount
    FROM {{ ref('customers') }} c
    JOIN {{ ref('orders') }} o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name
)

SELECT
    customer_id,
    first_name,
    last_name,
    number_of_orders,
    total_order_amount
FROM customer_orders
