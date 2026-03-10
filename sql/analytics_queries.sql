SELECT
    category,
    COUNT(*)                            AS total_products,
    ROUND(AVG(price), 2)               AS avg_price_usd,
    ROUND(MIN(price), 2)               AS min_price_usd,
    ROUND(MAX(price), 2)               AS max_price_usd,
    ROUND(SUM(revenue_potential), 2)   AS total_revenue_potential,
    ROUND(AVG(rating_rate), 2)         AS avg_rating
FROM products
GROUP BY category
ORDER BY total_revenue_potential DESC;

SELECT
    id,
    title,
    category,
    ROUND(price, 2)                    AS price_usd,
    ROUND(price_brl, 2)               AS price_brl,
    rating_rate,
    rating_count,
    ROUND(revenue_potential, 2)        AS revenue_potential
FROM products
ORDER BY revenue_potential DESC
LIMIT 5;

SELECT
    price_range,
    COUNT(*)                           AS total_products,
    ROUND(AVG(price), 2)              AS avg_price_usd,
    ROUND(MIN(price), 2)              AS min_price,
    ROUND(MAX(price), 2)              AS max_price,
    ROUND(AVG(rating_rate), 2)        AS avg_rating
FROM products
GROUP BY price_range
ORDER BY avg_price_usd;

SELECT
    rating_class,
    COUNT(*)                           AS total_products,
    ROUND(AVG(price), 2)              AS avg_price_usd,
    ROUND(AVG(rating_rate), 3)        AS avg_rating_score,
    SUM(rating_count)                 AS total_reviews,
    ROUND(AVG(revenue_potential), 2)  AS avg_revenue_potential
FROM products
GROUP BY rating_class
ORDER BY avg_rating_score DESC;

SELECT
    oi.category,
    COUNT(DISTINCT oi.cart_id)         AS total_orders,
    SUM(oi.quantity)                   AS total_items_sold,
    ROUND(SUM(oi.item_revenue_usd), 2) AS total_revenue_usd,
    ROUND(SUM(oi.item_revenue_brl), 2) AS total_revenue_brl,
    ROUND(AVG(oi.item_revenue_usd), 2) AS avg_order_value_usd
FROM order_items oi
GROUP BY oi.category
ORDER BY total_revenue_usd DESC;

SELECT
    user_id,
    COUNT(DISTINCT cart_id)            AS total_orders,
    SUM(quantity)                      AS total_items,
    ROUND(SUM(item_revenue_usd), 2)    AS total_spent_usd,
    ROUND(AVG(item_revenue_usd), 2)    AS avg_order_value_usd
FROM order_items
GROUP BY user_id
ORDER BY total_spent_usd DESC;

SELECT
    p.id,
    p.title,
    p.category,
    p.price,
    p.rating_rate
FROM products p
LEFT JOIN order_items oi ON p.id = oi.product_id
WHERE oi.product_id IS NULL
ORDER BY p.category;

SELECT
    category,
    ROUND(AVG(price), 2)              AS avg_price,
    ROUND(AVG(rating_rate), 2)        AS avg_rating,
    ROUND(
        (AVG(price * rating_rate) - AVG(price) * AVG(rating_rate))
        / (NULLIF(
            SQRT(
                (AVG(price * price) - AVG(price) * AVG(price)) *
                (AVG(rating_rate * rating_rate) - AVG(rating_rate) * AVG(rating_rate))
            ), 0))
    , 4)                              AS pearson_correlation
FROM products
GROUP BY category
ORDER BY ABS(pearson_correlation) DESC;
