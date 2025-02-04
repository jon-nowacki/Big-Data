/*
Smoothing out weekly sales
*/


WITH weekly_sales AS (
    SELECT 
        week,
        store_nbr,
        sku_nbr,
        sales,
        PERCENT_RANK() OVER (PARTITION BY week, store_nbr, sku_nbr ORDER BY sales) AS percentile
    FROM 
        sales_data
)
SELECT 
    week,
    store_nbr,
    sku_nbr,
    MAX(sales) FILTER (WHERE percentile <= 0.95) AS week_cap
FROM 
    weekly_sales
GROUP BY 
    week, store_nbr, sku_nbr
ORDER BY 
    week, store_nbr, sku_nbr;
