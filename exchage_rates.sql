{{
    config(
        materialized = 'view',
        tags=["audit"]
    )
}}
WITH exchange_rates AS (
    SELECT 
        'EUR --> EUR' AS currency_route, 1 AS exchange_rate UNION ALL
        SELECT 'GBP --> AUD', 1.769187 UNION ALL
        SELECT 'AUD --> AUD', 1 UNION ALL
        SELECT 'AUD --> EUR', 0.6330682833 UNION ALL
        SELECT 'GBP --> USD', 1.245988333 UNION ALL
        SELECT 'USD --> USD', 1 UNION ALL
        SELECT 'USD --> AUD', 1.4944464 UNION ALL
        SELECT 'GBP --> EUR', 1.158676 UNION ALL
        SELECT 'EUR --> GBP', 0.8580157143 UNION ALL
        SELECT 'USD --> EUR', 0.940688 UNION ALL
        SELECT 'EUR --> AUD', 1.517202857 UNION ALL
        SELECT 'EUR --> USD', 1.078802857 UNION ALL
        SELECT 'USD --> GBP', 0.8128025 UNION ALL
        SELECT 'AUD --> USD', 0.6703371429 UNION ALL
        SELECT 'AUD --> GBP', 0.5424276 UNION ALL
        SELECT 'GBP --> GBP', 1
)

select * from exchange_rates

