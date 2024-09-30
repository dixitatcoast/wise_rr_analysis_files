{{
    config(
        materialized = 'view',
        tags=["audit"]
    )
}}
with
    updated_txns as (
        select
            transaction_id,
            transaction_date,
            customer_id,
            amount_gbp,
            replace(tx.currency_route,' --> ','') as currency_route,
            split_part(tx.currency_route, ' --> ', 1) as source_currency,
            split_part(tx.currency_route, ' --> ', 2) as destination_currency
        from dbt_dev.raw_transactions as tx
    )

select
    *,
    'GBP' || source_currency AS gbp_currency_route ,
       case
        when source_currency = destination_currency then 'Same Currency Route' else 'Cross Currency Route'
    end as transaction_type,
     CASE
        WHEN amount_gbp > 0 THEN TRUE
        ELSE FALSE
    END AS is_positive_amount,
    CASE 
        WHEN COUNT(*) OVER (PARTITION BY transaction_id) = 1 THEN TRUE
        ELSE FALSE
    END AS is_unique_txn
from updated_txns
