{{
    config(
        materialized = 'table',
        tags=["audit"]
    )
}}

select 
cust.*, 
txn.transaction_id,
txn.gbp_currency_route,
txn.amount_gbp,
txn.transaction_date,
txn.currency_route,
txn.source_currency, 
txn.amount_gbp*s_exchange.exchange_rate as source_amount,
txn.destination_currency,
round(source_amount * (1- 0.0064)*exchange_rates.exchange_rate,2) as destination_amount ,
source_amount * (0.0064) as wise_mid_market_fees,
txn.transaction_type ,
txn.is_positive_amount,
txn.is_unique_txn,
transaction_date >= customer_since_date as is_after_transaction,
        CASE 
        WHEN 
            (txn.currency_route LIKE '%GBP%' AND current_address_country = 'GBR') OR
            (txn.currency_route LIKE '%USD%' AND current_address_country = 'USA') OR
            (txn.currency_route LIKE '%EUR%' AND current_address_country IN ('FRA', 'DEU','ISL','ESP', 'ITA', 'PRT','NOR','LIE')) OR
            (txn.currency_route LIKE '%AUD%' AND current_address_country = 'AUS')
        THEN TRUE
        ELSE FALSE
    END AS is_local_entity_txn,
    ( is_positive_amount and  is_unique_txn and is_after_transaction) as is_valid_transaction
    from  {{ ref('customers') }} as cust 
    left join {{ ref('transactions') }}  as txn on txn.customer_id = cust.customer_id
    left join {{ ref('exchange_rates') }} as exchange_rates on exchange_rates.currency_route=txn.currency_route
    left join {{ ref('exchange_rates') }} as s_exchange on s_exchange.currency_route=txn.gbp_currency_route 
  order by customer_id, transaction_id