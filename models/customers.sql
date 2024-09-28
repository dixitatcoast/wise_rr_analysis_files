{{
    config(
        materialized = 'view',
        tags=["audit"]
    )
}}
select customer_id,
    customer_type,
    CASE 
        WHEN Current_Address_Country = 'UK' THEN 'GBR' 
        ELSE Current_Address_Country 
    END AS current_address_country,

    customer_since_date

 from dbt_dev.raw_customers 