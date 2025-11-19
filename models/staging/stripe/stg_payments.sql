with src as (

    select *
    from {{ source('stripe', 'payment') }}

),
staged as (

    select 
        id as payment_id,
        orderid as order_id,
        paymentmethod as payment_method,
        status as payment_status,
        amount as payment_amount,
        case
            when amount < 1000 then 'low'
            when amount < 2000 then 'mid'
            else 'high'
        end as payment_amount_tier,
        created as payment_created,
        datediff('day', created, {{ dbt.current_timestamp() }} ) as days_since_created
    from src
)
select * from staged