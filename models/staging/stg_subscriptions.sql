with source as (
    select * from {{ source('raw', 'raw_subscriptions') }}
),
cleaned as (
    select
        subscription_id,
        company_id,
        plan_name,
        billing_cycle,
        CAST(mrr AS DECIMAL(10,2))      as mrr,
        CAST(arr AS DECIMAL(10,2))      as arr,
        CAST(discount_pct AS INTEGER)   as discount_pct,
        try_to_date(start_date, 'DD MMMM YYYY') as start_date,
        try_to_date(end_date, 'DD MMMM YYYY')   as end_date,
        status,
        CAST(seats_included AS INTEGER) as seats_included
    from source
)
select * from cleaned