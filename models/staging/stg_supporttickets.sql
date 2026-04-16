with source as (
    select * from {{ source('raw', 'raw_support_tickets') }}
),
cleaned as (
    select
        ticket_id,
        company_id,
        user_id,
        category,
        priority,
        channel,
        status,
        try_to_date(created_date, 'DD MMMM YYYY')               as created_date,
        CAST(resolved_hours AS DECIMAL(10,2))   as resolved_hours,
        CAST(csat_score AS DECIMAL(10,1))       as csat_score,
        CAST(first_response_hours AS INTEGER)   as first_response_hours
    from source
)
select * from cleaned