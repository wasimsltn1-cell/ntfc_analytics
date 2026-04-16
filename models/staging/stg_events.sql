with source as (
    select * from {{ source('raw', 'raw_events') }}
),
cleaned as (
    select
        event_id,
        user_id,
        company_id,
        feature,
        platform,
        try_to_date(event_date)           as event_date,
        event_month,
        try_to_number(session_duration_sec) as session_duration_sec,
        is_error
    from source
)
select * from cleaned