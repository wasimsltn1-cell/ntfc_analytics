with source as (
    select * from {{ source('raw', 'raw_users') }}
),
cleaned as (
    select
        user_id,
        company_id,
        role,
        try_to_date(created_date)         as created_date,
        try_to_date(last_login_date)      as last_login_date,
        is_active,
        email_verified,
        country
    from source
)
select * from cleaned