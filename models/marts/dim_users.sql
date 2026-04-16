with users as (
    select * from {{ ref('stg_users') }}
),
companies as (
    select
        company_id,
        company_name,
        industry,
        country,
        company_size,
        current_plan
    from {{ ref('dim_companies') }}
)
select
    u.user_id,
    u.company_id,
    u.role,
    u.created_date,
    u.last_login_date,
    u.is_active,
    u.email_verified,
    c.company_name,
    c.industry,
    c.company_size,
    c.current_plan,
    datediff('day', u.last_login_date, current_date()) as days_since_last_login 
    -- days_since_last_login is calculated against current_date() at query time.
    -- in a production pipeline this would use the pipeline execution date
    -- to avoid incorrect values when data is not loaded incrementally.
from users u
left join companies c on u.company_id = c.company_id