with events as (
    select * from {{ ref('stg_events') }}
),
users as (
    select
        user_id,
        company_id,
        role,
        is_active
    from {{ ref('stg_users') }}
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
    e.event_id,
    e.user_id,
    e.company_id,
    u.role                          as user_role,
    u.is_active                     as user_is_active,
    c.company_name,
    c.industry,
    c.country,
    c.company_size,
    c.current_plan,
    e.feature,
    e.platform,
    e.event_date,
    e.event_month,
    e.session_duration_sec,
    -- convert seconds to minutes for readability in dashboards
    round(e.session_duration_sec / 60.0, 2)  as session_duration_min,
    e.is_error
from events e
left join users u on e.user_id = u.user_id
left join companies c on e.company_id = c.company_id