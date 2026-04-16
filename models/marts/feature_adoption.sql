with events as (
    -- all product usage events from staging
    select * from {{ ref('stg_events') }}
),
companies as (
    -- pull only the context columns we need for slicing
    select
        company_id,
        current_plan,
        industry,
        country,
        company_size
    from {{ ref('dim_companies') }}
)
select
    -- dimensions: what we slice and filter by in dashboards
    e.feature,
    e.event_month,
    c.current_plan,
    c.industry,
    c.country,
    c.company_size,

    -- volume metrics: how much is this feature being used
    count(*)                                        as total_events,
    count(distinct e.user_id)                       as unique_users,
    count(distinct e.company_id)                    as unique_companies,

    -- quality metric: what % of events resulted in an error
    -- high error rate signals a product bug or UX problem
    round(
        sum(case when e.is_error = 'True' then 1 else 0 end) * 100.0
        / count(*), 2
    )                                               as error_rate_pct,

    -- engagement metric: how long users spend per session on this feature
    -- converted from seconds to minutes for readability
    round(avg(e.session_duration_sec) / 60.0, 2)   as avg_session_duration_min,

    -- platform breakdown: pivot row values into columns
    -- tells us where users prefer to access each feature
    sum(case when e.platform = 'web'            then 1 else 0 end) as web_events,
    sum(case when e.platform = 'mobile_ios'     then 1 else 0 end) as ios_events,
    sum(case when e.platform = 'mobile_android' then 1 else 0 end) as android_events,
    sum(case when e.platform = 'api'            then 1 else 0 end) as api_events

from events e
left join companies c on e.company_id = c.company_id

-- one row per feature per month per plan per industry per country per company size
-- granular enough to answer most product questions without hitting raw event volume
group by
    e.feature,
    e.event_month,
    c.current_plan,
    c.industry,
    c.country,
    c.company_size