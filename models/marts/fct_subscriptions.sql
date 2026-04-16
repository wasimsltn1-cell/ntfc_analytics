with subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
),
companies as (
    select
        company_id,
        company_name,
        industry,
        country,
        company_size,
        employee_count,
        lead_source
    from {{ ref('dim_companies') }}
)
select
    s.subscription_id,
    s.company_id,
    c.company_name,
    c.industry,
    c.country,
    c.company_size,
    c.employee_count,
    c.lead_source,
    s.plan_name,
    s.billing_cycle,
    s.mrr,
    s.arr,
    s.discount_pct,
    s.start_date,
    s.end_date,
    s.status,
    s.seats_included,
    -- a subscription is churned if it has an end date in the past
    case when s.status = 'churned' then 1 else 0 end    as is_churned,
    -- flags annual subscribers for billing cycle analysis
    case when s.billing_cycle = 'annual' then 1 else 0 end as is_annual,
    datediff('day', s.start_date, 
        coalesce(s.end_date, current_date()))            as subscription_length_days
from subscriptions s
left join companies c on s.company_id = c.company_id