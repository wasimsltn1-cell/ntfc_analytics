with companies as (
    select * from {{ ref('stg_companies') }}
),
subscriptions as (
    select
        company_id,
        count(*) as total_subscriptions,
        sum(case 
                when status = 'active' 
                then 1 
                else 0 
            end) as active_subscriptions,
        max(mrr) as current_mrr,
        max(arr) as current_arr,
        max(plan_name) as current_plan
    from {{ ref('stg_subscriptions') }}
    group by company_id
)
select
    c.company_id,
    c.company_name,
    c.industry,
    c.country,
    c.company_size,
    c.employee_count,
    c.lead_source,
    c.signup_date,
    c.is_active,
    s.current_plan,
    s.current_mrr,
    s.current_arr,
    s.total_subscriptions,
    s.active_subscriptions
from companies c
left join subscriptions s on c.company_id = s.company_id