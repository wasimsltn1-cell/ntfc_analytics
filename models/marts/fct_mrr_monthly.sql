with subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
),
companies as (
    select
        company_id,
        company_name,
        industry,
        country,
        company_size
    from {{ ref('dim_companies') }}
)
select
    date_trunc('month', s.start_date)          as month,
    s.company_id,
    c.company_name,
    c.industry,
    c.country,
    c.company_size,
    s.plan_name,
    s.billing_cycle,
    s.status,
    s.mrr,
    s.arr,
    sum(s.mrr) over (
        partition by date_trunc('month', s.start_date), s.plan_name
    )                                          as plan_mrr_for_month,
    -- running total mrr over time
    sum(s.mrr) over (
        order by date_trunc('month', s.start_date)
        rows between unbounded preceding and current row
    )                                          as cumulative_mrr
from subscriptions s
left join companies c on s.company_id = c.company_id