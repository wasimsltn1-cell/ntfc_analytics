with companies as (
    select * from {{ ref('dim_companies') }}
),
-- when did each company last use the product?
last_activity as (
    select
        company_id,
        max(event_date)             as last_event_date,
        count(*)                    as total_events,
        count(distinct user_id)     as unique_active_users
    from {{ ref('stg_events') }}
    group by company_id
),
-- how many unresolved support tickets per company?
open_tickets as (
    select
        company_id,
        count(*)                    as open_ticket_count
    from {{ ref('stg_supporttickets') }}
    where status in ('open', 'in_progress')
    group by company_id
),
-- how many failed or overdue payments per company?
failed_invoices as (
    select
        company_id,
        count(*)                    as failed_invoice_count
    from {{ ref('stg_invoices') }}
    where payment_status in ('failed', 'overdue')
    group by company_id
),
-- calculate days since last activity for reuse
activity_gap as (
    select
        company_id,
        last_event_date,
        total_events,
        unique_active_users,
        datediff('day', last_event_date, current_date()) as days_since_last_activity
    from last_activity
),
-- calculate raw churn score
scored as (
    select
        c.company_id,
        c.company_name,
        c.industry,
        c.country,
        c.current_plan,
        c.current_mrr,
        c.company_size,
        a.last_event_date,
        a.total_events,
        a.unique_active_users,
        a.days_since_last_activity,
        coalesce(t.open_ticket_count, 0)        as open_ticket_count,
        coalesce(f.failed_invoice_count, 0)     as failed_invoice_count,
        -- activity score: higher gap = higher score
        case
            when a.days_since_last_activity > 30 then 3
            when a.days_since_last_activity > 14 then 2
            when a.days_since_last_activity > 7  then 1
            else 0
        end                                     as activity_score,
        -- payment score
        case
            when coalesce(f.failed_invoice_count, 0) > 0 then 2
            else 0
        end                                     as payment_score,
        -- support score
        case
            when coalesce(t.open_ticket_count, 0) > 2 then 1
            else 0
        end                                     as support_score,
        -- single user score
        case
            when a.unique_active_users = 1 then 1
            else 0
        end                                     as single_user_score
    from companies c
    left join activity_gap a    on c.company_id = a.company_id
    left join open_tickets t    on c.company_id = t.company_id
    left join failed_invoices f on c.company_id = f.company_id
    where c.is_active = 'True'
    and c.current_plan is not null
)
-- final output with total score and risk label
select
    company_id,
    company_name,
    industry,
    country,
    current_plan,
    current_mrr,
    company_size,
    last_event_date,
    total_events,
    unique_active_users,
    days_since_last_activity,
    open_ticket_count,
    failed_invoice_count,
    activity_score,
    payment_score,
    support_score,
    single_user_score,
    -- total churn risk score
    activity_score + payment_score + support_score + single_user_score as churn_risk_score,
    -- human readable label
    case
        when activity_score + payment_score + support_score + single_user_score >= 4 then 'high'
        when activity_score + payment_score + support_score + single_user_score >= 2 then 'medium'
        else 'low'
    end                                         as churn_risk_label
from scored
order by churn_risk_score desc