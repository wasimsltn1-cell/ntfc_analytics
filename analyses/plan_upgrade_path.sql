-- Plan Upgrade Path Analysis
-- Shows the most common sequence of plan changes per company.
-- Helps answer: do customers upgrade predictably or skip tiers?
-- Also surfaces companies that downgraded -- a churn warning signal.

with subscription_history as (
    select
        company_id,
        plan_name,
        start_date,
        end_date,
        status,
        -- rank each subscription by start date per company
        row_number() over (
            partition by company_id
            order by start_date asc
        )                                               as sub_sequence
    from NTFC_ANALYTICS.STAGING.STG_SUBSCRIPTIONS
    where plan_name != 'Free Trial'
),
plan_transitions as (
    select
        a.company_id,
        a.plan_name                                     as from_plan,
        b.plan_name                                     as to_plan,
        a.start_date                                    as from_start_date,
        b.start_date                                    as to_start_date,
        datediff('day', a.start_date, b.start_date)     as days_between_plans,
        -- classify the transition type
        case
            when b.plan_name = 'Enterprise'
             and a.plan_name != 'Business'              then 'skip upgrade'
            when b.plan_name > a.plan_name              then 'upgrade'
            when b.plan_name < a.plan_name              then 'downgrade'
            else 'lateral move'
        end                                             as transition_type
    from subscription_history a
    inner join subscription_history b
        on a.company_id = b.company_id
        and b.sub_sequence = a.sub_sequence + 1
)
select
    from_plan,
    to_plan,
    transition_type,
    count(*)                                            as total_transitions,
    round(avg(days_between_plans), 0)                   as avg_days_between_plans,
    round(count(*) * 100.0 / sum(count(*)) over (), 1)  as pct_of_all_transitions
from plan_transitions
group by from_plan, to_plan, transition_type
order by total_transitions desc