-- Cohort Retention Analysis
-- Shows what percentage of companies from each signup month
-- are still active 30, 60, and 90 days after signing up.
-- Helps answer: are we retaining early customers?

with company_cohorts as (
    select
        company_id,
        date_trunc('month', signup_date)        as cohort_month,
        signup_date
    from NTFC_ANALYTICS.STAGING.STG_COMPANIES
),
company_activity as (
    select
        company_id,
        min(event_date)                         as first_event_date,
        max(event_date)                         as last_event_date
    from NTFC_ANALYTICS.STAGING.STG_EVENTS
    group by company_id
),
cohort_activity as (
    select
        c.company_id,
        c.cohort_month,
        c.signup_date,
        a.first_event_date,
        a.last_event_date,
        datediff('day', c.signup_date, a.last_event_date) as days_active
    from company_cohorts c
    left join company_activity a on c.company_id = a.company_id
)
select
    cohort_month,
    count(distinct company_id)                  as total_companies,
    -- still active after 30 days
    count(distinct case
        when days_active >= 30 then company_id
    end)                                        as retained_30d,
    -- still active after 60 days
    count(distinct case
        when days_active >= 60 then company_id
    end)                                        as retained_60d,
    -- still active after 90 days
    count(distinct case
        when days_active >= 90 then company_id
    end)                                        as retained_90d,
    -- retention rates as percentages
    round(count(distinct case when days_active >= 30 then company_id end) * 100.0
        / nullif(count(distinct company_id), 0), 1) as retention_rate_30d,
    round(count(distinct case when days_active >= 60 then company_id end) * 100.0
        / nullif(count(distinct company_id), 0), 1) as retention_rate_60d,
    round(count(distinct case when days_active >= 90 then company_id end) * 100.0
        / nullif(count(distinct company_id), 0), 1) as retention_rate_90d
from cohort_activity
group by cohort_month
order by cohort_month