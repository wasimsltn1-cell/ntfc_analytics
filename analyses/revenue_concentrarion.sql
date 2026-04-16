-- Revenue Concentration Analysis (Pareto)
-- Shows what percentage of total MRR comes from the top 20% of customers.
-- A business health check -- if top 20% of customers generate
-- 80% of revenue, the business is highly concentrated and vulnerable to churn.

with company_mrr as (
    select
        company_id,
        company_name,
        industry,
        country,
        current_plan,
        current_mrr
    from NTFC_ANALYTICS.MARTS.DIM_COMPANIES
    where current_mrr is not null
      and current_mrr > 0
),
ranked as (
    select
        company_id,
        company_name,
        industry,
        country,
        current_plan,
        current_mrr,
        -- rank companies by MRR highest to lowest
        row_number() over (order by current_mrr desc)   as mrr_rank,
        count(*) over ()                                as total_companies,
        sum(current_mrr) over ()                        as total_mrr,
        -- running total of MRR from highest to lowest
        sum(current_mrr) over (
            order by current_mrr desc
            rows between unbounded preceding and current row
        )                                               as cumulative_mrr
    from company_mrr
)
select
    company_id,
    company_name,
    industry,
    country,
    current_plan,
    current_mrr,
    mrr_rank,
    total_companies,
    round(mrr_rank * 100.0 / total_companies, 1)        as percentile,
    cumulative_mrr,
    total_mrr,
    round(cumulative_mrr * 100.0 / total_mrr, 1)        as cumulative_mrr_pct,
    -- flag top 20% of customers by count
    case
        when mrr_rank <= total_companies * 0.20 then 'top 20%'
        else 'bottom 80%'
    end                                                 as customer_segment
from ranked
order by mrr_rank