select
    current_plan,
    count(*)                                        as company_count,
    round(sum(current_mrr), 0)                      as plan_mrr,
    round(sum(current_mrr) * 100.0 / 
        sum(sum(current_mrr)) over (), 1)           as pct_of_total_mrr
from NTFC_ANALYTICS.MARTS.DIM_COMPANIES
where current_mrr is not null
  and current_mrr > 0
group by current_plan
order by plan_mrr desc
