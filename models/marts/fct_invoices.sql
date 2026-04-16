with invoices as (
    select * from {{ ref('stg_invoices') }}
),
subscriptions as (
    select
        subscription_id,
        plan_name,
        billing_cycle
    from {{ ref('stg_subscriptions') }}
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
    i.invoice_id,
    i.subscription_id,
    i.company_id,
    c.company_name,
    c.industry,
    c.country,
    c.company_size,
    s.plan_name,
    s.billing_cycle,
    i.invoice_date,
    i.due_date,
    i.amount_usd,
    i.tax_pct,
    -- calculate tax amount and total amount including tax
    round(i.amount_usd * i.tax_pct / 100, 2)           as tax_amount,
    round(i.amount_usd + (i.amount_usd * i.tax_pct / 100), 2) as total_amount_with_tax,
    i.payment_method,
    i.payment_status,
    i.paid_date,
    -- flag overdue invoices
    case when i.payment_status = 'overdue' then 1 else 0 end as is_overdue,
    -- flag failed payments
    case when i.payment_status = 'failed' then 1 else 0 end  as is_failed,
    -- days taken to pay from invoice date
    -- null if not yet paid
    datediff('day', i.invoice_date, i.paid_date)        as days_to_pay
from invoices i
left join subscriptions s on i.subscription_id = s.subscription_id
left join companies c on i.company_id = c.company_id