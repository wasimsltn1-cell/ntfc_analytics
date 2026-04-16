with source as (
    select * from {{ source('raw', 'raw_invoices') }}
),
cleaned as (
    select
        invoice_id,
        subscription_id,
        company_id,
        coalesce(
            try_to_date(invoice_date, 'DD.MM.YY'),
            try_to_date(invoice_date, 'D.MM.YY'),
            try_to_date(invoice_date, 'DD.M.YY'),
            try_to_date(invoice_date, 'D.M.YY')
        )                                         as invoice_date,
        coalesce(
            try_to_date(due_date, 'DD.MM.YY'),
            try_to_date(due_date, 'D.MM.YY'),
            try_to_date(due_date, 'DD.M.YY'),
            try_to_date(due_date, 'D.M.YY')
        )                                         as due_date,
        CAST(amount_usd AS DECIMAL(10,2))         as amount_usd,
        CAST(tax_pct AS INTEGER)                  as tax_pct,
        payment_method,
        payment_status,
        COALESCE(
            try_to_date(paid_date, 'DD MMMM YYYY'),
            try_to_date(paid_date, 'D.M.YY')
        )                                         as paid_date
    from source
)
select * from cleaned