with source AS  
    (SELECT * from {{source('raw', 'raw_companies')}}),
cleaned as (
    SELECT
        company_id,
        company_name,
        industry,
        country,
        company_size,
        lead_source,
        try_to_date(signup_date) as signup_date,
        is_active,
        case company_size
            when '1-10'    then 5
            when '11-50'   then 30
            when '51-200'  then 125
            when '201-500' then 350
            when '500+'    then 500
        end                                    as employee_count
    from source
)
select * from cleaned