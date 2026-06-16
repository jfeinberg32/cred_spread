with

source as (
    select *
    from {{ source('raw', 'fred_series') }}
    where series_name = 'hy_oas'
),

deduped as (
    select *
    from source
    qualify row_number() over (
        partition by date
        order by ingested_at desc
    ) = 1
),

business_days as (
    select *
    from deduped
    where value is not null
),

forward_filled as (
    select
        date,
        coalesce(
            value,
            last_value(value ignore nulls) over (
                order by date
                rows between unbounded preceding and current row
            )
        ) as hy_oas
    from business_days
)

select *
from forward_filled
order by date