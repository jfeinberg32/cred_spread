with

source as (
    select *
    from {{ source('raw', 'fred_series') }}
    where series_name = 'yield_curve'
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

renamed as (
    select
        date,
        value as yield_curve
    from business_days
)

select *
from renamed
order by date