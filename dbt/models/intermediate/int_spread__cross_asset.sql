with

spreads as (
    select * from {{ ref('int_spread__features') }}
),

vix as (
    select * from {{ ref('stg_fred__vix') }}
),

yield_curve as (
    select * from {{ ref('stg_fred__yield_curve') }}
),

ted as (
    select * from {{ ref('stg_fred__ted_spread') }}
),

joined as (
    select
        spreads.*,
        vix.vix,
        yield_curve.yield_curve,
        ted.ted_spread
    from spreads
    left join vix          using (date)
    left join yield_curve  using (date)
    left join ted          using (date)
),

with_cross_asset_features as (
    select
        *,

        (vix - avg(vix) over w252) / nullif(stddev(vix) over w252, 0)
            as vix_zscore_252d,

        vix - lag(vix, 21) over (order by date) as vix_mom_21d,

        yield_curve - lag(yield_curve, 21) over (order by date)
            as yield_curve_mom_21d,

        (
            (hy_oas_zscore_252d) +
            (vix - avg(vix) over w252) / nullif(stddev(vix) over w252, 0)
        ) / 2.0 as stress_composite

    from joined
    window
        w252 as (order by date rows between 251 preceding and current row)
)

select *
from with_cross_asset_features
order by date