with

source as (
    select * from {{ ref('int_spread__cross_asset') }}
),

ranked as (
    select
        *,
        row_number() over (order by date) as row_num
    from source
),

warmed_up as (
    select *
    from ranked
    where row_num > 252
),

final as (
    select
        date,
        hy_oas,
        ig_oas,
        vix,
        yield_curve,
        hy_ig_spread_ratio,
        hy_oas_zscore_252d,
        ig_oas_zscore_252d,
        vix_zscore_252d,
        hy_oas_mom_21d,
        hy_oas_mom_63d,
        vix_mom_21d,
        yield_curve_mom_21d,
        hy_oas_vol_21d,
        hy_oas_vol_63d,
        stress_composite

    from warmed_up
    where
        hy_oas                 is not null
        and ig_oas             is not null
        and vix                is not null
        and yield_curve        is not null
        and hy_oas_zscore_252d is not null
        and ig_oas_zscore_252d is not null
        and vix_zscore_252d    is not null
        and hy_oas_mom_21d     is not null
        and hy_oas_vol_21d     is not null
        and stress_composite   is not null
)

select *
from final
order by date