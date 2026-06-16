with

hy as (
    select * from {{ ref('stg_fred__hy_oas') }}
),

ig as (
    select * from {{ ref('stg_fred__ig_oas') }}
),

joined as (
    select
        hy.date,
        hy.hy_oas,
        ig.ig_oas
    from hy
    left join ig using (date)
),

with_ratio as (
    select
        *,
        case
            when ig_oas > 0 then hy_oas / ig_oas
            else null
        end as hy_ig_spread_ratio
    from joined
),

with_features as (
    select
        date,
        hy_oas,
        ig_oas,
        hy_ig_spread_ratio,

        (hy_oas - avg(hy_oas) over w252) / nullif(stddev(hy_oas) over w252, 0)
            as hy_oas_zscore_252d,

        (ig_oas - avg(ig_oas) over w252) / nullif(stddev(ig_oas) over w252, 0)
            as ig_oas_zscore_252d,

        hy_oas - lag(hy_oas, 21) over (order by date)  as hy_oas_mom_21d,
        hy_oas - lag(hy_oas, 63) over (order by date)  as hy_oas_mom_63d,

        stddev(hy_oas - lag(hy_oas, 1) over (order by date)) over w21
            as hy_oas_vol_21d,

        stddev(hy_oas - lag(hy_oas, 1) over (order by date)) over w63
            as hy_oas_vol_63d,

        avg(hy_oas) over w63   as hy_oas_avg_63d,
        avg(hy_oas) over w252  as hy_oas_avg_252d

    from with_ratio
    window
        w21  as (order by date rows between 20 preceding and current row),
        w63  as (order by date rows between 62 preceding and current row),
        w252 as (order by date rows between 251 preceding and current row)
)

select *
from with_features
order by date