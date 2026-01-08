with calculate_driver_total_time as (
    select
        driver_race_id,
        event_weekend_id,
        driver_number,
        starting_grid_position,
        position,
        classified_position,
        interval_time__ns,
        interval_time__formatted,
        case when interval_time__ns is not null then
            sum(interval_time__ns) over (
                partition by event_weekend_id    
                order by position
                rows between unbounded preceding and current row)
            else null
        end as total_time__ns,
        points_awarded
    from {{ ref('stg_fast_f1__race_results') }}
)

select
    *,
    {{ format_duration_ns('total_time__ns') }} as total_time__formatted
from 
    calculate_driver_total_time
