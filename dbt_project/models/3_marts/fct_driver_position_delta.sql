select
    driver_race_id,
    event_weekend_id,
    driver_number,
    starting_grid_position, 
    position as finishing_grid_position,
    starting_grid_position - position as position_delta,
    case
        when starting_grid_position > position then 'gained'
        when starting_grid_position < position then 'lost'
        when starting_grid_position = position then 'maintained'
    end as position_delta_type
from
    {{ ref('fct_race_results') }}
where   
    is_completed_race
