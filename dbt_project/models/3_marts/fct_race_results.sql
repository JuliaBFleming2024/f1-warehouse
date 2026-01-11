select
    driver_race_id,
    event_weekend_id,
    driver_number,
    starting_grid_position,
    position,
    classified_position,
    is_completed_race,
    interval_time__ns,
    interval_time__formatted,
    total_time__ns,
    total_time__formatted,
    points_awarded
from 
    {{ ref('int_race_results__calculate_total_time') }}

