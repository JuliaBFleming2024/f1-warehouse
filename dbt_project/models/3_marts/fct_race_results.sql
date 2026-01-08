select
    driver_race_id,
    event_weekend_id,
    driver_number,
    starting_grid_position,
    position,
    classified_position,
    interval_time__ns,
    interval_time__formatted,
    normalized_time__ns,
    normalized_time__formatted,
    points_awarded
from 
    {{ ref('int_race_results__calculate_total_time') }}

