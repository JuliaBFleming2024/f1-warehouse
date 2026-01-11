select
    driver_race_id,
    event_weekend_id,
    driver_number,
    starting_grid_position,
    position,
    classified_position,
    case when starting_grid_position > position