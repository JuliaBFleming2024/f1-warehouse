select
    year,
    -- VARCHAR
    location as race_name,
    driver_number,
    -- VARCHAR
    broadcast_name,
    -- VARCHAR
    abbreviation as driver_last_name_abbreviation,
    -- VARCHAR
    driver_id,
    -- VARCHAR
    team_name,
    -- VARCHAR
    team_color as team_hex_color_code,
    -- VARCHAR
    team_id,
    -- VARCHAR
    first_name as driver_first_name,
    -- VARCHAR
    last_name as driver_last_name,
    -- VARCHAR
    full_name as driver_full_name,
    -- VARCHAR
    headshot_url as driver_head_shot,
    -- VARCHAR
    country_code as driver_country_code,
    -- VARCHAR
    position::int as position,
    -- -- -- -- DOUBLE PRECISION
    classified_position, -- W, did not start -- R - retired
    -- VARCHAR
    grid_position as starting_grid_position,
    -- -- -- -- DOUBLE PRECISION
    time as finish_time__ns,
    (time/10^9) as 
    -- VARCHAR
    status,
    -- VARCHAR
    points as points_awarded,
    -- -- -- -- DOUBLE PRECISION
    laps::int as lap_count
    -- -- -- DOUBLE PRECISION

from {{ source('fastf1_dataset','race_results') }}
