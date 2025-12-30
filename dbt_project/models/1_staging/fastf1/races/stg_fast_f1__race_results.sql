select
    {{ dbt_utils.generate_surrogate_key([
        'year',
        'location',
        'driver_number'
    ]) }} as race_year_driver_id,
    year,
    location as race_name,
    driver_number,
    broadcast_name as driver_broadcast_name,
    abbreviation as driver_last_name_abbreviation,
    driver_id,
    team_name,
    team_color as team_hex_color_code,
    team_id,
    first_name as driver_first_name,
    last_name as driver_last_name,
    full_name as driver_full_name,
    headshot_url as driver_head_shot,
    country_code as driver_country_code,
    position::int as position,
    case
        when classified_position = 'W' then 'Did not Start'
        when classified_position = 'R' then 'Retired'
        else classified_position
    end as classified_position,
    grid_position::int as starting_grid_position,
    nullif(time::numeric, 'NaN') as interval_time__ns,
    {{ format_duration_ns('time') }} as interval_time__formatted,
    status,
    points::int as points_awarded,
    laps::int as lap_count
from {{ source('fastf1_dataset', 'race_results') }}
