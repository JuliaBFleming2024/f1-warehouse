with driver_periods as (
    select distinct
        driver_id, 
        driver_number,
        team_name,
        team_id,
        min(year) as valid_from_year,
        max(year) as valid_to_year
    from 
        {{ ref('stg_fast_f1__race_results') }}
    group by 
        driver_id, 
        driver_number, 
        team_name, 
        team_id
)

select
    {{ dbt_utils.generate_surrogate_key([
        'driver_id',
        'valid_from_year',
        'team_id'
    ]) }} as driver_key,
    driver_id,
    driver_number,
    team_name,
    team_id,
    valid_from_year,
    valid_to_year,
    case
        when valid_to_year = max(valid_to_year) over (partition by driver_id)
        then true
        else false
    end as is_current
from driver_periods