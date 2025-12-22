with source as (
    select * from {{ ref('stg_fast_f1__schedule') }}
),

sessions as (

    select
        event_weekend_id,
        year,
        country,
        city,
        1 as weekend_session_number,
        session_1_type as session_type,
        session_1_at as session_at,
        session_1_at__utc as session_at_utc

    from source
    where session_1_type is not null

    union all

    select
        event_weekend_id,
        year,
        country,
        city,
        2 as weekend_session_number,
        session_2_type as session_type,
        session_2_at as session_at,
        session_2_at__utc as session_at_utc

    from source
    where session_2_type is not null

    union all

    select
        event_weekend_id,
        year,
        country,
        city,
        3 as weekend_session_number,
        session_3_type as session_type,
        session_3_at as session_at,
        session_3_at__utc as session_at_utc

    from source
    where session_3_type is not null

    union all

    select
        event_weekend_id,
        year,
        country,
        city,
        4 as weekend_session_number,
        session_4_type as session_type,
        session_4_at as session_at,
        session_4_at__utc as session_at_utc

    from source
    where session_4_type is not null

    union all

    select
        event_weekend_id,
        year,
        country,
        city,
        5 as weekend_session_number,
        session_5_type as session_type,
        session_5_at as session_at,
        session_5_at__utc as session_at_utc

    from source
    where session_5_type is not null

)

select
    {{ dbt_utils.generate_surrogate_key([
        'event_weekend_id',
        'weekend_session_number'
    ]) }} as session_id,
    *
from sessions
