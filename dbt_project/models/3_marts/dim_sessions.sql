select
    session_id,
    event_weekend_id,
    year,
    country,
    city,
    weekend_session_number,
    session_type,
    session_at_utc,
    is_final_session
from
    {{ ref('int_unpivot_weekends_to_sessions') }}
