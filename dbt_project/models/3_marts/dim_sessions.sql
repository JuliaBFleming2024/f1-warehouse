select
    session_id,
    event_weekend_id,
    year,
    country,
    city,
    weekend_session_number,
    session_type,
    session_at_utc
from
    {{ ref('int_unpivot_weekends_to_sessions') }}


