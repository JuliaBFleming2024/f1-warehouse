{% macro format_duration_ns(time_column) %}
(
    --This macro will take a duration in nanoseconds, and format in a 
    --  Hour:Minute:Second:Milisecond. Nanoseonds is the default timing returned 
    --  by the fastf1 resource. If a different unit is found, convert to nano first 
    --  before calling the macro. (Note, if I end up having a mix of units in the future,
    --  change the macro so that it converts from seconds always convert before calling)
    case
        when cast({{ time_column }} as numeric) = 'NaN' then null
        else lpad(
            (floor({{ time_column }}::numeric / 1000000000 / 3600))::text,
            2,
            '0'
        ) || ':' ||
        lpad(
            (floor({{ time_column }}::numeric / 1000000000) % 3600 / 60)::text,
            2,
            '0'
        ) || ':' ||
        lpad(
            (floor({{ time_column }}::numeric / 1000000000) % 60)::text,
            2,
            '0'
        ) || '.' ||
        lpad(
            (floor({{ time_column }}::numeric % 1000000000) / 1000000)::text,
            3,
            '0'
        )
    end

    -- lpad handles the leading zeros
    -- floor rounds to the nearest whole number
)
{% endmacro %}
