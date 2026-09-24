{#
Choose one latest related record per event, using the event date as the cutoff.

Parameters:
- event_relation / record_relation: SQL relations or CTE names.
- key_pairs: list of {event: column, record: column} equality join keys.
- select_columns: record columns returned alongside event_id.
- event_id / event_date / record_date / record_id: configurable key and date
  columns; record_id is the deterministic tie breaker for equal record dates.

This helper is generic to any event-to-history relationship. Callers provide
the domain-specific keys and columns; no source or business rule is embedded.
#}
{% macro latest_record_as_of(
    event_relation,
    record_relation,
    key_pairs,
    select_columns,
    event_id,
    event_date,
    record_date,
    record_id
) %}
(
    select
        e.{{ event_id }},
        {% for column in select_columns %}
        r.{{ column }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ event_relation }} as e
    inner join {{ record_relation }} as r
        on
            {% for pair in key_pairs %}
            e.{{ pair['event'] }} = r.{{ pair['record'] }}
            {% if not loop.last %}and{% endif %}
            {% endfor %}
            and safe_cast(r.{{ record_date }} as date) <= e.{{ event_date }}
    qualify row_number() over (
        partition by e.{{ event_id }}
        order by r.{{ record_date }} desc, r.{{ record_id }} desc
    ) = 1
)
{% endmacro %}
