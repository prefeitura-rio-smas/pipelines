{% macro ids_profissionais_atendimento(id_principal, ids_compartilhados) %}
    array_concat(
        -- O principal vem primeiro e é preservado mesmo quando desconhecido.
        [safe_cast(trim(cast({{ id_principal }} as string)) as int64)],
        array(
            select distinct id_profissional
            from (
                select safe_cast(trim(valor) as int64) as id_profissional
                from unnest(split({{ ids_compartilhados }}, ',')) as valor
            ) as compartilhados
            where
                id_profissional is not null
                and id_profissional != 0
                and id_profissional is distinct from safe_cast(trim(cast({{ id_principal }} as string)) as int64)
        )
    )
{% endmacro %}
