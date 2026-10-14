{{ config(materialized='ephemeral') }}

-- Uma linha por atendimento e posição profissional.
with atendimentos as (
    {% for modulo, modelo in [('familia', 'raw_atendimentos_familias'), ('usuario', 'raw_atendimentos_usuarios')] %}
        select
            id_atendimento_modulo,
            id_atendimento,
            id_unidade,
            id_paciente as id_usuario,
            {{ 'id_familia' if modulo == 'familia' else 'cast(null as int64)' }} as id_familia, -- noqa: AL09
            id_tipo_atendimento,
            data_atendimento,
            data_cadastro_atendimento,
            data_saida,
            hora_atendimento,
            '{{ modulo }}' as origem_modulo,
            flag_cancelado,
            id_login_cadastro,
            safe_cast(trim(cast(id_profissional as string)) as int64) as id_profissional,
            id_profissional_compartilhado
        from {{ ref(modelo) }}
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select
    a.* except (id_profissional, id_profissional_compartilhado),
    p.id_profissional,
    p.flag_profissional_principal
from atendimentos as a,
    unnest(array_concat(
        [struct(a.id_profissional, true as flag_profissional_principal)],
        array(
            select as struct
                id_profissional,
                false as flag_profissional_principal
            from (
                select distinct safe_cast(trim(codigo) as int64) as id_profissional
                from unnest(split(a.id_profissional_compartilhado)) as codigo
            ) as compartilhados
            where id_profissional != 0 and id_profissional is distinct from a.id_profissional
        )
    )) as p
