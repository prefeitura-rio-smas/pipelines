{% macro datas_atendimentos_tecnicos_centro_pop() %}
    select
        e.id_usuario,
        e.id_unidade,
        coalesce(
            (
                select max(safe.parse_date('%d/%m/%Y', data_informada))
                from unnest(regexp_extract_all(
                    {{ texto_html_legivel('e.descricao_evolucao') }},
                    r'(?i)Registro\s*:\s*(\d{2}/\d{2}/\d{4})'
                )) as data_informada
            ),
            date(e.data_evolucao)
        ) as data_ultimo_atend_tecnico_informada,
        date(e.data_evolucao) as data_evolucao
    from {{ ref('int_evolucoes_por_usuario') }} as e
    left join {{ ref('dim_profissionais') }} as profissional
        on
            e.origem_modulo != 'administrativa'
            and e.id_profissional = profissional.id_profissional
    left join {{ ref('raw_operadores') }} as operador
        on
            e.origem_modulo = 'administrativa'
            and e.id_profissional = operador.id_login
    where
        e.tipo_servico = 'centro_pop'
        and case
            when e.origem_modulo = 'administrativa' then operador.nivel_conta
            else profissional.nivel_conta
        end in (2, 5)
{% endmacro %}
