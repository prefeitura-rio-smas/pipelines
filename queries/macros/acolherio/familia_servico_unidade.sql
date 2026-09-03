{% macro familia_servico_unidade(id_servico, tipo_unidade) %}
    -- Famílias vinculadas a um serviço assistencial × membros ativos, com
    -- atribuição de unidade em 3 níveis (serviço → operador de cadastro →
    -- atendimento mais recente no tipo de unidade informado).
    -- Colunas: id_familia, data_cadastro_servico, id_usuario,
    -- data_nascimento, beneficio, violacoes, vulnerabilidades, id_unidade.
    -- Ex. CRAS/PAIF: familia_servico_unidade(1, 'CRAS').
    -- Ex. CREAS/PAEFI: familia_servico_unidade(6, 'CREAS').
    -- (Subqueries aninhadas, sem WITH: BigQuery não permite WITH dentro de FROM.)
    (
        select
            p.id_familia,
            p.data_cadastro_servico,
            m.id_usuario,
            u.data_nascimento,
            u.beneficio,
            u.violacoes,
            vf.vulnerabilidades,
            coalesce(p.id_unidade, ul.id_unidade, af.id_unidade) as id_unidade
        from (
            select
                id_familia,
                id_unidade,
                id_login_cadastro,
                data_cadastro as data_cadastro_servico
            from {{ ref('raw_familias_servicos_assistenciais') }}
            where
                id_servico_assistencial = {{ id_servico }}
                and data_cancelamento is null
        ) as p
        inner join (
            select
                id_familia,
                id_paciente as id_usuario
            from {{ ref('raw_membros_familia') }}
            where data_saida is null
        ) as m on p.id_familia = m.id_familia
        inner join (
            select
                id_usuario,
                data_nascimento,
                beneficio,
                violacoes
            from {{ ref('dim_usuarios') }}
        ) as u on m.id_usuario = u.id_usuario
        left join (
            select
                id_familia,
                array_agg(
                    struct(
                        id_vulnerabilidade,
                        data_cadastro
                    )
                ) as vulnerabilidades
            from {{ ref('raw_familias_vulnerabilidades') }}
            where data_cancelamento is null
            group by id_familia
        ) as vf on p.id_familia = vf.id_familia
        left join (
            select
                id_login,
                array_agg(id_unidade order by id_unidade limit 1)[offset(0)] as id_unidade
            from {{ ref('raw_operadores_unidades') }}
            group by id_login
        ) as ul on p.id_login_cadastro = ul.id_login
        left join (
            select
                a.id_familia,
                array_agg(a.id_unidade order by a.data_atendimento desc, a.id_unidade asc limit 1)[offset(0)] as id_unidade
            from {{ ref('raw_atendimentos_familias') }} as a
            inner join {{ ref('dim_unidades') }} as d
                on
                    a.id_unidade = d.id_unidade
                    and d.tipo_unidade = '{{ tipo_unidade }}'
            where {{ nao_cancelado('a.flag_cancelado') }}
            group by a.id_familia
        ) as af on p.id_familia = af.id_familia
    )
{% endmacro %}
