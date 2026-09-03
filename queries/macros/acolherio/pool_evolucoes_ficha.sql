{% macro pool_evolucoes_ficha(aba_ficha, incluir_familia_membros=true) %}
    -- Pool de evoluções elegível a RMA: módulo administrativo tipo 'F'
    -- registrado na aba informada + (opcional) módulo família explodido
    -- nos membros ativos (com surrogate keys resolvidas).
    -- Colunas: id_evolucao_sk, id_usuario_sk, id_unidade_sk, id_unidade,
    -- data_evolucao, descricao_evolucao.
    -- Ex. CRAS: pool_evolucoes_ficha('CRAS - Ficha de Atendimento Individualizado').
    (
        select
            e.id_evolucao_sk,
            e.id_usuario_sk,
            e.id_unidade_sk,
            e.id_unidade,
            e.data_evolucao,
            e.descricao_evolucao
        from {{ ref('fct_evolucoes') }} as e
        where
            e.origem_modulo = 'administrativa'
            and e.tipo_evolucao = 'F'
            and regexp_extract(e.descricao_evolucao, r'<h3>(.*?)</h3>')
            = '{{ aba_ficha }}'
        {% if incluir_familia_membros %}
        union all
        select
            f.id_evolucao_sk,
            u.id_usuario_sk,
            f.id_unidade_sk,
            f.id_unidade,
            f.data_evolucao,
            f.descricao_evolucao
        from {{ ref('fct_evolucoes') }} as f
        inner join {{ ref('raw_membros_familia') }} as m
            on f.id_familia = m.id_familia
        inner join {{ ref('dim_usuarios') }} as u
            on m.id_paciente = u.id_usuario
        where
            f.origem_modulo = 'familia'
            and m.data_saida is null
        {% endif %}
    )
{% endmacro %}
