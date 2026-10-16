{{ config(
    partition_by={'field': 'competencia', 'data_type': 'date', 'granularity': 'month'},
    cluster_by=['id_unidade', 'tipo_registro', 'id_usuario'],
    tags=['socioassistencial_unificada']
) }}

-- UNION BY NAME mantém STRUCTs tipados específicos de cada natureza; os demais
-- são NULL. O grão nunca é expandido pelos atributos multivalorados do contexto.
with registros as (
    {% for modelo, grupo in [
        ('int_socioassistencial_atendimentos', 'atendimento'),
        ('int_socioassistencial_acompanhamentos', 'acompanhamento'),
        ('int_socioassistencial_vinculos_servicos', 'vinculo_servico'),
        ('int_socioassistencial_atividades', 'atividade'),
        ('int_socioassistencial_formularios', 'formulario'),
        ('int_socioassistencial_encaminhamentos', 'encaminhamento'),
        ('int_socioassistencial_abordagens', 'abordagem')
    ] %}
        select
            tipo_registro,
            origem_registro,
            id_registro_origem,
            id_usuario,
            id_familia,
            id_unidade,
            data_evento,
            competencia,
            data_referencia,
            {{ grupo }}
        from {{ ref(modelo) }}
        {% if not loop.last %}full outer union all by name{% endif %}
    {% endfor %}
)

select
    {{ dbt_utils.generate_surrogate_key(['r.tipo_registro', 'r.origem_registro', 'r.id_registro_origem']) }} as id_registro,
    r.tipo_registro,
    r.origem_registro,
    r.id_registro_origem,
    r.id_usuario,
    r.id_familia,
    r.id_unidade,
    un.nome_unidade,
    coalesce(
        d.tipo_unidade,
        if(un.id_unidade is not null, {{ map_tipo_unidade('un.nome_unidade') }}, null)
    ) as tipo_unidade,
    coalesce(un.cas, r.abordagem.cas_informada) as cas,
    r.data_evento,
    r.competencia,
    r.data_referencia,
    case
        when r.tipo_registro = 'ABORDAGEM' then datetime(r.abordagem.timestamp_captura, 'America/Sao_Paulo')
        else {{ extrair_ultima_atualizacao('raw_configuracoes_sistema') }}
    end as ultima_atualizacao,
    current_timestamp() as instante_materializacao,
    {{ calc_idade('coalesce(u.usuario.data_nascimento, r.abordagem.data_nascimento)', 'r.data_referencia') }} as idade_na_referencia,
    u.usuario,
    u.perfil_social,
    f.familia,
    r.atendimento,
    r.acompanhamento,
    r.vinculo_servico,
    r.atividade,
    r.formulario,
    r.encaminhamento,
    r.abordagem,
    pop.planilha_centro_pop
from registros as r
left join {{ ref('raw_unidades') }} as un on r.id_unidade = un.id_unidade
left join {{ ref('dim_unidades') }} as d on r.id_unidade = d.id_unidade
left join {{ ref('int_socioassistencial_contexto_usuarios') }} as u on r.id_usuario = u.id_usuario
left join {{ ref('int_socioassistencial_contexto_familias') }} as f on r.id_familia = f.id_familia
left join {{ ref('int_socioassistencial_contexto_centro_pop') }} as pop
    on
        r.tipo_registro = 'ATENDIMENTO'
        and r.atendimento.seq_atendimento = pop.id_atendimento
        and r.id_usuario is not distinct from pop.id_usuario
        and r.id_unidade is not distinct from pop.id_unidade
        and r.data_evento is not distinct from pop.data_atendimento
