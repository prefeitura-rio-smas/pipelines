with base as (
    select * from {{ ref('raw_unidades') }}
),

tipo as (
    select * from {{ ref('raw_tipos_unidade') }}
),

capacidade as (
    select * from {{ ref('int_capacidade_unidades') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['b.id_unidade']) }} as id_unidade_sk,
        b.id_unidade,
        b.nome_unidade,
        b.cas,
        b.esfera,
        b.email_unidade,
        b.flag_unidade_ativa,
        t.id_tipo_unidade,
        t.nome_tipo,
        t.classe,
        t.descricao_classe,
        case
            when t.nome_tipo is null then 'Não Informado'
            when lower(t.nome_tipo) like 'albergue%' then 'Albergue'
            when lower(t.nome_tipo) like 'central de recepção%' then 'Central de Recepção'
            when lower(t.nome_tipo) like '%cras%' then 'CRAS'
            when lower(t.nome_tipo) like '%creas%' then 'CREAS'
            when lower(t.nome_tipo) like 'centro de ref.espec.popula%' then 'Centro POP'
            when lower(t.nome_tipo) like 'complexo da urs%' then 'URS'
            when lower(t.nome_tipo) like 'urs%' then 'URS'
            when lower(t.nome_tipo) = 'república' then 'República'
            when lower(t.nome_tipo) = 'moradia primeiro' then 'Lares Cariocas'
            when upper(b.nome_unidade) like '%ESCRITÓRIO%' then 'Escritório Social'
            else t.nome_tipo
        end as tipo_unidade,
        cap.total_vagas,
        cap.vagas_disponiveis,
        cap.vagas_bloqueadas,
        cap.vagas_homens,
        cap.vagas_mulheres,
        cap.vagas_neutras,
        cap.leitos_bloqueados_infra,
        cap.leitos_bloqueados_judiciais,
        cap.flag_administra_leitos,
        cap.tipo_publico,
        cap.flag_acessibilidade,
        cap.grau_dependencia,
        cap.abrangencia,
        cap.flag_eixo_adulto,
        cap.flag_eixo_familia,
        cap.flag_eixo_idoso
    from base b
    left join tipo t on b.id_tipo_unidade = t.id_tipo_unidade
    left join capacidade cap on b.id_unidade = cap.id_unidade
    where b.nome_unidade not like '%TESTE%'
)

select * from final
