-- recce smoke test: validacao do data review em PR
with base as (
    select * from {{ ref('raw_unidades') }}
),

tipo as (
    select * from {{ ref('raw_tipos_unidade') }}
),

capacidade as (
    select * from {{ ref('int_capacidade_unidades') }}
),

planilha_email as (
    select
        lower(trim(unidade_atendimento)) as nome_unidade,
        string_agg(email, ', ') as email_planilha
    from {{ ref('raw_sheets_filtro_email_prontuario') }}
    group by 1
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['b.id_unidade']) }} as id_unidade_sk,
        b.id_unidade,
        b.nome_unidade,
        b.cas,
        b.esfera,
        b.email_unidade,
        concat(
            coalesce(
                case
                    when lower(trim(b.cas)) = '10' then 'cas10@prefeitura.rio'
                    when lower(trim(b.cas)) = '09' then 'cas9@prefeitura.rio'
                    when lower(trim(b.cas)) = '08' then 'cas8@prefeitura.rio'
                    when lower(trim(b.cas)) = '07' then 'cas7@prefeitura.rio'
                    when lower(trim(b.cas)) = '06' then 'cas6@prefeitura.rio'
                    when lower(trim(b.cas)) = '05' then 'cas5@prefeitura.rio'
                    when lower(trim(b.cas)) = '04' then 'cas4@prefeitura.rio'
                    when lower(trim(b.cas)) = '03' then 'cas3@prefeitura.rio'
                    when lower(trim(b.cas)) = '02' then 'cas2@prefeitura.rio'
                    when lower(trim(b.cas)) = '01' then 'cas1@prefeitura.rio'
                end,
                ''
            ), ',',
            coalesce(pe.email_planilha, ''), ', ',
            coalesce(b.email_unidade, '')
        ) as email_filtro,
        b.flag_unidade_ativa,
        t.id_tipo_unidade,
        t.nome_tipo,
        t.classe,
        t.descricao_classe,
        case
            when nome_tipo is null then 'Não Informado'
            when lower(nome_tipo) like 'albergue%' then 'Albergue'
            when lower(nome_tipo) like 'central de recepção%' then 'Central de Recepção'
            when lower(nome_tipo) like '%cras%' then 'CRAS'
            when lower(nome_tipo) like '%creas%' then 'CREAS'
            when lower(nome_tipo) like 'centro de ref.espec.popula%' then 'Centro POP'
            when lower(nome_tipo) like 'complexo da urs%' then 'URS'
            when lower(nome_tipo) like 'urs%' then 'URS'
            when lower(nome_tipo) = 'república' then 'República'
            when lower(nome_tipo) = 'moradia primeiro' then 'Lares Cariocas'
            when lower(nome_tipo) like '%unid.de acolhi.institucional conveniadas%' then 'Conveniada'
            when nome_tipo = 'Unid.de Acolhi.Inst.Conv.Pess.Deficiência- Adultos' then 'Conveniada'
            else nome_tipo
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
    left join planilha_email pe on lower(trim(b.nome_unidade)) = pe.nome_unidade
    where b.nome_unidade not like '%TESTE%'
)

select * from final
