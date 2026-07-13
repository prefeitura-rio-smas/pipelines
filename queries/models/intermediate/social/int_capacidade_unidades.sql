with config as (
    select * from {{ ref('raw_configuracao_unidades') }}
),

capacidade as (
    select * from {{ ref('raw_capacidade_unidades') }}
),

-- Processa os eixos da unidade (campo indeixo separado por vírgulas)
eixos_unnest as (
    select
        cap.id_unidade,
        trim(eixo) as eixo
    from {{ ref('raw_capacidade_unidades') }} cap
    left join unnest(split(ifnull(cap.tipo_publico, ''), ',')) as eixo
),

eixos_agregados as (
    select
        id_unidade,
        {{ map_coluna_indeixo_adulto('eixo') }} as flag_eixo_adulto,
        {{ map_coluna_indeixo_familia('eixo') }} as flag_eixo_familia,
        {{ map_coluna_indeixo_idoso('eixo') }} as flag_eixo_idoso
    from eixos_unnest
    group by id_unidade
),

unificado as (
    select
        config.id_unidade,
        config.total_leitos,
        config.leitos_bloqueados as vagas_bloqueadas,
        config.leitos_bloqueados_infra,
        config.leitos_bloqueados_judiciais,
        config.flag_administra_leitos,
        capacidade.tipo_publico,
        capacidade.flag_acessibilidade,
        capacidade.vagas_homens,
        capacidade.vagas_mulheres,
        capacidade.vagas_neutras,
        capacidade.grau_dependencia,
        capacidade.abrangencia,
        eixos.flag_eixo_adulto,
        eixos.flag_eixo_familia,
        eixos.flag_eixo_idoso,
        case when config.flag_administra_leitos = 'S'
            then coalesce(capacidade.vagas_homens, 0)
                + coalesce(capacidade.vagas_mulheres, 0)
                + coalesce(capacidade.vagas_neutras, 0)
            else coalesce(config.total_leitos, 0)
        end as total_vagas,
        case when config.flag_administra_leitos = 'S'
            then coalesce(capacidade.vagas_homens, 0)
                + coalesce(capacidade.vagas_mulheres, 0)
                + coalesce(capacidade.vagas_neutras, 0)
                - coalesce(config.leitos_bloqueados_infra, 0)
                - coalesce(config.leitos_bloqueados_judiciais, 0)
            else coalesce(config.total_leitos, 0)
                - coalesce(config.leitos_bloqueados_infra, 0)
                - coalesce(config.leitos_bloqueados_judiciais, 0)
        end as vagas_disponiveis
    from config
    left join capacidade on config.id_unidade = capacidade.id_unidade
    left join eixos_agregados eixos on config.id_unidade = eixos.id_unidade
)

select * from unificado
