with config as (
    select * from {{ ref('raw_configuracao_unidades') }}
),

capacidade as (
    select * from {{ ref('raw_capacidade_unidades') }}
),

unificado as (
    select
        config.id_unidade,
        config.total_leitos,
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
)

select * from unificado
