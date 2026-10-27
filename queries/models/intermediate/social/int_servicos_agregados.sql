-- Serviços socioassistenciais das famílias, agregados por família (1 linha por id_familia).
-- Código→rótulo vem do seed acolherio_dominios (dominio='servassist'): mapeamento
-- código→rótulo é DADO, não macro (ver ops/macros/taxonomia-macros.md §3).
with servicos as (
    select distinct
        id_familia,
        id_servico_assistencial,
        id_unidade,
        id_login_cadastro,
        data_cadastro,
        data_cancelamento
    from {{ ref('raw_familias_servicos_assistenciais') }}
),

dominios as (
    select
        codigo,
        rotulo
    from {{ ref('acolherio_dominios') }}
    where
        dominio = 'servassist'
        and (vigente_ate is null or vigente_ate >= current_date())
),

servicos_com_nome as (
    select
        servicos.id_familia,
        servicos.id_servico_assistencial,
        servicos.id_unidade,
        servicos.id_login_cadastro,
        servicos.data_cadastro,
        servicos.data_cancelamento,
        coalesce(
            dominios.rotulo,
            format('Serviço não mapeado (código %s)', cast(servicos.id_servico_assistencial as string))
        ) as nome_servico_socioassistencial
    from servicos
    left join dominios
        on cast(servicos.id_servico_assistencial as string) = dominios.codigo
)

select
    id_familia,

    -- Legado: array sem nome do serviço, preservado para dim_familias e consumidores
    -- existentes. Ganhou ORDER BY para deixar de ser não determinístico.
    array_agg(struct(
        id_servico_assistencial,
        data_cadastro,
        id_login_cadastro as id_profissional
    ) order by id_servico_assistencial, data_cadastro, id_login_cadastro) as servicos,

    -- Atributo vigente: nomes dos serviços sem data de cancelamento.
    string_agg(
        distinct if(data_cancelamento is null, nome_servico_socioassistencial, null),
        ', ' order by if(data_cancelamento is null, nome_servico_socioassistencial, null)
    ) as servicos_ativos,

    -- Grão de vínculo dentro da linha da família: todos os vínculos, inclusive cancelados.
    array_agg(struct(
        id_servico_assistencial,
        nome_servico_socioassistencial,
        id_unidade,
        id_login_cadastro as id_profissional,
        data_cadastro,
        data_cancelamento
    ) order by id_servico_assistencial, data_cadastro, data_cancelamento, id_unidade, id_login_cadastro)
        as servicos_historico
from servicos_com_nome
group by id_familia
