-- recce-review-trigger PR #113: validação do data review em staging.
with fct_atendimentos as (
    select * from {{ ref('fct_atendimentos') }}
),

filtro_email_dev as (
    select * from {{ source('dashboard_acolherio', 'filtro_email_dev') }}
),

-- Restaura os atributos dimensionais que o fct_atendimentos deixou de expor (ver cda9167).
base_enriquecida as (
    select
        a.*,
        dp.nome as profissional,
        dp.cbo_principal_descricao as profissional_cbo,
        dun.tipo_unidade,
        cast(date_diff(current_date(), du.data_nascimento, year) as int64) as idade,
        dun.nome_unidade as unidade_atendimento,
        a.tipo_atendimento_descricao as nome_atendimento_original,
        a.data_atendimento as data_de_atendimento,
        a.data_atendimento as data_cadastro_atendimento,
        a.hora_atendimento as hora_de_atendimento,
        a.id_profissional as profissional_id,
        concat(dun.email_filtro, ',', dun.email_unidade, ',', z.email) as email
    from fct_atendimentos as a
    left join {{ ref('dim_usuarios') }} as du on a.id_usuario_sk = du.id_usuario_sk
    left join {{ ref('dim_profissionais') }} as dp on a.id_profissional_sk = dp.id_profissional_sk
    left join {{ ref('dim_unidades') }} as dun on a.id_unidade_sk = dun.id_unidade_sk
    left join filtro_email_dev as z
        on dun.nome_unidade = z.unidade_atendimento
),

base_preparada as (
    select
        a.*,
        case
            when a.nome_atendimento_original like '%Recepção%' then 'Atendimento Recepção'
            when a.profissional = 'ATENDIMENTO RECEPÇÃO' then 'Atendimento Recepção'
            when
                a.profissional_cbo in ('Administrador', 'Articulador Comunitário', 'Assistente administrativo', 'Educador social', 'Orientador social', 'Recepcionista')
                and a.nome_atendimento_original like '%CadÚnico%'
                then 'Atendimento Recepção'
            when a.profissional_cbo in ('Advogado', 'Assistente social', 'Pedagogo', 'Psicólogo') then 'Atendimento Técnico'
            else 'Outros Atendimentos'
        end as tipo_atendimento,

        case
            when
                a.profissional_cbo in ('Administrador', 'Articulador Comunitário', 'Assistente administrativo', 'Educador social', 'Orientador social', 'Recepcionista')
                and a.tipo_unidade = 'CRAS'
                and a.nome_atendimento_original like '%CadÚnico%'
                then 'CRAS - Recepção - Ação CadÚnico'
            when
                a.profissional_cbo in ('Administrador', 'Articulador Comunitário', 'Assistente administrativo', 'Educador social', 'Orientador social', 'Recepcionista')
                and a.tipo_unidade = 'CREAS'
                and a.nome_atendimento_original like '%CadÚnico%'
                then 'CREAS - Recepção - Ação CadÚnico'
            else a.nome_atendimento_original
        end as nome_atendimento,

        case
            when a.idade < 18 then 'Até 17 anos'
            when a.idade >= 18 and a.idade < 30 then 'De 18 a 29 anos'
            when a.idade >= 30 and a.idade < 45 then 'De 30 a 44 anos'
            when a.idade >= 45 and a.idade < 60 then 'De 45 a 59 anos'
            when a.idade >= 60 and a.idade < 75 then 'De 60 a 74 anos'
            when a.idade >= 75 then 'Mais de 75 anos'
            else 'Não Informado'
        end as idade_faixa
    from base_enriquecida as a
)

select
    *,
    row_number() over (
        partition by id_atendimento
        order by data_cadastro_atendimento
    ) as cbo_unico_rank,

    row_number() over (
        partition by profissional_id, hora_de_atendimento, data_de_atendimento, nome_atendimento_original, id_usuario, unidade_atendimento
        order by data_cadastro_atendimento
    ) as atendimento_unico_rank,

    {{ extrair_ultima_atualizacao('raw_configuracoes_sistema') }} as ultima_atualizacao

from base_preparada

qualify
    cbo_unico_rank = 1
    and atendimento_unico_rank = 1
