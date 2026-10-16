-- Um vínculo pode existir sem qualquer atendimento. Mantemos inclusive vínculos
-- encerrados no mês de ingresso, que participam das entradas, mas não do estoque.
with vinculos_identificados as (
    select
        id_familia,
        vinculo_servico.id_servico as id_servico_assistencial,
        id_unidade,
        vinculo_servico.id_login_cadastro,
        vinculo_servico.data_cadastro,
        vinculo_servico.data_cancelamento,
        id_registro_origem as id_vinculo_origem
    from {{ ref('int_socioassistencial_vinculos_servicos') }}
    where vinculo_servico.flag_intervalo_valido
),

vinculos_mensais as (
    select
        v.*,
        competencia
    from vinculos_identificados as v,
        unnest(generate_date_array(
            {{ inicio_mes('v.data_cadastro') }},
            least(
                coalesce({{ inicio_mes('v.data_cancelamento') }}, {{ mes_referencia() }}),
                {{ mes_referencia() }},
                {{ inicio_mes('current_date()') }}
            ), interval 1 month
        )) as competencia
),

familias_servico_unidade_mes as (
    select
        id_familia,
        id_servico_assistencial,
        id_unidade,
        competencia,
        logical_or({{ vigencia_no_fechamento('data_cadastro', 'data_cancelamento', 'competencia') }}) as flag_ativo_fechamento,
        logical_or({{ inicio_mes('data_cadastro') }} = competencia) as flag_novo_no_mes,
        array_agg(struct(
            id_vinculo_origem, id_login_cadastro, data_cadastro, data_cancelamento
        ) order by data_cadastro, id_vinculo_origem) as vinculos
    from vinculos_mensais
    group by id_familia, id_servico_assistencial, id_unidade, competencia
)

select
    'ACOMPANHAMENTO_MENSAL' as tipo_registro,
    'prontuario:gh_famil_servassist' as origem_registro,
    {{ dbt_utils.generate_surrogate_key(['id_familia', 'id_servico_assistencial', 'id_unidade', 'competencia']) }} as id_registro_origem,
    cast(null as int64) as id_usuario,
    id_familia,
    id_unidade,
    cast(null as date) as data_evento,
    competencia,
    {{ fim_mes('competencia') }} as data_referencia,
    struct(
        'FAMILIA' as tipo_sujeito,
        id_servico_assistencial as id_servico,
        {{ nome_servico_socioassistencial('id_servico_assistencial') }} as nome_servico,
        flag_ativo_fechamento,
        flag_novo_no_mes,
        vinculos,
        'unidade_do_vinculo' as origem_unidade,
        competencia = {{ inicio_mes('current_date()') }} as flag_competencia_em_curso
    ) as acompanhamento
from familias_servico_unidade_mes
