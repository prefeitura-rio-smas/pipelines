-- Não há ID de vínculo na fonte. A chave de conteúdo identifica o registro
-- observado; quantidade_linhas_origem conserva a multiplicidade de cópias exatas.
with vinculos as (
    select
        id_familia,
        id_servico_assistencial,
        id_unidade,
        id_login_cadastro,
        data_cadastro,
        data_cancelamento,
        count(*) as quantidade_linhas_origem
    from {{ ref('raw_familias_servicos_assistenciais') }}
    group by
        id_familia, id_servico_assistencial, id_unidade,
        id_login_cadastro, data_cadastro, data_cancelamento
)

select
    'VINCULO_SERVICO' as tipo_registro,
    'prontuario:gh_famil_servassist' as origem_registro,
    {{ dbt_utils.generate_surrogate_key([
        'id_familia', 'id_servico_assistencial', 'id_unidade',
        'id_login_cadastro', 'data_cadastro', 'data_cancelamento'
    ]) }} as id_registro_origem,
    cast(null as int64) as id_usuario,
    id_familia,
    id_unidade,
    date(data_cadastro) as data_evento,
    {{ inicio_mes('data_cadastro') }} as competencia,
    date(data_cadastro) as data_referencia,
    struct(
        id_servico_assistencial as id_servico,
        id_login_cadastro,
        data_cadastro,
        data_cancelamento,
        quantidade_linhas_origem,
        data_cadastro is not null and (
            data_cancelamento is null or data_cancelamento >= data_cadastro
        ) as flag_intervalo_valido
    ) as vinculo_servico
from vinculos
