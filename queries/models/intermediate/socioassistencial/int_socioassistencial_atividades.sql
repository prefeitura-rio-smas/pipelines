-- Reutiliza o catálogo, mas lê presenças raw para preservar usuários de teste.
-- A fonte não fornece PK: cópias exatas da chave composta são consolidadas e auditáveis.
with presencas as (
    select
        id_atividade,
        id_usuario,
        data_presenca,
        hora_presenca,
        id_unidade,
        id_setor,
        count(*) as quantidade_linhas_origem
    from {{ ref('raw_presencas_usuarios') }}
    group by id_atividade, id_usuario, data_presenca, hora_presenca, id_unidade, id_setor
)

select
    'ATIVIDADE_COLETIVA' as tipo_registro,
    'prontuario:gh_ativ_paciente' as origem_registro,
    {{ dbt_utils.generate_surrogate_key([
        'p.id_atividade', 'p.id_usuario', 'p.data_presenca', 'p.hora_presenca', 'p.id_unidade', 'p.id_setor'
    ]) }} as id_registro_origem,
    p.id_usuario,
    cast(null as int64) as id_familia,
    coalesce(p.id_unidade, a.id_unidade) as id_unidade,
    date(p.data_presenca) as data_evento,
    {{ inicio_mes('p.data_presenca') }} as competencia,
    {{ fim_mes('p.data_presenca') }} as data_referencia,
    struct(
        p.id_atividade,
        p.hora_presenca,
        p.id_unidade as id_unidade_presenca,
        a.id_unidade as id_unidade_atividade,
        p.id_setor,
        p.quantidade_linhas_origem,
        a.nome_atividade,
        a.id_tipo_atividade,
        a.nome_tipo_atividade,
        a.recorrencia,
        {{ classificar_atividade_rma('a') }}
    ) as atividade
from presencas as p
left join {{ ref('dim_atividades_grupo') }} as a on p.id_atividade = a.id_atividade
