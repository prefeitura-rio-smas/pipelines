-- depends_on: {{ ref('mart_planilha_unificada_centro_pop') }}

with usuarios_mes as (
    select * from unnest([
        struct('positivo_depois_negativo' as id_usuario_unidade_mes, 1 as id_usuario, date '2026-09-30' as data_referencia),
        ('negativo_depois_desconhecido', 2, date '2026-09-30'),
        ('todos_desconhecidos', 3, date '2026-09-30'),
        ('positivo_futuro', 4, date '2026-09-30'),
        ('limite_inclusivo', 5, date '2026-09-30'),
        ('desempate_por_id', 6, date '2026-09-30'),
        ('sem_formulario', 7, date '2026-09-30')
    ])
),

formularios as (
    select * from unnest([
        struct(
            1 as id_paciente, 'usuario:1' as id_evolucao, date '2026-08-01' as data_evolucao,
            'Diabetes' as situacao, 'CAPS' as local_tratamento, 'Sim' as uso_substancias
        ),
        (1, 'familia:1', date '2026-09-01', 'Não', 'Não', 'Não'),
        (2, 'usuario:2', date '2026-08-01', 'Não', 'Não', 'Sim'),
        (2, 'usuario:3', date '2026-09-01', 'Não informado', 'Não sabe', 'Não'),
        (3, 'usuario:4', date '2026-09-01', null, null, 'Não Informado'),
        (4, 'usuario:5', date '2026-09-01', 'Não', 'Não', 'Não'),
        (4, 'usuario:6', date '2026-10-01', 'Diabetes', 'CAPS', 'Sim'),
        (5, 'usuario:7', date '2026-09-30', 'Diabetes', 'CAPS', 'Sim'),
        (6, 'usuario:8', date '2026-09-30', 'Não', 'Não', 'Não'),
        (6, 'usuario:9', date '2026-09-30', 'Não', 'CAPS', 'Sim')
    ])
),

respostas_saude as (
    select
        id_paciente,
        id_evolucao,
        data_evolucao,
        uso_substancias,
        {{ interpretar_resposta_saude('situacao') }} as possui_problema_saude,
        {{ interpretar_resposta_saude('local_tratamento') }} as possui_acompanhamento_saude
    from formularios
),

obtidos as (
    {{ situacao_saude_ate_mes('usuarios_mes', 'respostas_saude') }}
),

esperados as (
    select * from unnest([
        struct(
            'positivo_depois_negativo' as id_usuario_unidade_mes,
            true as possui_historico_problema_saude,
            false as possui_acompanhamento_saude,
            'Não' as uso_substancias
        ),
        ('negativo_depois_desconhecido', false, null, 'Não'),
        ('todos_desconhecidos', null, null, 'Não Informado'),
        ('positivo_futuro', false, false, 'Não'),
        ('limite_inclusivo', true, true, 'Sim'),
        ('desempate_por_id', false, true, 'Sim')
    ])
)

select coalesce(o.id_usuario_unidade_mes, e.id_usuario_unidade_mes) as caso
from obtidos as o
full join esperados as e on o.id_usuario_unidade_mes = e.id_usuario_unidade_mes
where
    o.id_usuario_unidade_mes is null
    or e.id_usuario_unidade_mes is null
    or o.possui_historico_problema_saude is distinct from e.possui_historico_problema_saude
    or o.possui_acompanhamento_saude is distinct from e.possui_acompanhamento_saude
    or o.uso_substancias is distinct from e.uso_substancias
