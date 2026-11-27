{{ config(tags=['planilha_unificada']) }}
-- depends_on: {{ ref('int_acolherio_atendimentos') }}

with candidatos as (
    select
        evento.seq_atendimento,
        evento.modulo,
        evento.flag_cancelado,
        evento.data_cadastro_atendimento,
        7 as profissional_id,
        '10:00' as hora_de_atendimento,
        date '2026-01-01' as data_de_atendimento,
        'Recepção' as nome_atendimento_original,
        evento.seq_atendimento as id_usuario,
        'CREAS' as unidade_atendimento
    from unnest([
        struct(42 as seq_atendimento, 'u' as modulo, 'N' as flag_cancelado, date '2026-01-02' as data_cadastro_atendimento),
        struct(42, 'f', 'N', date '2026-01-01'),
        struct(43, 'u', cast(null as string), date '2026-01-02'),
        struct(43, 'f', 'S', date '2026-01-01'),
        struct(44, 'u', 'S', date '2026-01-01'),
        struct(44, 'u', 'N', date '2026-01-02')
    ]) as evento
),

obtido as (
    {{ selecionar_participacoes_atendimento('candidatos') }}
),

esperado as (
    select * from unnest([
        struct(42 as seq_atendimento, 'u' as modulo, date '2026-01-02' as data_cadastro_atendimento),
        struct(42, 'f', date '2026-01-01'),
        struct(43, 'u', date '2026-01-02'),
        struct(44, 'u', date '2026-01-02')
    ])
)

select
    coalesce(e.seq_atendimento, o.seq_atendimento) as seq_atendimento,
    coalesce(e.modulo, o.modulo) as modulo
from esperado as e
full join obtido as o
    on
        e.seq_atendimento = o.seq_atendimento
        and e.modulo = o.modulo
        and e.data_cadastro_atendimento = o.data_cadastro_atendimento
where e.modulo is null or o.modulo is null
