-- Fixtures usam valores posicionais nas linhas UNION ALL.
-- noqa: disable=AL03
{{ config(enabled=target.name != 'prod', tags=['planilha_unificada']) }}
with casos as (
    select * from unnest([
        struct(1 as evento, 'u' as modulo, 1 as usuario, 1 as profissional, 1 as tipo, 900 as hora, '' as compartilhados, 'N' as cancelado, 1 as operador),
        (2, 'f', 1, 1, 2, 1000, '2,2,01', 'N', 1),
        (10, 'f', 1, 2, 2, 1100, '', 'N', 1),
        (11, 'f', 1, 2, 2, 1100, '', 'N', 1),
        (20, 'u', 1, 2, 2, 1200, '', 'N', 1),
        (20, 'f', 2, 2, 2, 1300, '', 'N', 1),
        (30, 'f', 1, 2, 2, 1400, '', 'S', 1),
        (40, 'f', 1, 2, 2, 1500, '', 'N', 99),
        (50, 'u', 3, 2, 2, 1600, '', 'N', 1),
        (60, 'f', 4, 2, 2, 1700, '', 'N', 1),
        (70, 'f', 1, 99, 2, 1800, '', 'N', 1)
    ])
),

fixture_raw_atendimentos_usuarios as (
    select
        evento as id_atendimento,
        date '2026-01-01' as data_atendimento,
        hora as hora_atendimento,
        cast(null as date) as data_saida,
        tipo as id_tipo_atendimento,
        profissional as id_profissional,
        usuario as id_paciente,
        10 as id_unidade,
        datetime '2026-01-01 09:00:00' as data_cadastro_atendimento,
        '' as local_atendimento,
        cancelado as flag_cancelado,
        compartilhados as id_profissional_compartilhado,
        operador as id_login_cadastro
    from casos
    where modulo = 'u'
),

fixture_raw_atendimentos_familias as (
    select
        evento as id_atendimento,
        date '2026-01-01' as data_atendimento,
        hora as hora_atendimento,
        cast(null as date) as data_saida,
        tipo as id_tipo_atendimento,
        profissional as id_profissional,
        usuario as id_paciente,
        10 as id_unidade,
        datetime '2026-01-01 09:00:00' as data_cadastro_atendimento,
        '' as local_atendimento,
        cancelado as flag_cancelado,
        compartilhados as id_profissional_compartilhado,
        operador as id_login_cadastro,
        100 + usuario as id_familia
    from casos
    where modulo = 'f'
),

fixture_raw_usuarios as (
    select
        id as id_paciente,
        '12345678901' as cpf,
        'Rua' as logradouro,
        '1' as numero_endereco,
        '' as complemento_endereco,
        'Centro' as bairro,
        '' as ponto_referencia,
        date '2000-01-01' as data_nascimento,
        '01' as raca_cor,
        'F' as sexo,
        if(id = 4, 'TESTE', 'Usuário') as nome
    from unnest([1, 2, 3, 4]) as id
),

fixture_raw_membros_familia as (
    select
        1 as id_paciente,
        101 as id_familia,
        cast(null as date) as data_saida
    union all
    select
        1,
        999,
        date '2025-01-01'
    union all
    select
        3,
        103,
        date '2025-01-01'
),

fixture_raw_tipos_atendimento as (
    select
        1 as id_tipo_atendimento,
        'Recepção CadÚnico' as descricao
    union all
    select
        2,
        'Atendimento Social'
),

fixture_raw_unidades as (
    select
        10 as id_unidade,
        'CRAS UNIDADE' as nome_unidade,
        '01' as cas,
        'unidade@example.test' as email_unidade
),

fixture_raw_operadores as (
    select
        1 as id_login,
        'Operador' as nome_operador
),

fixture_raw_profissionais as (
    select
        1 as id_profissional,
        'Assistente' as nome
    union all
    select
        2,
        'Educador'
),

fixture_raw_profissionais_ocupacoes as (
    select
        1 as id_profissional,
        251605 as codigo_cbo
    union all
    select
        1,
        999999
    union all
    select
        2,
        515305
),

fixture_raw_cbo as (
    select
        251605 as codigo_cbo,
        'Assistente social' as descricao
    union all
    select
        999999,
        'Outro'
    union all
    select
        515305,
        'Educador social'
),

fixture_raw_familias_projetos_sociais as (
    select
        101 as id_familia,
        99 as id_projeto_social,
        'S' as indicador_ativo,
        cast(null as date) as data_cancelamento
    union all
    select
        101,
        1,
        'S',
        date '2025-01-01'
    union all
    select
        101,
        2,
        'N',
        cast(null as date)
),

fixture_email as (
    select
        'CRAS UNIDADE' as unidade_atendimento,
        'gestor@example.test' as email
),

obtido as (
{{ pu_atendimentos_bi_sql({'raw_atendimentos_usuarios': 'fixture_raw_atendimentos_usuarios', 'raw_atendimentos_familias': 'fixture_raw_atendimentos_familias', 'raw_membros_familia': 'fixture_raw_membros_familia', 'raw_usuarios': 'fixture_raw_usuarios', 'raw_tipos_atendimento': 'fixture_raw_tipos_atendimento', 'raw_unidades': 'fixture_raw_unidades', 'raw_operadores': 'fixture_raw_operadores', 'raw_profissionais': 'fixture_raw_profissionais', 'raw_profissionais_ocupacoes': 'fixture_raw_profissionais_ocupacoes', 'raw_cbo': 'fixture_raw_cbo', 'raw_familias_projetos_sociais': 'fixture_raw_familias_projetos_sociais'}, 'fixture_email') }}
),

esperado as (
    select * from unnest([
        struct(1 as seq_atendimento, 'u' as modulo, 1 as profissional_id, 251605 as _codigo_cbo, 'Atendimento Recepção' as tipo_atendimento, 101 as id_familia),
        struct(2 as seq_atendimento, 'f' as modulo, 1 as profissional_id, 251605 as _codigo_cbo, 'Atendimento Técnico' as tipo_atendimento, 101 as id_familia),
        struct(2 as seq_atendimento, 'f' as modulo, 2 as profissional_id, 515305 as _codigo_cbo, 'Outros Atendimentos' as tipo_atendimento, 101 as id_familia),
        struct(10 as seq_atendimento, 'f' as modulo, 2 as profissional_id, 515305 as _codigo_cbo, 'Outros Atendimentos' as tipo_atendimento, 101 as id_familia),
        struct(20 as seq_atendimento, 'f' as modulo, 2 as profissional_id, 515305 as _codigo_cbo, 'Outros Atendimentos' as tipo_atendimento, 102 as id_familia),
        struct(30 as seq_atendimento, 'f' as modulo, 2 as profissional_id, 515305 as _codigo_cbo, 'Outros Atendimentos' as tipo_atendimento, 101 as id_familia),
        struct(70 as seq_atendimento, 'f' as modulo, cast(null as int64) as profissional_id, cast(null as int64) as _codigo_cbo, 'Outros Atendimentos' as tipo_atendimento, 101 as id_familia)
    ])
),

comparacao as (
    select
        to_json_string(struct(seq_atendimento, modulo, profissional_id, _codigo_cbo, tipo_atendimento, id_familia)) as conteudo,
        1 as delta
    from obtido
    union all
    select
        to_json_string(e),
        -1
    from esperado as e
),

falhas as (
    select conteudo from comparacao
    group by conteudo
    having sum(delta) != 0
    union all
    select 'projetos_sociais' from obtido
    where id_familia = 101 and projetos_sociais != 'Projeto não mapeado (ID 99)'
)

select * from falhas
