# ruff: noqa: S101, S608, S701
"""SELECTs sintéticos sobre a mesma macro do Looker; nenhuma materialização.
Definir PRONTUARIO_VALIDACAO_TABELA com a tabela candidata atual.
Executar da raiz com .venv/bin/python queries/validation/validar_consumo_seguro.py.
"""
import json
import os
from pathlib import Path
import re
import sys

from google.cloud import bigquery
from jinja2 import Environment

TABLE = os.environ['PRONTUARIO_VALIDACAO_TABELA']
client = bigquery.Client(project='rj-smas-dev')
schema = client.get_table(TABLE).schema
assert [f.name for f in schema[:6]] == ['id_usuario_unidade', 'id_usuario', 'id_unidade', 'nome_unidade', 'email_filtro', 'usuario']
assert schema[0].field_type == 'STRING'
active_paths = None
macro = Environment(autoescape=False).from_string(Path('queries/macros/dominios/prontuario_eletronico/consumo_seguro.sql').read_text()).module

def sql_type(f):
    t = 'struct<' + ','.join(x.name+' '+sql_type(x) for x in f.fields) + '>' if f.field_type == 'RECORD' else f.field_type
    return 'array<'+t+'>' if f.mode == 'REPEATED' else t

def value(f, unit=None, path='', profissional=None):
    path += '.' + f.name
    if f.mode == 'REPEATED':
        if f.name in ('registros', 'historico_mensal', 'projetos_sociais'):
            if active_paths is not None and path not in active_paths:
                return 'cast([] as '+sql_type(f)+')'
            participacoes = range(1, unit + 1) if path == '.atendimentos.registros' else [None]
            return '[' + ','.join(record(f.fields, unit, path, n) for n in participacoes) + ']'
        if f.name == 'profissionais':
            return '[' + record(f.fields, unit, path, profissional) + ']'
        if f.name in ('formularios', 'familias', 'servicos'):
            return '[' + record(f.fields, unit, path) + ']'
        return 'cast([] as '+sql_type(f)+')'
    if f.field_type == 'RECORD':
        return record(f.fields, unit, path)
    if f.name == 'email_filtro':
        assert path == '.email_filtro', f'Filtro aninhado: {path}'
        return "'" + ({1:' A@example.invalid; admin@example.invalid, ab@example.invalid', 2:'b@example.invalid,admin@example.invalid,ab@example.invalid', 3:'c@example.invalid,admin@example.invalid',4:'',5:'a@example.invalid'}.get(unit,'global@example.invalid')) + "'"
    if f.name == 'id_unidade' and unit == 5:
        return "''" if path == '.id_unidade' else f'cast(null as {sql_type(f)})'
    if f.name == 'atendimento_compartilhado':
        return "'Sim'" if unit > 1 else "'Não'"
    if f.field_type in ('INTEGER', 'INT64'):
        return str(unit or 999)
    if f.field_type == 'DATE':
        return "date '2026-11-01'"
    if f.field_type == 'DATETIME':
        return "datetime '2026-11-01 00:00:00'"
    if f.field_type == 'BOOLEAN':
        return 'true'
    if path == '.id_usuario_unidade':
        return "'999:" + ('' if unit == 5 else str(unit)) + "'"
    if f.name == 'id_usuario':
        return "'999'"
    if f.name == 'origem_modulo':
        return "'familia'"
    if f.name == 'tipo_servico':
        return "'cras'"
    if f.name == 'id_unidade':
        return "'"+str(unit or 999)+"'"
    return "'" + ('GLOBAL' if unit is None else 'UNIT'+str(unit)) + "'"

def record(fields, unit=None, path='', profissional=None):
    return 'struct(' + ','.join(
        (f"'{profissional}'" if f.name == 'id_profissional' and profissional is not None
         else value(f, unit, path, profissional))
        + ' as ' + f.name for f in fields
    ) + ')'

def fixture(units):
    return '(' + ' union all '.join(
        'select ' + ','.join(value(f, unit)+' as '+f.name for f in schema) for unit in units
    ) + ')'


source = fixture(range(1, 6))
for identity, expected in [('a@example.invalid',1),(' A@EXAMPLE.INVALID ',1),('ab@example.invalid',2),('admin@example.invalid',3),('example.invalid',0),('aa@example.invalid',0),('unknown@example.invalid',0),('',0),(None,0)]:
    sql = macro.prontuario_consumo_seguro(source, '@DS_USER_EMAIL')
    job = client.query('select to_json_string(t) as resultado from ('+sql+') t order by t.id_unidade', job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('DS_USER_EMAIL','STRING',identity)], maximum_bytes_billed=5_000_000_000))
    rows=list(job.result())
    assert len(rows) == expected, (identity, len(rows))
    for unit, row in enumerate(rows, start=1):
        d = json.loads(row.resultado)
        assert d['id_usuario'] == d['usuario']['id_usuario'] == '999'
        assert d['id_unidade'] == str(unit)
        assert d['id_usuario_unidade'] == f'999:{unit}'
        assert d['email_filtro'] == identity.strip().lower()
        for name in ['atendimentos', 'evolucoes', 'centro_pop', 'creas', 'alta_complexidade']:
            assert [int(r['id_unidade']) for r in d[name]['registros']] == [unit] * (unit if name == 'atendimentos' else 1), name
        assert len(d['centro_pop']['historico_mensal']) == 1
        assert len(d['projetos_sociais']) == 1
        assert d['atendimentos']['quantidade'] == d['atendimentos']['quantidade_cras'] == unit
        for n, participacao in enumerate(d['atendimentos']['registros'], start=1):
            assert len(participacao['profissionais']) == 1
            assert participacao['profissionais'][0]['id_profissional'] == str(n)
            assert participacao['atendimento_compartilhado'] == ('Sim' if unit > 1 else 'Não')
        assert d['evolucoes']['quantidade'] == 1
        assert all(f['servicos'] == [] for f in d['familias'])
        assert d['usuario']['logradouro'] is None
        assert d['usuario']['flag_possui_violacao_direito'] is None
        assert d['centro_pop']['observacoes_pai'] is None
        assert d['creas']['mse_observacoes'] is None
    print(json.dumps({'identity': identity, 'expected':expected,'passed':True,'job':job.job_id,'bytes':job.total_bytes_processed,'slot_ms':job.slot_millis}))

for active_paths in [set(), {'.evolucoes.registros'}, {'.centro_pop.historico_mensal'}]:
    source = fixture([1])
    sql = macro.prontuario_consumo_seguro(source, "'a@example.invalid'")
    job = client.query(
        'select count(*) as n from ('+sql+')',
        job_config=bigquery.QueryJobConfig(maximum_bytes_billed=5_000_000_000),
    )
    result = next(iter(job.result()))
    assert result.n == bool(active_paths)
    print(json.dumps({'only_paths':sorted(active_paths), 'passed':True, 'job':job.job_id}))

# Opcional: exercita um cidadão real multiunidade sem imprimir dados pessoais.
if '--real' in sys.argv:
    q = f'''
    with unidades as (
        select usuario.id_usuario, id_unidade, email_filtro
        from `{TABLE}`
        where id_unidade != '' and email_filtro != '' and array_length(atendimentos.registros) > 0
    ), amostra as (
        select a.id_usuario, email
        from unidades as a
        join unidades as b on a.id_usuario = b.id_usuario and a.id_unidade != b.id_unidade
        cross join unnest(split(a.email_filtro, ', ')) as email
        where email not in unnest(split(b.email_filtro, ', '))
        limit 1
    )
    select to_json_string(p) as original, a.email
    from `{TABLE}` as p
    join amostra as a on p.usuario.id_usuario = a.id_usuario
    '''
    original_job = client.query(q, job_config=bigquery.QueryJobConfig(maximum_bytes_billed=5_000_000_000))
    sample = list(original_job.result())
    assert sample, 'Amostra sem cidadão com unidades e permissões diferentes'
    originals = [json.loads(row.original) for row in sample]
    citizen = originals[0]['usuario']['id_usuario']
    selected = sample[0].email
    paths = ['projetos_sociais', 'atendimentos.registros', 'evolucoes.registros', 'centro_pop.registros', 'centro_pop.historico_mensal', 'creas.registros', 'alta_complexidade.registros']

    def records(row, path):
        for part in path.split('.'):
            row = row[part]
        return row

    def recipients(row):
        return {e.strip().lower() for e in re.split('[;,\r\n]', row['email_filtro'] or '') if e.strip()}

    expected_rows = {
        row['id_unidade']: row for row in originals
        if row['id_unidade'] and selected in recipients(row) and any(records(row, path) for path in paths)
    }
    relation = f'(select * from `{TABLE}` where usuario.id_usuario = @cidadao)'
    query = macro.prontuario_consumo_seguro(relation, '@DS_USER_EMAIL')
    config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter('DS_USER_EMAIL', 'STRING', selected),
        bigquery.ScalarQueryParameter('cidadao', 'STRING', citizen),
    ], maximum_bytes_billed=5_000_000_000)
    job = client.query('select to_json_string(t) as resultado from ('+query+') t', job_config=config)
    results = [json.loads(row.resultado) for row in job.result()]
    assert len(results) == len(expected_rows)
    for result in results:
        assert result['id_usuario'] == result['usuario']['id_usuario'] == citizen
        original = expected_rows.pop(result['id_unidade'])
        assert result['id_usuario_unidade'] == original['id_usuario_unidade'] == f"{citizen}:{result['id_unidade']}"
        for path in paths:
            expected = records(original, path)
            if path == 'atendimentos.registros':
                expected = [dict(record, projetos_sociais_familia=None) for record in expected]
            assert records(result, path) == expected, path
        assert result['atendimentos']['quantidade'] == len(result['atendimentos']['registros'])
    print(json.dumps({'real_mult_unit_isolation': True, 'job':job.job_id, 'bytes': job.total_bytes_processed}))

    dry_config=bigquery.QueryJobConfig(query_parameters=config.query_parameters,dry_run=True,use_query_cache=False)
    dry=client.query(query,job_config=dry_config)
    def shape(fields, path=''):
        # A projeção segura omite o resumo técnico na raiz de Centro POP.
        return [
            (f.name, f.field_type, f.mode == 'REPEATED', shape(f.fields, path + '.' + f.name))
            for f in fields
            if (path, f.name) != ('.centro_pop', 'data_ultimo_atend_tecnico_informada')
        ]

    assert shape(dry.schema) == shape(schema), 'Schema de consumo difere do contrato publicado'
    print(json.dumps({'schema_equal':True,'dry_bytes':dry.total_bytes_processed}))
