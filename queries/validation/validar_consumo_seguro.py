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
active_paths = None
macro = Environment(autoescape=False).from_string(Path('queries/macros/dominios/prontuario_eletronico/consumo_seguro.sql').read_text()).module

def sql_type(f):
    t = 'struct<' + ','.join(x.name+' '+sql_type(x) for x in f.fields) + '>' if f.field_type == 'RECORD' else f.field_type
    return 'array<'+t+'>' if f.mode == 'REPEATED' else t

def value(f, unit=None, path=''):
    path += '.' + f.name
    if f.mode == 'REPEATED':
        if f.name in ('registros', 'historico_mensal', 'projetos_sociais'):
            if active_paths is not None and path not in active_paths:
                return 'cast([] as '+sql_type(f)+')'
            return '[' + ','.join(record(f.fields, u, path) for u in range(1, 6)) + ']'
        if f.name == 'profissionais':
            return '[' + record(f.fields, unit, path) + ',' + record(f.fields, unit, path) + ']'
        if f.name in ('formularios', 'familias', 'servicos'):
            return '[' + record(f.fields, unit, path) + ']'
        return 'cast([] as '+sql_type(f)+')'
    if f.field_type == 'RECORD':
        return record(f.fields, unit, path)
    if f.name == 'email_filtro':
        return "'" + ({1:' A@example.invalid; admin@example.invalid, ab@example.invalid', 2:'b@example.invalid,admin@example.invalid,ab@example.invalid', 3:'c@example.invalid,admin@example.invalid',4:'',5:'a@example.invalid'}.get(unit,'global@example.invalid')) + "'"
    if f.name == 'id_unidade' and unit == 5:
        return 'null'
    if f.field_type in ('INTEGER', 'INT64'):
        return str(unit or 999)
    if f.field_type == 'DATE':
        return "date '2026-11-01'"
    if f.field_type == 'DATETIME':
        return "datetime '2026-11-01 00:00:00'"
    if f.field_type == 'BOOLEAN':
        return 'true'
    if f.name == 'origem_modulo':
        return "'familia'"
    if f.name == 'tipo_servico':
        return "'cras'"
    if f.name == 'id_unidade':
        return "'"+str(unit or 999)+"'"
    return "'" + ('GLOBAL' if unit is None else 'UNIT'+str(unit)) + "'"

def record(fields, unit=None, path=''):
    return 'struct(' + ','.join(value(f, unit, path)+' as '+f.name for f in fields) + ')'

source = '(select ' + ','.join(value(f)+' as '+f.name for f in schema) + ')'
for identity, expected in [('a@example.invalid',1),(' A@EXAMPLE.INVALID ',1),('ab@example.invalid',2),('admin@example.invalid',3),('example.invalid',0),('aa@example.invalid',0),('unknown@example.invalid',0),('',0),(None,0)]:
    sql = macro.prontuario_consumo_seguro(source, '@DS_USER_EMAIL')
    job = client.query('select to_json_string(t) as resultado from ('+sql+') t', job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('DS_USER_EMAIL','STRING',identity)], maximum_bytes_billed=5_000_000_000))
    rows=list(job.result())
    assert len(rows) == bool(expected), (identity, len(rows))
    if rows:
        d=json.loads(rows[0].resultado)
        for name in ['atendimentos','evolucoes','centro_pop','creas','alta_complexidade']:
            assert len(d[name]['registros']) == expected, name
            assert [int(r['id_unidade']) for r in d[name]['registros']] == list(range(1, expected + 1)), name
        assert len(d['centro_pop']['historico_mensal']) == expected
        assert len(d['projetos_sociais']) == expected
        assert d['atendimentos']['quantidade'] == expected
        assert all(len(r['profissionais']) == 2 for r in d['atendimentos']['registros'])
        assert d['atendimentos']['quantidade_cras'] == expected
        assert d['evolucoes']['quantidade'] == expected
        assert all(f['servicos'] == [] for f in d['familias'])
        assert d['usuario']['logradouro'] is None
        assert d['usuario']['flag_possui_violacao_direito'] is None
        assert d['centro_pop']['observacoes_pai'] is None
        assert d['creas']['mse_observacoes'] is None
        if expected == 1:
            assert 'UNIT2' not in rows[0].resultado and 'UNIT3' not in rows[0].resultado
        assert 'UNIT4' not in rows[0].resultado and 'UNIT5' not in rows[0].resultado
    print(json.dumps({'identity': identity, 'expected':expected,'passed':True,'bytes':job.total_bytes_processed,'slot_ms':job.slot_millis}))

for active_paths in [set(), {'.evolucoes.registros'}, {'.centro_pop.historico_mensal'}]:
    source = '(select ' + ','.join(value(f)+' as '+f.name for f in schema) + ')'
    sql = macro.prontuario_consumo_seguro(source, "'a@example.invalid'")
    job = client.query('select count(*) as n from ('+sql+')',job_config=bigquery.QueryJobConfig(maximum_bytes_billed=5_000_000_000))
    assert next(iter(job.result())).n == bool(active_paths)
    print(json.dumps({'only_paths':sorted(active_paths), 'passed':True}))

# Opcional: exercita um cidadão real multiunidade sem imprimir dados pessoais.
if '--real' in sys.argv:
    q = f'''select to_json_string(p) as original from `{TABLE}` p
    where (select count(distinct r.id_unidade) from unnest(p.atendimentos.registros) r) > 1
    limit 1'''
    original_job = client.query(q, job_config=bigquery.QueryJobConfig(maximum_bytes_billed=5_000_000_000))
    originals=list(original_job.result())
    assert originals, 'Não foi encontrado cidadão multiunidade'
    original=json.loads(originals[0].original)
    paths=['projetos_sociais','atendimentos.registros','evolucoes.registros','centro_pop.registros','centro_pop.historico_mensal','creas.registros','alta_complexidade.registros']
    def records(d, path):
        for part in path.split('.'):
            d=d[part]
        return d
    def allowed(r, email):
        return str(r.get('id_unidade') or '').strip() != '' and email in [t.strip().lower() for t in re.split('[;,\r\n]',r.get('email_filtro') or '')]
    emails={e.strip().lower() for r in original['atendimentos']['registros'] for e in re.split('[;,\r\n]',r.get('email_filtro') or '') if e.strip()}
    selected=next((e for e in sorted(emails) if 0 < sum(allowed(r,e) for r in original['atendimentos']['registros']) < len(original['atendimentos']['registros'])),None)
    assert selected, 'Amostra sem destinatário de escopo parcial; selecionar outra amostra'
    relation=f'(select * from `{TABLE}` where usuario.id_usuario = @cidadao)'
    query=macro.prontuario_consumo_seguro(relation,'@DS_USER_EMAIL')
    config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('DS_USER_EMAIL','STRING',selected),bigquery.ScalarQueryParameter('cidadao','STRING',original['usuario']['id_usuario'])],maximum_bytes_billed=5_000_000_000)
    job=client.query('select to_json_string(t) as resultado from ('+query+') t',job_config=config)
    result=json.loads(next(iter(job.result())).resultado)
    for path in paths:
        expected=[r for r in records(original,path) if allowed(r,selected)]
        for r in expected:
            r['email_filtro']=selected
            if path == 'atendimentos.registros':
                r['projetos_sociais_familia'] = None
        assert records(result,path)==expected, path
    assert result['atendimentos']['quantidade']==len(result['atendimentos']['registros'])
    print(json.dumps({'real_mult_unit_isolation':True,'bytes':job.total_bytes_processed,'slot_ms':job.slot_millis,'sample_bytes':original_job.total_bytes_processed}))

    dry_config=bigquery.QueryJobConfig(query_parameters=config.query_parameters,dry_run=True,use_query_cache=False)
    dry=client.query(query,job_config=dry_config)
    def shape(fields):
        return [(f.name,f.field_type,f.mode=='REPEATED',shape(f.fields)) for f in fields]
    assert shape(dry.schema)==shape(schema), 'Schema de consumo difere do contrato publicado'
    print(json.dumps({'schema_equal':True,'dry_bytes':dry.total_bytes_processed}))
