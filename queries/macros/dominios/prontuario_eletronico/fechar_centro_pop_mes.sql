{% macro fechar_centro_pop_mes(mes, id_fechamento, nova_versao=false) %}
    {% set data = modules.datetime.datetime.strptime(mes, '%Y-%m-%d').date() %}
    {% if data.day != 1 or mes < '2026-11-01' or data >= modules.datetime.datetime.now(modules.pytz.timezone('America/Sao_Paulo')).date().replace(day=1) %}
        {{ exceptions.raise_compiler_error('Fechar somente competências encerradas, no primeiro dia do mês, a partir de novembro/2026.') }}
    {% endif %}
    {% if not modules.re.match('^[A-Za-z0-9_-]{1,100}$', id_fechamento) %}
        {{ exceptions.raise_compiler_error('id_fechamento deve conter apenas letras, números, hífen ou sublinhado.') }}
    {% endif %}
    {% set candidata = ref('centro_pop_mensal') %}
    {% set destino = ref('centro_pop_snapshots') %}
    {% set sql %}
create table if not exists `{{ destino.database }}.{{ destino.schema }}.centro_pop_fechamento_controle`
options(description='Controle de concorrência dos fechamentos Centro POP') as select 1 as chave, '' as nonce;
begin transaction;
update `{{ destino.database }}.{{ destino.schema }}.centro_pop_fechamento_controle` set nonce = generate_uuid() where chave = 1;
assert not exists (select 1 from {{ destino }} where id_fechamento = '{{ id_fechamento }}' and mes_referencia != date('{{ mes }}')) as 'Identificador já usado em outra competência';
if not exists (select 1 from {{ destino }} where id_fechamento = '{{ id_fechamento }}') then
    assert not exists (select 1 from {{ candidata }} where data_carga_fontes is null) as 'Metadados de carga ausentes';
    assert not exists (select 1 from {{ candidata }} group by id_usuario, id_unidade, mes_referencia having count(*) != 1) as 'Chaves mensais duplicadas';
    assert not exists (select 1 from {{ candidata }} where id_unidade is null) as 'Unidade ausente';
    assert (select count(*) from {{ candidata }}) > 0 as 'Candidata vazia';
    assert (select countif(mes_referencia != date('{{ mes }}') or data_atualizacao < timestamp_sub(current_timestamp(), interval 24 hour)) from {{ candidata }}) = 0 as 'Reconstruir e validar a candidata para a competência solicitada';
    {% if nova_versao is not sameas true %}
    assert not exists (select 1 from {{ destino }} where mes_referencia = date('{{ mes }}')) as 'Mês fechado: solicitar nova_versao explicitamente';
    {% endif %}
    insert into {{ destino }}
    select * replace (
        'Fechado' as situacao,
        (select coalesce(max(versao), 0) + 1 from {{ destino }} where mes_referencia = date('{{ mes }}')) as versao,
        current_timestamp() as data_snapshot,
        '{{ id_fechamento }}' as id_fechamento
    ) from {{ candidata }};
end if;
commit transaction;
{% endset %}
    {% do run_query(sql) %}
{% endmacro %}
