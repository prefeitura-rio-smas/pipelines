{% macro esquema_publicacao_prontuario(colunas) -%}
    {% set esquema = [] %}
    {% for coluna in colunas %}
        {% do esquema.append([coluna.name, coluna.dtype, coluna.mode, esquema_publicacao_prontuario(coluna.fields)]) %}
    {% endfor %}
    {{ return(esquema) }}
{%- endmacro %}

{% macro publicar_prontuario_dev(schema_candidata, schema_destino='abordagem') %}
    {% set prefixo_ci = '^ci_prontuario_[a-z0-9_]+$' %}
    {% if target.database != 'rj-smas-dev' or target.name == 'prod'
        or not modules.re.match(prefixo_ci, schema_candidata)
        or (schema_destino != 'abordagem' and not modules.re.match(prefixo_ci, schema_destino))
        or schema_candidata == schema_destino %}
        {{ exceptions.raise_compiler_error('Use uma candidata isolada ci_prontuario_* e um destino distinto em rj-smas-dev.') }}
    {% endif %}

    {% set candidata = adapter.get_relation(database=target.database, schema=schema_candidata, identifier='prontuario_eletronico') %}
    {% set existente = adapter.get_relation(database=target.database, schema=schema_destino, identifier='prontuario_eletronico') %}
    {% if candidata is none or not candidata.is_table or (existente is not none and not existente.is_table) %}
        {{ exceptions.raise_compiler_error('A candidata deve existir como tabela e o destino deve ser uma tabela ou estar ausente.') }}
    {% endif %}

    {% set colunas = adapter.get_columns_in_relation(candidata) %}
    {% if existente is not none and esquema_publicacao_prontuario(colunas) != esquema_publicacao_prontuario(adapter.get_columns_in_relation(existente)) %}
        {{ exceptions.raise_compiler_error('A candidata apresenta esquema diferente do destino dev.') }}
    {% endif %}

    {% set destino = api.Relation.create(database=target.database, schema=schema_destino, identifier='prontuario_eletronico') %}
    {% set controle = api.Relation.create(database=target.database, schema=schema_destino, identifier='prontuario_eletronico_publicacao') %}
    {% set nomes = colunas | map(attribute='quoted') | list %}

    {% set sql %}
        declare criacao_candidata timestamp default (
            select creation_time
            from `rj-smas-dev.{{ schema_candidata }}.INFORMATION_SCHEMA.TABLES`
            where table_name = 'prontuario_eletronico'
        );
        declare linhas_alteradas int64 default 0;

        assert (
            select count(*) > 0 and count(*) = count(distinct nullif(id_usuario_unidade, ''))
            from {{ candidata }}
        ) as 'Candidata vazia ou com chave inválida';

        create table if not exists {{ destino }} like {{ candidata }} options(expiration_timestamp=null);
        create table if not exists {{ controle }} as
        select timestamp '1970-01-01 00:00:00+00' as ultima_criacao, cast(null as string) as ultimo_schema;

        begin transaction;

        -- A mesma linha de controle serializa publicações e impede cargas antigas.
        update {{ controle }}
        set ultima_criacao = criacao_candidata, ultimo_schema = '{{ schema_candidata }}'
        where ultima_criacao < criacao_candidata
            or (ultima_criacao = criacao_candidata and ultimo_schema = '{{ schema_candidata }}');

        select if(@@row_count = 1, true, error('Candidata anterior à última publicação ou controle inválido'));

        merge {{ destino }} as d
        using {{ candidata }} as c
        on d.id_usuario_unidade = c.id_usuario_unidade
        when matched and to_json_string(d) is distinct from to_json_string(c) then
            update set
                {% for nome in nomes if nome != '`id_usuario_unidade`' %}
                    {{ nome }} = c.{{ nome }}{% if not loop.last %},{% endif %}
                {% endfor %}
        when not matched then
            insert ({{ nomes | join(', ') }})
            values (
                {% for nome in nomes %}
                    c.{{ nome }}{% if not loop.last %},{% endif %}
                {% endfor %}
            )
        when not matched by source then delete;

        set linhas_alteradas = @@row_count;
        commit transaction;

        select linhas_alteradas;
    {% endset %}

    {% set resultado = run_query(sql) %}
    {% do log('Linhas alteradas no Prontuário dev: ' ~ resultado.rows[0][0], info=true) %}
{% endmacro %}
