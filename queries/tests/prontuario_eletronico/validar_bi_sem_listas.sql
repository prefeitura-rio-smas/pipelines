{{ config(tags=['prontuario_bi_unidade']) }}
{% for nome in ['prontuario_bi_atendimentos', 'prontuario_bi_linhas_profissionais', 'prontuario_bi_evolucoes', 'prontuario_bi_perfil_unidade'] %}
    {% set tabela = ref(nome) %}
    select
        '{{ nome }}' as fonte,
        field_path
    from `{{ tabela.database }}.{{ tabela.schema }}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
    where table_name = '{{ tabela.identifier }}' and starts_with(data_type, 'ARRAY<')
    {% if not loop.last %}union all{% endif %}
{% endfor %}
