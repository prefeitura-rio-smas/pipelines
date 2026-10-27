{{ config(tags=['planilha_unificada']) }}
-- depends_on: {{ ref('planilha_unificada') }}
with campos as (
    select
        field_path,
        data_type
    from `{{ ref('planilha_unificada').database }}.{{ ref('planilha_unificada').schema }}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
    where table_name = '{{ ref('planilha_unificada').identifier }}'
),

esperados as (
    {% for nome in ['uf_nascimento', 'serie_escolar', 'data_cadunico_informada', 'nis', 'numero_rg', 'possui_rg', 'possui_certidao_nascimento', 'numero_certidao_nascimento', 'data_atualizacao', 'data_limite_cadastro_atual', 'flag_familia_ambigua', 'flag_nis_ambiguo', 'flag_rg_ambiguo', 'flag_certidao_ambigua', 'logradouro', 'numero', 'complemento', 'referencia_ou_comunidade'] %}
        {% if not loop.first %} union all {% endif %}
        select
            'usuario.{{ nome }}' as field_path,
            'STRING' as data_type
    {% endfor %}
    {% for bloco in ['atendimentos', 'evolucoes'] %}
        {% for servico in ['cras', 'creas', 'centro_pop', 'outros'] %}
            union all
            select
                '{{ bloco }}.quantidade_{{ servico }}' as field_path,
                'INT64' as data_type
        {% endfor %}
    {% endfor %}
    union all
    select
        'evolucoes.registros.formularios.campos' as field_path,
        'ARRAY<STRING>' as data_type
)

select e.field_path
from esperados as e left join campos as c on e.field_path = c.field_path
where c.field_path is null or c.data_type != e.data_type
union all
select field_path from campos
where regexp_contains(field_path, r'^usuario\.(cadastro_complementar|documentos_cadunico|endereco)($|\.)|^(atendimentos|evolucoes)\.quantidade_por_servico($|\.)')
