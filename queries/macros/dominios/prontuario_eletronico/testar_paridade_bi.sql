{% test pu_selecao_bi_paridade(model) %}
-- Preserva a seleção e os campos originais; a data da base fica auditável, enquanto a data pública vem do formulário.
{% set colunas = [
    'modulo',
    'cas',
    'tipo_unidade',
    'unidade_atendimento',
    'seq_atendimento',
    'data_de_atendimento',
    'hora_de_atendimento_original',
    'hora_de_atendimento',
    'id_usuario',
    'id_familia',
    'nome_usuario',
    'documentacao',
    'numero_documento',
    'endereco',
    'endereco_numero',
    'endereco_complemento',
    'bairro',
    'referencia_ou_comunidade',
    'nome_atendimento_original',
    'data_nascimento',
    'idade',
    'raca_cor',
    'sexo',
    'profissional_id',
    'profissional',
    'profissional_cbo_original',
    'profissional_cbo',
    'cadastrante',
    'data_cadastro_atendimento',
    'tipo_atendimento',
    'nome_atendimento',
    'idade_faixa',
    'id_atendimento',
    'cbo_unico_rank',
    'atendimento_unico_rank',
    'projetos_sociais'
] %}
with comparacao as (
    {% for relacao, delta in [(ref('raw_atendimentos_programada'), 1), (model, -1)] %}
    {% if not loop.first %}union all{% endif %}
    select to_json_string(struct(
        {% for coluna in colunas %}
            {% if coluna == 'data_de_atendimento' and delta == -1 %}_data_de_atendimento_base{% else %}{{ coluna }}{% endif %} as {{ coluna }}{% if not loop.last %},{% endif %}
        {% endfor %}
    )) as conteudo, {{ delta }} as delta
    from {{ relacao }}
    {% endfor %}
)
select conteudo from comparacao group by conteudo having sum(delta) != 0
{% endtest %}

{% test pu_linhas_bi_preservadas(model) %}
with esperado as (
    select
        cast(id_usuario as string) as id_usuario,
        concat(cast(seq_atendimento as string), modulo) as id_atendimento_modulo,
        coalesce(cast(id_familia as string), '') as id_familia,
        coalesce(cast(data_de_atendimento as string), '') as data_atendimento,
        coalesce(hora_de_atendimento, '') as hora_atendimento,
        coalesce(unidade_atendimento, '') as nome_unidade,
        coalesce(nome_atendimento_original, '') as tipo_atendimento_descricao,
        coalesce(cas, '') as cas,
        coalesce(email_unidade, '') as email_unidade,
        coalesce(email_cas, '') as email_cas,
        coalesce(cadastrante, '') as cadastrante,
        coalesce(cast(data_cadastro_atendimento as string), '') as data_cadastro_atendimento,
        coalesce(projetos_sociais, '') as projetos_sociais_familia,
        coalesce(cast(profissional_id as string), '') as id_profissional,
        coalesce(profissional, '') as nome_profissional,
        coalesce(profissional_cbo_original, '') as cbo_principal_descricao,
        coalesce(profissional_cbo, '') as profissional_cbo,
        coalesce(tipo_atendimento, '') as classificacao_atendimento,
        coalesce(nome_atendimento, '') as nome_atendimento,
        coalesce(id_atendimento, '') as id_atendimento_legado
    from {{ ref('int_atendimentos_bi') }}
), obtido as (
    select
        p.usuario.id_usuario as id_usuario,
        a.id_atendimento_modulo as id_atendimento_modulo,
        coalesce(cast(pr.id_familia_referencia as string), '') as id_familia,
        a.data_atendimento as data_atendimento,
        a.hora_atendimento as hora_atendimento,
        a.nome_unidade as nome_unidade,
        a.tipo_atendimento_descricao as tipo_atendimento_descricao,
        a.cas as cas,
        a.email_unidade as email_unidade,
        a.email_cas as email_cas,
        a.cadastrante as cadastrante,
        a.data_cadastro_atendimento as data_cadastro_atendimento,
        a.projetos_sociais_familia as projetos_sociais_familia,
        pr.id_profissional as id_profissional,
        pr.nome_profissional as nome_profissional,
        pr.cbo_principal_descricao as cbo_principal_descricao,
        pr.profissional_cbo as profissional_cbo,
        pr.classificacao_atendimento as classificacao_atendimento,
        pr.nome_atendimento as nome_atendimento,
        pr.id_atendimento_legado as id_atendimento_legado
    from {{ model }} as p,
        unnest(p.atendimentos.registros) as a,
        unnest(a.profissionais) as pr
), comparacao as (
    select to_json_string(e) as conteudo, 1 as delta from esperado as e
    union all
    select to_json_string(o), -1 from obtido as o
)
select conteudo from comparacao group by conteudo having sum(delta) != 0
{% endtest %}

{% test pu_quantidades_bi(model) %}
select p.usuario.id_usuario
from {{ model }} as p
where p.atendimentos.quantidade != array_length(p.atendimentos.registros)
    or p.atendimentos.quantidade_linhas_bi != (
        select coalesce(sum(array_length(a.profissionais)), 0)
        from unnest(p.atendimentos.registros) as a
    )
{% endtest %}
