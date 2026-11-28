{% macro registros_projetos_sociais(incluir_login=false) %}
select
    id_familia,
    id_projeto_social,
    id_unidade,
    indicador_ativo,
    data_cadastro,
    data_cancelamento
    {% if incluir_login %}
        , id_login_cadastro
        , to_json_string(p) as desempate
    {% endif %}
from {{ ref('raw_familias_projetos_sociais') }} as p
{% endmacro %}

{% macro projetos_dos_membros() %}
select
    p.id_projeto_social,
    p.id_unidade,
    p.id_login_cadastro,
    p.data_cadastro,
    p.data_cancelamento,
    p.indicador_ativo,
    m.id_paciente as id_usuario
from {{ ref('raw_membros_familia') }} as m
inner join ({{ registros_projetos_sociais(incluir_login=true) }}) as p on m.id_familia = p.id_familia
where m.data_saida is null
qualify row_number() over (
    partition by m.id_paciente, p.id_projeto_social, p.id_unidade
    order by p.data_cadastro desc nulls last, p.desempate desc
) = 1
{% endmacro %}
