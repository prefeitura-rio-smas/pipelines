{{ config(materialized='ephemeral', tags=['planilha_unificada']) }}
-- Endereço da seleção de atendimentos; cadastro para usuários sem atendimento.
with foto as (
    select distinct
        id_usuario,
        endereco as logradouro,
        endereco_numero as numero,
        endereco_complemento as complemento,
        referencia_ou_comunidade
    from {{ ref('int_acolherio_atendimentos') }}
),

cadastro as (
    select distinct
        id_paciente,
        trim(regexp_replace(logradouro, r'[,\s;]+', ' ')) as logradouro,
        trim(regexp_replace(numero_endereco, r'[,\s;]+', ' ')) as numero,
        trim(regexp_replace(complemento_endereco, r'[,\s;]+', ' ')) as complemento,
        trim(regexp_replace(ponto_referencia, r'[,\s;]+', ' ')) as referencia_ou_comunidade
    from {{ ref('raw_usuarios') }}
)

select
    u.id_usuario,
    struct(
        {% for campo in ['logradouro', 'numero', 'complemento', 'referencia_ou_comunidade'] %}
            if(f.id_usuario is not null, f.{{ campo }}, c.{{ campo }}) as {{ campo }}{% if not loop.last %},{% endif %}
        {% endfor %}
    ) as endereco,
    case
        when f.id_usuario is not null then 'referencia_bi'
        when c.id_paciente is not null then 'cadastro' else 'sem_registro'
    end as origem_endereco
from {{ ref('dim_usuarios') }} as u
left join foto as f on u.id_usuario = f.id_usuario
left join cadastro as c on u.id_usuario = c.id_paciente
