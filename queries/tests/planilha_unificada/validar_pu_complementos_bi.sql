{{ config(tags=['planilha_unificada']) }}
with esperado as (
    select
        cast(r.id_usuario as string) as id_usuario,
        concat(cast(r.seq_atendimento as string), r.modulo) as chave,
        to_json_string(struct(
            coalesce(r.cas, '') as cas,
            coalesce(r.email_unidade, '') as email_unidade,
            coalesce(r.email_cas, '') as email_cas,
            coalesce(r.email, '') as email,
            coalesce(r.cadastrante, '') as cadastrante,
            coalesce(cast(r.data_cadastro_atendimento as string), '') as data_cadastro_atendimento,
            cast(r.profissional_id as string) as id_profissional,
            coalesce(cast(r.id_familia as string), '') as id_familia_referencia,
            coalesce(r.id_atendimento, '') as id_atendimento_legado
        )) as valor
    from {{ pu_programada() }} as r
    where r.profissional_id is not null
),

obtido as (
    select
        p.usuario.id_usuario,
        a.id_atendimento_modulo as chave,
        to_json_string(struct(
            a.cas, a.email_unidade, a.email_cas, a.email, a.cadastrante, a.data_cadastro_atendimento,
            pr.id_profissional, pr.id_familia_referencia, pr.id_atendimento_legado
        )) as valor
    from {{ ref('planilha_unificada') }} as p,
        unnest(p.atendimentos.registros) as a, unnest(a.profissionais) as pr
),

comparacao as (
    select
        *,
        1 as lado
    from esperado
    union all
    select
        *,
        -1 as lado
    from obtido
)

select id_usuario from comparacao
group by id_usuario, chave, valor
having sum(lado) != 0
union all
select p.usuario.id_usuario
from {{ ref('planilha_unificada') }} as p
left join {{ ref('int_pu_enderecos_bi') }} as e on p.usuario.id_usuario = cast(e.id_usuario as string)
where to_json_string(struct(p.usuario.logradouro, p.usuario.numero, p.usuario.complemento, p.usuario.referencia_ou_comunidade)) is distinct from to_json_string(struct(
    coalesce(e.endereco.logradouro, '') as logradouro,
    coalesce(e.endereco.numero, '') as numero,
    coalesce(e.endereco.complemento, '') as complemento,
    coalesce(e.endereco.referencia_ou_comunidade, '') as referencia_ou_comunidade
))
