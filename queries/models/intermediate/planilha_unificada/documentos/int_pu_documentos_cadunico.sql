-- Snapshot atual: CPF ambíguo não recebe um documento/família arbitrário.
with documentos as (
    select distinct
        nullif(regexp_replace(coalesce(d.cpf, ''), r'[^0-9]', ''), '') as cpf_normalizado,
        safe_cast(trim(d.id_familia) as int64) as id_familia_cadunico,
        nullif(trim(m.nis), '') as nis,
        case
            when nullif(trim(d.rg), '') is null then null
            when regexp_replace(trim(d.rg), r'^0+', '') = '' then '0'
            else regexp_replace(trim(d.rg), r'^0+', '')
        end as numero_rg,
        safe_cast(d.id_certidao_civil as int64) = 1 as possui_certidao_nascimento,
        if(
            safe_cast(d.id_certidao_civil as int64) = 1,
            nullif(trim(d.id_termi_matricula_certidao), ''),
            null
        ) as numero_certidao_nascimento
    from {{ pu_ref('raw_documento_pessoa') }} as d
    left join {{ pu_ref('raw_identificacao_membro') }} as m on d.id_membro_familia = m.id_membro_familia
),

documentos_por_cpf as (
    select
        cpf_normalizado,
        if(count(distinct id_familia_cadunico) = 1, max(id_familia_cadunico), null) as id_familia_cadunico,
        if(count(distinct nis) = 1, max(nis), null) as nis,
        if(count(distinct numero_rg) = 1, max(numero_rg), null) as numero_rg,
        if(logical_or(possui_certidao_nascimento), true, null) as possui_certidao_nascimento,
        if(count(distinct numero_certidao_nascimento) = 1, max(numero_certidao_nascimento), null) as numero_certidao_nascimento,
        count(distinct id_familia_cadunico) > 1 as flag_familia_ambigua,
        count(distinct nis) > 1 as flag_nis_ambiguo,
        count(distinct numero_rg) > 1 as flag_rg_ambiguo,
        count(distinct numero_certidao_nascimento) > 1 as flag_certidao_ambigua
    from documentos
    where cpf_normalizado is not null
    group by cpf_normalizado
),

controle_atual as (
    select
        safe_cast(trim(id_familia) as int64) as id_familia_cadunico,
        safe_cast(data_atualizacao as date) as data_atualizacao,
        safe_cast(data_limite_catastro_atual as date) as data_limite_cadastro_atual
    from {{ pu_ref('raw_identificacao_controle') }}
    where safe_cast(trim(id_familia) as int64) is not null
    qualify row_number() over (
        partition by safe_cast(trim(id_familia) as int64)
        order by data_atualizacao desc, data_alteracao desc, data_limite_catastro_atual desc
    ) = 1
)

select
    u.id_usuario,
    struct(
        d.cpf_normalizado,
        d.id_familia_cadunico,
        d.nis,
        d.numero_rg,
        if(d.numero_rg is not null, true, null) as possui_rg,
        d.possui_certidao_nascimento,
        d.numero_certidao_nascimento,
        c.data_atualizacao,
        c.data_limite_cadastro_atual,
        d.flag_familia_ambigua,
        d.flag_nis_ambiguo,
        d.flag_rg_ambiguo,
        d.flag_certidao_ambigua
    ) as documentos_cadunico
from {{ pu_ref('dim_usuarios') }} as u
inner join documentos_por_cpf as d
    on nullif(regexp_replace(coalesce(u.cpf, ''), r'[^0-9]', ''), '') = d.cpf_normalizado
left join controle_atual as c on d.id_familia_cadunico = c.id_familia_cadunico
