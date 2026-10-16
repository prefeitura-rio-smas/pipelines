-- CadÚnico é associado por CPF, sem confundir sua família com a do prontuário.
with documentos_cadunico as (
    select
        s.cpf_normalizado,
        if(count(distinct s.nis) = 1, max(s.nis), null) as nis,
        if(
            count(distinct safe_cast(trim(s.id_familia) as int64)) = 1,
            max(safe_cast(trim(s.id_familia) as int64)),
            null
        ) as id_familia_cadunico,
        if(
            count(distinct s.numero_rg_normalizado) = 1,
            max(s.numero_rg_normalizado),
            null
        ) as numero_rg,
        if(
            count(distinct s.numero_rg_normalizado) = 1,
            'Sim',
            'Não Informado'
        ) as flag_possui_rg,
        if(logical_or(safe_cast(s.id_certidao_civil as int64) = 1), 'Sim', 'Não Informado') as flag_possui_certidao_nascimento,
        if(
            count(distinct if(
                safe_cast(s.id_certidao_civil as int64) = 1,
                nullif(trim(s.id_termi_matricula_certidao), ''),
                null
            )) = 1,
            max(if(
                safe_cast(s.id_certidao_civil as int64) = 1,
                nullif(trim(s.id_termi_matricula_certidao), ''),
                null
            )),
            null
        ) as numero_certidao_nascimento
    from (
        select
            d.id_familia,
            d.id_certidao_civil,
            d.id_termi_matricula_certidao,
            m.nis,
            nullif(regexp_replace(coalesce(d.cpf, ''), r'[^0-9]', ''), '') as cpf_normalizado,
            case
                when nullif(trim(d.rg), '') is null then null
                when regexp_replace(trim(d.rg), r'^0+', '') = '' then '0'
                else regexp_replace(trim(d.rg), r'^0+', '')
            end as numero_rg_normalizado
        from {{ ref('raw_documento_pessoa') }} as d
        left join {{ ref('raw_identificacao_membro') }} as m on d.id_membro_familia = m.id_membro_familia
        where nullif(regexp_replace(coalesce(d.cpf, ''), r'[^0-9]', ''), '') is not null
    ) as s
    group by s.cpf_normalizado
)

select * from documentos_cadunico
