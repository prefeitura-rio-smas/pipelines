with atendimentos_explodidos as (
    select * from {{ ref('int_atendimentos_acolherio__explodidos') }}
),

p as ( select * from {{ source('brutos_acolherio_staging', 'gh_cidadao_pac') }} ),
c as ( select * from {{ source('brutos_acolherio_staging', 'gh_tpatendimentos') }} ),
e as ( select * from {{ source('brutos_acolherio_staging', 'gh_us') }} ),
s as ( select * from {{ source('brutos_acolherio_staging', 'gh_contas') }} ),
q as ( select * from {{ source('brutos_acolherio_staging', 'gh_prof') }} ),
v as ( select * from {{ source('brutos_acolherio_staging', 'gh_profocup') }} ),
cb as ( select * from {{ source('brutos_acolherio_staging', 'gh_cbo') }} )

select
    b.modulo,
    e.apus as cas,
    case
        when e.dscus like 'ALBERGUE%' then 'ALBERGUE'
        when e.dscus like 'CRAF%' then 'CENTRAL DE RECEPÇÃO'
        when e.dscus like 'CRAS%' then 'CRAS'
        when e.dscus like 'CREAS%' then 'CREAS'
        when e.dscus like 'CRI%' then 'CENTRAL DE RECEPÇÃO'
        when e.dscus like 'REPÚBLICA%' then 'REPÚBLICA'
        when e.dscus like 'URS%' then 'URS'
        else 'UNIDADE CONVENIADA'
    end as tipo_unidade,
    e.dscus as unidade_atendimento,
    e.emailprof as email_unidade,
    case
        when e.apus = '10' then 'cas10@prefeitura.rio'
        when e.apus = '09' then 'cas9@prefeitura.rio'
        when e.apus = '08' then 'cas8@prefeitura.rio'
        when e.apus = '07' then 'cas7@prefeitura.rio'
        when e.apus = '06' then 'cas6@prefeitura.rio'
        when e.apus = '05' then 'cas5@prefeitura.rio'
        when e.apus = '04' then 'cas4@prefeitura.rio'
        when e.apus = '03' then 'cas3@prefeitura.rio'
        when e.apus = '02' then 'cas2@prefeitura.rio'
        when e.apus = '01' then 'cas1@prefeitura.rio'
    end as email_cas,
    b.seqatend as seq_atendimento,
    b.dtentrada as data_de_atendimento,
    b.horaent as hora_de_atendimento_original,
    format('%02d:%02d', div(safe_cast(b.horaent as int64), 100), mod(safe_cast(b.horaent as int64), 100)) as hora_de_atendimento,
    b.seqpac as id_usuario,
    b.seqfamil as id_familia,
    p.dscnomepac as nome_usuario,
    case when p.numcpfpac is null or p.numcpfpac = '' then '' else "CPF" end as documentacao,
    case
        when p.numcpfpac is null or p.numcpfpac = '' then null
        else concat(
            substring(lpad(p.numcpfpac, 11, '0'), 1, 3), '.',
            substring(lpad(p.numcpfpac, 11, '0'), 4, 3), '.',
            substring(lpad(p.numcpfpac, 11, '0'), 7, 3), '-',
            substring(lpad(p.numcpfpac, 11, '0'), 10, 2)
        )
    end as numero_documento,
    trim(regexp_replace(p.dslogradouro, r'[,\s;]+', ' ')) as endereco,
    trim(regexp_replace(p.numend, r'[,\s;]+', ' ')) as endereco_numero,
    trim(regexp_replace(p.complend, r'[,\s;]+', ' ')) as endereco_complemento,
    case when p.dscbairroender = '' then 'NAO INFORMADO' else p.dscbairroender end as bairro,
    trim(regexp_replace(p.pontorefe, r'[,\s;]+', ' ')) as referencia_ou_comunidade,
    c.descatend as nome_atendimento_original,
    p.datnascim as data_nascimento,
    date_diff(current_date(), p.datnascim, year) - if(extract(dayofyear from p.datnascim) > extract(dayofyear from current_date()), 1, 0) as idade,
    case
        when p.racacor = '01' then 'Branca'
        when p.racacor = '02' then 'Preta'
        when p.racacor = '03' then 'Parda'
        when p.racacor = '04' then 'Amarela'
        when p.racacor = '05' then 'Indigena'
        else 'Nao informado'
    end as raca_cor,
    p.indsexo as sexo,
    q.seqprof as profissional_id,
    
    -- Regra de substituição do profissional pelo cadastrante real
    case
        when trim(upper(q.nomeprof)) in ('ATENDIMENTO RECEPÇÃO', 'ATENDIMENTO BUSCA ATIVA', 'ATENDIMENTO ENTREVISTADOR SOCIAL') 
        then trim(upper(s.nompess)) 
        else trim(upper(q.nomeprof))
    end as profissional,

    cb.dsccbo as profissional_cbo_original,
    case
        when cb.dsccbo like 'Articulador Comunitário%' then 'Articulador Comunitário'
        when cb.dsccbo like 'Assistente administrativo%' then 'Assistente administrativo'
        when cb.dsccbo like 'Assistente Social%' then 'Assistente social'
        when cb.dsccbo like 'Assistente social%' then 'Assistente social'
        when cb.dsccbo like 'Educador social%' then 'Educador social'
        when cb.dsccbo like 'Entrevistador Social%' then 'Entrevistador social'
        when cb.dsccbo like 'Pedagogo%' then 'Pedagogo'
        when cb.dsccbo like 'Psicólogo%' then 'Psicólogo'
        when cb.dsccbo like 'Recepcionista%' then 'Recepcionista'
        else cb.dsccbo
    end as profissional_cbo,
    trim(upper(s.nompess)) as cadastrante,
    b.datcadast as data_cadastro_atendimento

from atendimentos_explodidos b
join p on p.seqpac = b.seqpac
left join c on c.seqtpatend = b.seqtpatend
left join e on e.sequs = b.sequs
left join s on s.seqlogin = b.seqlogincad
left join q on q.seqprof = b.seqprof
left join v on v.seqprof = q.seqprof
left join cb on cb.codcbo = v.codcbo

where
    s.nompess not like '%TESTE%'
    and p.dscnomepac not like '%TESTE%' 
    and e.dscus not like '%TESTE%'