-- Model responsável pelos profissionais do Acolherio

with profissionais as (
    select 
        seqprof,
        sequs,
        seqlogin,
        nomgue,
        crmbloq,
        matrprof as matricula,
        sigufconselho as uf_conselho,
        nomeprof as nome_profissional,
        nomsocprof as nome_social_profissional,
        indusonomsoc as flag_uso_nome_social,
        codcns as codigo_csns,
        cpfprof as cpf,
        indsexo as sexo,
        indgenero as genero,
        indativo as flag_profissional_ativo,
        dtcadast as data_cadastro_profissional,
        datultmodif as data_ultima_modificacao_cadastro,
        emailprof as email_profissional,
        dsctel as telefone,
    from {{ source('brutos_acolherio_staging', 'gh_prof') }}
)

select * from profissionais