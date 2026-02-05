
with profissionais_sem_teste as (
    select 
        {{ dbt_utils.generate_surrogate_key(['seqprof']) }} as seqprof_sk,
        seqprof,
        sequs,
        seqlogin,
        nomgue,
        crmbloq,
        matricula,
        uf_conselho,
        nome_profissional,
        nome_social_profissional,
        flag_uso_nome_social,
        codigo_csns,
        cpf,
        sexo,
        genero,
        flag_profissional_ativo,
        data_cadastro_profissional,
        data_ultima_modificacao_cadastro,
        email_profissional,
        telefone,
    from {{ ref('stg_profissionais') }}
    where not regexp_contains(nome_profissional, "(?i)teste")
)

select * from profissionais_sem_teste