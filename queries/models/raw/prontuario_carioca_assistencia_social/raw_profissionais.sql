-- Camada Raw: Profissionais do Prontuário Carioca
with source as (
    select 
        seqprof as id_profissional,
        sequs as id_unidade,
        seqlogin as id_login,
        nomgue as nome_guerra,
        crmbloq as flag_bloqueio,
        matrprof as matricula,
        sigufconselho as uf_conselho,
        nomeprof as nome,
        nomsocprof as nome_social,
        indusonomsoc as flag_uso_nome_social,
        codcns as cns,
        cpfprof as cpf,
        indsexo as sexo,
        indgenero as genero,
        indativo as flag_ativo,
        dtcadast as data_cadastro,
        datultmodif as data_ultima_modificacao,
        emailprof as email,
        dsctel as telefone,
    from {{ source('brutos_acolherio_staging', 'gh_prof') }}
)
select * from source
