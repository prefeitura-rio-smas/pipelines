-- Tabela que retorna todas as informações de cada membro
-- Não contém dados testes
-- Existem cpf duplicados

{{ config(materialized='table')}}


with filtro_no_teste as (
    select
        cd_pac.seqpac,
        cd_pac.nome_usuario,
        cd_pac.nome_social,
        cd_pac.filiacao_mae,
        cd_pac.data_nascimento,
        cd_pac.nacionalidade,
        cd_pac.condicao_estrangeira,
        cd_pac.pais_origem,
        cd_pac.bairro,
        cd_pac.raca,
        cd_pac.cpf,
        cd_pac.sexo,
        cd_pac.genero,
        cd_pac.prontuario,
        cd_pac.data_cadastro_usuario,
        cd_dados.flag_cadunico,
        cd_dados.numero_processo_decisao_apoiada,
        cd_dados.nome_apoiador,
        cd_dados.saude_mental_comprometida,
    from {{ ref('stg_cidadao_pac_acolherio') }} cd_pac
    inner join {{ ref('stg_pac_dados_acolherio') }} cd_dados on cd_dados.seqpac = cd_pac.seqpac
    where not regexp_contains(nome_usuario, '(?i)teste')
)

select * from filtro_no_teste