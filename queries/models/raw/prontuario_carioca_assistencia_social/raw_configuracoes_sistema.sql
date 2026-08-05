-- Camada Raw: Configurações do sistema
with source as (

    select
        _airbyte_extracted_at as data_extracao_origem,
        codcnf          as id_configuracao,
        indtipocnf      as tipo_configuracao,
        dsctitcnf       as titulo_configuracao,
        dscicocnf       as icone_configuracao,
        dscclasscnf     as classe_configuracao,
        dsccsscnf       as css_configuracao,
        dsctargetcnf    as target_configuracao,
        dscrolecnf      as role_configuracao,
        dscperfcnf      as perfil_configuracao,
        dscobs          as observacao,
        indativo        as flag_ativo,
        numorder        as ordem
    from {{ source('brutos_acolherio_staging', 'sys_config') }}

)

select * from source

    