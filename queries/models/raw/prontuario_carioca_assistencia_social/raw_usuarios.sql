-- Camada Raw: Dados cadastrais dos pacientes
with source as (
    select
        seqpac as id_paciente,
        seqlogin as id_login_cadastro,
        sigufnasc as uf_nascimento,
        dscnomepac as nome,
        dscnomsoci as nome_social,
        dscnmmae as nome_mae,
        {{ map_coluna_estado_civil('estcivil') }} as estado_civil,
        datnascim as data_nascimento,
        nacional as nacionalidade,
        condestr as condicao_estrangeira,
        paisorigem as pais_origem,
        dscbairroender as bairro,
        racacor as raca_cor, 
        numcpfpac as cpf,
        indsexo as sexo,
        indgenero as genero,
        nuprontpapel as prontuario_papel,
        datcadast as data_cadastro
    from  {{ source('brutos_acolherio_staging', 'gh_cidadao_pac') }}
)
select * from source
