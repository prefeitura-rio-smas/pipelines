-- Camada Raw: pessoa_deficiencia
with source as (
    select
        id_prefeitura,
        id_tem_deficiencia,
        tem_deficiencia,
        id_familia,
        ajuda_especializada,
        ajuda_familia,
        ajuda_instituicao_social,
        nao_recebe_ajuda,
        ajuda_terceiros,
        ajuda_vizinhos,
        deficiencia_baixa_visao,
        deficiencia_cegueira,
        deficiencia_fisica,
        deficiencia_mental,
        deficiencia_sindrome_down,
        deficiencia_surdez_leve,
        deficiencia_surdez_profunda,
        deficiencia_transtorno_mental,
        id_membro_familia as id_membro,
        numero_registro_arquivo as numero_registro,
        id_versao_layout_arquivo,
        data_particao
    from {{ source('cadunico', 'pessoa_deficiencia') }}
)

select * from source
