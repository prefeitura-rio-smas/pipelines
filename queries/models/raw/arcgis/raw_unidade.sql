-- Camada Raw: Unidades da rede socioassistencial (CRAS/CAS)
-- Fonte: ArcGIS (equipamento_raw)
-- Colunas principais: nome_popular, endereco_completo, cas
with source as (
    select
        codigo_sic as codigo_unidade,
        nome_equip as nome_oficial,
        nome_equ_1 as nome_popular,
        tipoequipa as categoria,
        tipo_gesta as tipo_gestao,
        hierarquia as tipo_hierarquia,
        num_vagas_ as quantidade_vagas,
        servicos_o as servicos_oferecidos,
        bairros_at as bairro_atuacao,
        equip_ativ as equipe_ativa,
        telefone1_ as telefone_equipe_1,
        telefone2_ as telefone_equipe_2,
        telefone3_ as telefone_equipe_3,
        email_equi as email,
        nome_gesto as nome_gestor,
        celular_eq as celular,
        cep as cep,
        endereco_c as endereco_completo,
        ponto_refe as ponto_referencia,
        longitude as longitude_unidade,
        latitude as latitude_unidade,
        bairro as bairro,
        codbairro as codigo_bairro,
        ra as regiao_admnistrativa,
        codra as codigo_ra,
        rp as regiao_planejamento,
        codrp as codigo_rp,
        ap as area_planejamento,
        cas as cas,
        perfil_fam as perfil_familia,
        perfil_sex as perfil_sexo,
        perfil_fai as perfil_faixa_etaria,
        perfil_f_1 as perfil_crianca,
        perfil_f_2 as perfil_adolescente,
        perfil_f_3 as perfil_adulto,
        perfil_f_4 as perfil_idoso,
        perfil_f_5 as perfil_pcd,
        perfil_gra as perfil_gestante,
        hist_inaug as historico_inauguracao,
        obs_gerais as observacao_geral,
        data_de_pr as data_atualizacao,
        tipo_abrev as tipo_equipamento
    from {{ source('arcgis_raw', 'equipamento_raw') }}
)

select * from source