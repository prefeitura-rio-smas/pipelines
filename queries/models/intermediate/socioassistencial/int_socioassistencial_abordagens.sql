-- Ponte temporária para as transformações ArcGIS existentes; nenhuma nova
-- resolução de identidade. O cluster é anual e NÃO é uma chave entre sistemas.
-- is_duplicado indica outra ocorrência da pessoa, não uma abordagem inválida.
with fichas_univocas as (
    select
        globalid,
        uniquerowid,
        excluir_ficha,
        data_exclusao,
        exclusao_unidade_calculo,
        exclusao_unidade_cas
    from {{ ref('abordagem_ficha') }}
    where nullif(trim(uniquerowid), '') is not null
    -- Não escolher arbitrariamente controles de fichas com chave ambígua.
    qualify count(*) over (partition by lower(trim(uniquerowid))) = 1
),

unidades_por_nome as (
    select
        {{ clean_name('nome_unidade') }} as nome_unidade,
        if(count(distinct id_unidade) = 1, min(id_unidade), null) as id_unidade
    from {{ ref('raw_unidades') }}
    group by {{ clean_name('nome_unidade') }}
)

select
    'ABORDAGEM' as tipo_registro,
    'arcgis:abordagem_repeat' as origem_registro,
    a.globalid as id_registro_origem,
    cast(null as int64) as id_usuario,
    cast(null as int64) as id_familia,
    u.id_unidade,
    coalesce(a.data_abordagem, meta.data_abordagem_epoch) as data_evento,
    {{ inicio_mes('coalesce(a.data_abordagem, meta.data_abordagem_epoch)') }} as competencia,
    coalesce(a.data_abordagem, meta.data_abordagem_epoch) as data_referencia,
    struct(
        a.globalid,
        a.objectid,
        a.parentrowid,
        meta.data_abordagem_original,
        a.data_abordagem as data_abordagem_parser_legado,
        a.data_abordagem is null and meta.data_abordagem_epoch is not null as flag_data_recuperada,
        meta.data_criacao_original,
        meta.data_edicao_original,
        meta.timestamp_captura,
        ficha.excluir_ficha,
        ficha.data_exclusao,
        ficha.exclusao_unidade_calculo,
        ficha.exclusao_unidade_cas,
        ficha.globalid as id_ficha,
        ficha.uniquerowid as chave_ficha,
        ficha.globalid is not null as flag_ficha_localizada,
        c.cluster_id as id_pessoa_cluster_anual,
        a.ano_num_data_abordagem as ano_cluster,
        c.is_duplicado as flag_outra_ocorrencia_pessoa,
        a.repeat_nome_usuario as nome_pessoa,
        a.repeat_nome_social as nome_social,
        a.repeat_data_nascimento as data_nascimento,
        a.repeat_idade as idade_informada,
        a.repeat_sexo as sexo,
        a.repeat_raca_cor_etnia_tratada as raca_cor,
        a.repeat_unidade_calculo_tratada as unidade_informada,
        a.repeat_unidade_cas as cas_informada,
        a.resp_abordagem as responsavel_abordagem,
        a.turno_abordagem,
        a.flag_trabalho_infantil,
        a.flag_exploracao_sexual,
        a.flag_uso_drogas_ilicitas,
        a.flag_alcoolismo,
        a.flag_migrante,
        a.flag_imigrante,
        a.flag_refugiado,
        a.e_migrante,
        a.qual_substancia_psicoativa,
        a.principal_motivo_permanencia,
        a.permanencia_rua_tratada,
        a.tempo_permanencia_tratada,
        a.encaminhamento_rede_tratada,
        -- Preserva toda ocorrência; elegibilidade oficial depende de validar
        -- o repeat como contato realizado e os controles de exclusão da ficha.
        cast(null as bool) as flag_contabilizavel_rma
    ) as abordagem
from {{ ref('abordagem_repeat') }} as a
left join {{ ref('abordagem_repeat_dedup') }} as c on a.globalid = c.globalid
left join unidades_por_nome as u on {{ clean_name('a.repeat_unidade_calculo_tratada') }} = u.nome_unidade
left join {{ ref('raw_abordagens_metadados') }} as meta on a.globalid = meta.globalid
left join fichas_univocas as ficha on lower(trim(a.parentrowid)) = lower(trim(ficha.uniquerowid))
