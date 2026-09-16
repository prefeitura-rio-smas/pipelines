-- Mart: status do Cartão PIC (produto único empilhado)
-- Grão: 1 linha por (data_particao, cod_familiar_fam).
--
-- Consome int_status (atributos estáticos por folha) e enriquece com os
-- ATRIBUTOS DINÂMICOS: Survey (entregas), Controle CAS (CRAS/doc) e Eventos
-- (previsão de entrega + disparos WhatsApp).
--
-- Incremental insert_overwrite particionado por data_particao: a cada execução
-- (1h) sobrescreve apenas as partições >= última materializada (a atual é
-- reprocessada; partições novas entram sozinhas). As antigas ficam intactas.
--
-- Recorte de eventos (join com eventos_com_faixa) = alerta IN (retirar_*):
-- quem vai ao evento físico retirar cartão. Reconcessão (aviso) e mantidos
-- ficam sem evento.

{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'data_particao', 'data_type': 'date'},
    alias='status',
    tags=['cartao_pic_status'],
) }}

with

-- Base estática (partições históricas + folha nova com classificação)
base_status as (
    select * from {{ ref('int_status') }}
),

-- Controle CAS: enriquece com CRAS, doc_verificada, justificativa e obs
controle_cas as (
    select
        c.cpf_sem_formatacao,
        c.cras,
        c.doc_verificada,
        c.categoria_justificativa,
        c.obs,
        c.data_particao,
        u.endereco_completo
    from {{ ref('raw_controle_cas') }} as c
    left join {{ ref('raw_unidade') }} as u
        on c.cras = trim(upper(regexp_replace(normalize(u.nome_popular, nfd), r'\pM', '')))
),

-- Eventos: capacidade acumulada por bairro (quebras de bairro)
eventos_com_faixa as (
    select
        * except (bairro),
        upper(trim(bairro)) as bairro,
        concat(bairro, '_', cast(data_entrega as string), '_', hora_entrega) as id_evento_slot,
        coalesce(sum(quantidade) over (
            partition by bairro
            order by data_entrega, hora_entrega
        ), 999999) as rank_maximo_acumulado,
        coalesce(sum(quantidade) over (
            partition by bairro
            order by data_entrega, hora_entrega
            rows between unbounded preceding and 1 preceding
        ), 0) as cap_anterior
    from {{ ref('raw_bairros_entrega') }}
),

-- Beneficiário enriquecido: rank alfabético por bairro (distribuição nos slots)
beneficiario_enriquecido as (
    select
        s.*,
        c.cras as cras_previsto,
        c.doc_verificada,
        c.categoria_justificativa,
        c.obs,
        c.endereco_completo as endereco_cras_previsto,
        row_number() over (
            partition by s.bairro
            order by s.envelope asc
        ) as rank_alfabetico
    from base_status as s
    left join controle_cas as c
        on
            s.num_cpf_responsavel = c.cpf_sem_formatacao
            and s.data_particao = c.data_particao
),

-- Survey: entregas reais (dedup por CPF)
survey as (
    select * from {{ ref('raw_survey_primeira_infancia') }}
),

-- Junção final: base + eventos (recorte alerta IN retirar_*) + survey
final as (
    select
        s.data_particao,
        s.cod_familiar_fam,
        s.num_cpf_responsavel,
        s.cpf_formatado,
        s.envelope,
        s.nome_responsavel,
        e.nome_social as nome_social_responsavel,
        s.idade_responsavel,
        s.faixa_etaria_responsavel,
        s.cod_sexo_responsavel,
        s.qtd_criancas_0_a_4_anos,
        s.cas,
        s.bairro,
        s.nom_unidade_territorial_fam,
        s.endereco_fam,
        s.des_complemento_fam,
        s.des_complemento_adic_fam,
        s.txt_referencia_local_fam,
        s.num_tel_contato_1_fam,
        s.num_tel_contato_2_fam,
        s.nom_centro_assist_fam,
        b.tipo_entrega as tipo_entrega_prevista,
        b.local_entrega as local_entrega_previsto,
        b.endereco_entrega as endereco_entrega_previsto,
        b.data_entrega as data_entrega_prevista,
        b.hora_entrega as hora_entrega_prevista,
        s.cras_previsto,
        s.endereco_cras_previsto,
        b.data_retirada_cras as data_retirada_cras_prevista,
        b.hora_retirada_cras as hora_retirada_cras_prevista,
        e.local_entrega as tipo_entrega,
        e.responsavel_retirada as responsavel_pela_retirada,
        e.nome_procurador,
        e.cpf_procurador,
        e.profissional as profissional_entrega,
        e.equipamento_referencia as profissional_entrega_equipamento,
        e.profissional_digit as profissional_digitacao,
        e.equipamento_referencia_digit as profissional_digitacao_equipamento,
        e.last_edited_user as survey_ultima_edicao_conta,
        s.doc_verificada,
        s.categoria_justificativa,
        s.obs,
        s.flag_folha_anterior,
        s.flag_familia_nao_encontrada,
        s.flag_mudanca_rf,
        s.flag_vr_correcao,
        s.status,
        s.flag_recebe_envelope,
        s.situacao,
        s.mudou,
        s.cod_familiar_fam_origem,
        s.flag_ausencia_doc_civil,
        s.flag_reconcessao,
        s.acao_smas,
        s.alerta,
        s.filtro_email_cas,
        s.cohort,
        s.cohort_entrada,
        s.qtd_folhas,
        s.ativo_inativo_familia,
        s.motivo_inativo_familia,
        s.ativo_inativo_rf,
        s.motivo_inativo_rf,
        date(e.data_entrega, 'America/Sao_Paulo') as data_entrega,
        case
            when e.local_entrega_outros is null or e.local_entrega_outros = '' then e.local_entrega_cras
            else e.local_entrega_outros
        end as local_entrega,
        date(e.created_date, 'America/Sao_Paulo') as survey_data_criacao,
        date(e.last_edited_date, 'America/Sao_Paulo') as survey_ultima_edicao_data,
        date(safe_cast(e.timestamp_captura as timestamp), 'America/Sao_Paulo') as survey_data_extracao,
        -- Status de entrega do cartão (dinâmico por hora)
        case
            when date(e.data_entrega, 'America/Sao_Paulo') is not null then 'retirado'
            when b.tipo_entrega = 'CRAS' and (b.data_entrega is null or b.data_entrega >= current_date()) then 'não retirado'
            when (current_date() <= b.data_entrega or b.data_entrega is null) then 'aguardando evento'
            when current_date() > b.data_entrega then 'não retirado'
            else 'verificar'
        end as status_entrega,
        case
            when date(e.data_entrega, 'America/Sao_Paulo') is null then false
            when date(e.data_entrega, 'America/Sao_Paulo') is not null then true
        end as flag_entrega,
        -- Data de atualização: só a partição ATUAL recebe CURRENT_DATE (o snapshot
        -- diário versiona apenas ela); as antigas ficam com data fixa (não versionam).
        case
            when s.data_particao = (select max(bs.data_particao) from base_status as bs)
                then current_date('America/Sao_Paulo')
            else s.data_particao
        end as data_atualizacao,
        format_datetime('%H:%M:%S', current_datetime('America/Sao_Paulo')) as hora_atualizacao
    from beneficiario_enriquecido as s
    left join eventos_com_faixa as b
        on
            s.bairro = b.bairro
            and s.alerta in ('retirar_cartao', 'retirar_cartao_apos_atualizacao', 'retirar_cartao_regularizacao_documento')
            and s.rank_alfabetico > b.cap_anterior
            and s.rank_alfabetico <= b.rank_maximo_acumulado
    left join survey as e
        on s.num_cpf_responsavel = e.cpf_sem_formatacao
    qualify row_number() over (
        partition by s.data_particao, s.cod_familiar_fam
        order by b.data_entrega, b.hora_entrega
    ) = 1
)

select * from final as f

{% if is_incremental() %}
    where f.data_particao >= (select max(t.data_particao) from {{ this }} as t)
{% endif %}
