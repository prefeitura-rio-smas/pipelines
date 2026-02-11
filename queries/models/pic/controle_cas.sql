WITH controle_cas_base AS (

    SELECT 
      * EXCEPT(cartao_entregue, data_entrega_text, local_entrega, resp_retirada),
      NULLIF(data_entrega_text, 'None') as data_entrega_text,
      NULLIF(local_entrega, 'None') as local_entrega,
      NULLIF(resp_retirada, 'None') as resp_retirada,
      CASE
        WHEN NULLIF(data_entrega_text, 'None') IS NOT NULL THEN 'CARTAO ENTREGUE'
      END AS cartao_entregue 
    FROM {{ source('arcgis_raw', 'controle_cas_raw') }}

),

primeira_infancia AS (

    SELECT 
      * EXCEPT(cod_atend, local_entrega_cartao, data_entrega, responsavel_retirada, arquivar_registro),
      objectid as cod_atend,
      NULLIF(data_entrega, 'None') as data_entrega,
      NULLIF(responsavel_retirada, 'None') as responsavel_retirada,
      NULLIF(arquivar_registro, 'None') as arquivar_registro,
      case
        when NULLIF(local_entrega_cras, 'None') IS NULL OR local_entrega_cras = "" then local_entrega_outros  
        else local_entrega_cras 
      end as local_entrega_cartao
    FROM {{ source('arcgis_raw', 'primeira_infancia_carioca_raw') }}
    where NULLIF(arquivar_registro, 'None') is null
    qualify row_number () over (partition by cpf order by last_edited_date desc) = 1


),

cc as (
SELECT
  cc.objectid,
  cc.cpf,
  cc.nome_rf,
  cc.data_nascimento,
  cc.nome_mae,
  cc.cas,
  cc.cras,
  cc.bairro,
  cc.unidade_territorial,
  cc.endereco,
  cc.complemento_ref,
  cc.cep,
  cc.telefone,
  cc.tipo_evento,
  cc.local_entrega_previsto,
  cc.data_entrega_prevista,
  cc.data_entrega,
  pi.local_entrega_cartao AS local_entrega,
  cc.envelope,
  cc.num_cartao_vr,
  cc.nome_cartao_vr,
  cc.cartao_entregue,
  cc.doc_verificada,
  cc.resp_verificacao,
  cc.created_user,
  cc.created_date,
  cc.last_edited_user,
  cc.last_edited_date,
  cc.data_entrega_prevista_2,
  cc.cpf_resp_verific,
  cc.obs,
  pi.data_entrega AS data_entrega_text,
  cc.cras_3,
  pi.responsavel_retirada AS resp_retirada,
  cc.telefone_formatado,
  cc.categoria_justificativa

FROM controle_cas_base cc
LEFT JOIN primeira_infancia pi
  ON cc.cpf = pi.cpf
)

select * from cc