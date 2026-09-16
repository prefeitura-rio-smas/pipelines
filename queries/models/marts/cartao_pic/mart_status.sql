-- Mart: status do Cartão PIC (produto único empilhado)
-- Grão: 1 linha por (data_particao, cod_familiar_fam).
--
-- Apresentação final sobre a int_status (que já reúne folha + Controle CAS +
-- Survey + Eventos): aplica os cases de entrega e o carimbo de atualização.
--
-- Incremental insert_overwrite particionado por data_particao: a cada execução
-- (1h) sobrescreve apenas as partições >= última materializada (a atual é
-- reprocessada; partições novas entram sozinhas). As antigas ficam intactas.

{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'data_particao', 'data_type': 'date'},
    alias='status',
    tags=['cartao_pic_status'],
) }}

select
    s.*,
    -- Status de entrega do cartão (dinâmico por hora)
    case
        when s.data_entrega is not null then 'retirado'
        when s.tipo_entrega_prevista = 'CRAS' and (s.data_entrega_prevista is null or s.data_entrega_prevista >= current_date()) then 'não retirado'
        when (current_date() <= s.data_entrega_prevista or s.data_entrega_prevista is null) then 'aguardando evento'
        when current_date() > s.data_entrega_prevista then 'não retirado'
        else 'verificar'
    end as status_entrega,
    case
        when s.data_entrega is null then false
        when s.data_entrega is not null then true
    end as flag_entrega,
    -- Data de atualização: só a partição ATUAL recebe CURRENT_DATE (o snapshot
    -- diário versiona apenas ela); as antigas ficam com data fixa (não versionam).
    case
        when s.data_particao = (select max(bs.data_particao) from {{ ref('int_status') }} as bs)
            then current_date('America/Sao_Paulo')
        else s.data_particao
    end as data_atualizacao,
    format_datetime('%H:%M:%S', current_datetime('America/Sao_Paulo')) as hora_atualizacao
from {{ ref('int_status') }} as s

{% if is_incremental() %}
    where s.data_particao >= (select max(t.data_particao) from {{ this }} as t)
{% endif %}
