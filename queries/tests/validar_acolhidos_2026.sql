-- Teste: valida total de acolhidos únicos em 2026
-- Compara a contagem da mart_acolhimento_diaria com a fonte direta (gh_pac_ciclos)
-- A mart é o modelo correto pra essa validação pois o grão ciclo (fct_acolhimentos)
-- não permite filtrar por período sem lógica de sobreposição de intervalos.

with mart as (
    select distinct id_usuario
    from {{ ref('mart_acolhimento_diaria') }}
    where ano = 2026
),

fonte as (
    select distinct seqpac as id_usuario
    from {{ source('brutos_acolherio_staging', 'gh_pac_ciclos') }}
    where dtentrada <= '2026-12-31'
      and (dtsaida is null or dtsaida >= '2026-01-01')
),

na_mart_nao_na_fonte as (
    select 'na mart mas nao na fonte' as tipo, id_usuario from mart
    except distinct
    select 'na mart mas nao na fonte', id_usuario from fonte
),

na_fonte_nao_na_mart as (
    select 'na fonte mas nao na mart' as tipo, id_usuario from fonte
    except distinct
    select 'na fonte mas nao na mart', id_usuario from mart
),

divergencias as (
    select * from na_mart_nao_na_fonte
    union all
    select * from na_fonte_nao_na_mart
)

select
    tipo,
    count(*) as total
from divergencias
group by tipo
