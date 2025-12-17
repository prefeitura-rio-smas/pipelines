with contas_mais_de_uma_unidade as (
select 
    seqlogin,
    sequs,
    datacesso
from {{ source('cras_rma_prod', 'gh_contas_us')}}
where datacesso is not null
),

total_login_ordenador_mais_recente as (
select 
    seqlogin,
    sequs,
    datacesso,
    row_number() over (
        partition by seqlogin
        order by datacesso desc
    ) as ultimo_acesso_operador
from contas_mais_de_uma_unidade
order by seqlogin asc
)

select
    seqlogin,
    sequs,
    datacesso
from total_login_ordenador_mais_recente
where ultimo_acesso_operador = 1