/*
 Este teste verifica se todos os atendimentos do módulo usuário 
 da camada staging estão na tabela int_atendimentos da camada 
 intermediate.

 Importante ressaltar que a int_atendimentos possui todos os atendimentos.
 Ela possui atendimentos de todos os módulos (Usuário e Família)
*/ 
/*
    Cte retorna todos os atendimentos módulo
    de int_atendimentos, caso eles não existam na
    stg_atendimentos_usuarios, o seqatend_modulo fica null. 
*/
with tabela_base as (
select
    a.seqatend_modulo,
    a.modulo
from {{ ref('int_atendimentos') }} a
left join {{ ref('stg_atendimentos_usuarios') }} b
on a.seqatend_modulo = b.seqatend_modulo
)

/*
    Essa select retorna todos os casos em que o seqatend_modulo
    é null e possui módulo usuário.

    OBS: O teste unitário do dbt funciona retornando os casos
    problemáticos, tendo em vista isso, eu formulei esse teste
    para retorna os casos problemáticos. Se o teste não retornar
    nada é por que a regra de negócio está correta.
*/
select
    modulo
from tabela_base
where seqatend_modulo is null
and modulo = 'u'