-- Evoluções familiares só pertencem ao paciente explicitamente indicado.
-- Não converte id_familia em id_usuario nem distribui registros aos membros.
select distinct
    e.id_evolucao,
    e.origem_modulo,
    e.id_familia,
    e.id_unidade,
    un.nome_unidade,
    {{ pu_tipo_servico('un.tipo_unidade') }} as tipo_servico,
    e.id_profissional,
    e.codigo_abrangencia,
    e.id_atividade,
    e.data_evolucao,
    e.data_cancelamento,
    e.tipo_evolucao,
    e.descricao_evolucao,
    a.nome_atividade,
    case
        when e.origem_modulo = 'administrativa' then o.nome_operador
        else p.nome
    end as nome_profissional,
    coalesce(e.id_usuario, e.id_paciente_familia) as id_usuario,
    concat(e.origem_modulo, ':', cast(e.id_evolucao as string)) as id_evolucao_modulo
from {{ pu_ref('int_evolucoes') }} as e
left join {{ pu_ref('dim_profissionais') }} as p
    on e.origem_modulo != 'administrativa' and e.id_profissional = p.id_profissional
left join {{ pu_ref('raw_operadores') }} as o
    on e.origem_modulo = 'administrativa' and e.id_profissional = o.id_login
left join {{ pu_ref('raw_atividades_grupo') }} as a on e.id_atividade = a.id_atividade
left join {{ pu_ref('dim_unidades') }} as un on e.id_unidade = un.id_unidade
where
    coalesce(e.id_usuario, e.id_paciente_familia) is not null
    and e.data_cancelamento is null
