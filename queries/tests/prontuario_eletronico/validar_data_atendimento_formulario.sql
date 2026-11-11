select
    b.id_usuario,
    b.seq_atendimento,
    b.modulo
from {{ ref('int_atendimentos_bi') }} as b
left join {{ ref('int_datas_formularios_atendimento') }} as d
    on
        b.modulo = d.modulo and b.seq_atendimento = d.id_atendimento
        and b.id_usuario = d.id_usuario and b._id_unidade = d.id_unidade
where
    b.data_de_atendimento is distinct from d.data_atendimento
    or (b._origem_data_atendimento != 'formulario' and b.data_de_atendimento is not null)
union all
select
    id_usuario,
    id_atendimento,
    modulo
from {{ ref('int_datas_formularios_atendimento') }}
group by modulo, id_atendimento, id_usuario, id_unidade
having count(*) != 1
