-- Complementos atuais. Duplicatas idênticas são removidas; divergências
-- permanecem visíveis no teste de unicidade, sem seleção arbitrária.
select distinct
    u.id_paciente as id_usuario,
    struct(
        u.uf_nascimento,
        sm.serie_escolar,
        safe_cast(d.data_cadunico as date) as data_cadunico_informada
    ) as cadastro_complementar
from {{ pu_ref('raw_usuarios') }} as u
left join {{ pu_ref('raw_usuarios_saude_mental') }} as sm on u.id_paciente = sm.id_paciente
left join {{ pu_ref('raw_usuarios_detalhes') }} as d on u.id_paciente = d.id_paciente
