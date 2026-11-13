select p.usuario.id_usuario
from {{ ref('prontuario_eletronico') }} as p
left join {{ ref('raw_usuarios') }} as u on p.usuario.id_usuario = cast(u.id_paciente as string)
left join {{ ref('raw_usuarios_detalhes') }} as d on p.usuario.id_usuario = cast(d.id_paciente as string)
left join {{ ref('raw_usuarios_informacoes_medicas') }} as m on p.usuario.id_usuario = cast(m.id_usuario as string)
where
    p.usuario.nis is distinct from coalesce(cast(u.nis_cadastro as string), '')
    or p.usuario.possui_rg is distinct from if(nullif(trim(cast(u.numero_rg_cadastro as string)), '') is not null, 'Sim', 'Não')
    or p.usuario.possui_certidao_nascimento is distinct from if(nullif(trim(cast(d.matricula_certidao_nascimento as string)), '') is not null, 'Sim', 'Não')
    or p.centro_pop.uso_substancias_psicoativas is distinct from case upper(trim(cast(m.flag_uso_substancias_psicoativas as string))) when 'S' then 'Sim' when 'N' then 'Não' else '' end
    or p.centro_pop.possui_problema_saude is distinct from case upper(trim(cast(m.flag_possui_comorbidade as string))) when 'S' then 'Sim' when 'N' then 'Não' else '' end
