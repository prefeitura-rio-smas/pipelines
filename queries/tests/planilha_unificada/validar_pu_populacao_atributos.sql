{{ config(tags = ['planilha_unificada']) }}

select coalesce(cast(u.id_usuario as string), p.usuario.id_usuario) as id_usuario
from {{ pu_ref('dim_usuarios') }} as u
full outer join {{ ref('planilha_unificada') }} as p on cast(u.id_usuario as string) = p.usuario.id_usuario
left join {{ pu_ref('dim_unidades') }} as un on u.id_unidade_referencia = un.id_unidade
where
    u.id_usuario is null or p.usuario.id_usuario is null
    or coalesce(cast(u.id_usuario as string), '') is distinct from p.usuario.id_usuario
    or coalesce(cast(u.nome as string), '') is distinct from p.usuario.nome
    or coalesce(cast(u.nome_social as string), '') is distinct from p.usuario.nome_social
    or coalesce(cast(u.filiacao_mae as string), '') is distinct from p.usuario.filiacao_mae
    or coalesce(cast(u.nacionalidade as string), '') is distinct from p.usuario.nacionalidade
    or coalesce(cast(u.condicao_estrangeira as string), '') is distinct from p.usuario.condicao_estrangeira
    or coalesce(cast(u.pais_origem as string), '') is distinct from p.usuario.pais_origem
    or coalesce(cast(u.cpf as string), '') is distinct from p.usuario.cpf
    or coalesce(cast(u.data_nascimento as string), '') is distinct from p.usuario.data_nascimento
    or coalesce(cast(u.sexo as string), '') is distinct from p.usuario.sexo
    or coalesce(cast(u.raca_cor as string), '') is distinct from p.usuario.raca_cor
    or coalesce(cast(u.genero as string), '') is distinct from p.usuario.genero
    or coalesce(cast(u.bairro as string), '') is distinct from p.usuario.bairro
    or coalesce(cast(u.prontuario as string), '') is distinct from p.usuario.prontuario
    or coalesce(cast(u.flag_cadunico as string), '') is distinct from p.usuario.flag_cadunico
    or coalesce(cast(u.pontuacao as string), '') is distinct from p.usuario.pontuacao
    or coalesce(cast(u.grau_dependencia as string), '') is distinct from p.usuario.grau_dependencia
    or coalesce(cast(u.nome_apoiador as string), '') is distinct from p.usuario.nome_apoiador
    or coalesce(cast(u.numero_processo_decisao_apoiada as string), '') is distinct from p.usuario.numero_processo_decisao_apoiada
    or coalesce(cast(u.renda_ativa as string), '') is distinct from p.usuario.renda_ativa
    or coalesce(cast(u.atvd_remunerada as string), '') is distinct from p.usuario.atvd_remunerada
    or coalesce(cast(u.flag_decisao_apoiada as string), '') is distinct from p.usuario.flag_decisao_apoiada
    or coalesce(cast(u.flag_saude_mental_comprometida as string), '') is distinct from p.usuario.flag_saude_mental_comprometida
    or coalesce(cast(u.motivo_acolhimento as string), '') is distinct from p.usuario.motivo_acolhimento
    or coalesce(cast(u.orientacao_sexual as string), '') is distinct from p.usuario.orientacao_sexual
    or coalesce(cast(u.vinculo_trabalhista as string), '') is distinct from p.usuario.vinculo_trabalhista
    or coalesce(cast(u.flag_trabalha as string), '') is distinct from p.usuario.flag_trabalha
    or coalesce(cast(u.profissao as string), '') is distinct from p.usuario.profissao
    or coalesce(cast(u.flag_frequenta_escola as string), '') is distinct from p.usuario.flag_frequenta_escola
    or coalesce(cast(u.escolaridade_indice as string), '') is distinct from p.usuario.escolaridade_indice
    or coalesce(cast(u.flag_recebe_beneficio as string), '') is distinct from p.usuario.flag_recebe_beneficio
    or coalesce(cast(u.flag_curatela as string), '') is distinct from p.usuario.flag_curatela
    or coalesce(cast(u.tipo_curatela as string), '') is distinct from p.usuario.tipo_curatela
    or coalesce(cast(u.flag_deficiencia as string), '') is distinct from p.usuario.flag_deficiencia
    or coalesce(cast(u.flag_situacao_rua as string), '') is distinct from p.usuario.flag_situacao_rua
    or coalesce(cast(u.origem_demanda as string), '') is distinct from p.usuario.origem_demanda
    or coalesce(cast(u.flag_possui_violacao_direito as string), '') is distinct from p.usuario.flag_possui_violacao_direito
    or coalesce(un.nome_unidade, '') is distinct from p.usuario.nome_unidade_referencia
