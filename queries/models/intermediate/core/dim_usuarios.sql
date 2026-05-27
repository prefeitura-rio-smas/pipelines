with base as (
    select * from {{ ref('raw_usuarios') }}
),
detalhes as (
    select * from {{ ref('raw_usuarios_detalhes') }}
),
saude_mental as (
    select * from {{ ref('raw_usuarios_saude_mental') }}
),
violacoes as (
    select * from {{ ref('int_usuarios_violacoes') }}
),
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['base.id_paciente']) }} as id_usuario_sk,
        base.id_paciente as id_usuario,
        base.nome,
        base.cpf,
        base.data_nascimento,
        base.sexo,
        base.raca_cor,
        base.bairro,
        det.flag_cadunico,
        det.violacao_direito as violacao_direito_bruto,
        sm.flag_deficiencia,
        sm.tipo_deficiencia,
        sm.flag_situacao_rua,
        -- Enriquecimento com Structs e Flags
        if(v.id_usuario is not null, 'Sim', 'Não') as flag_possui_violacao_direito,
        v.violacoes
    from base
    left join detalhes det on base.id_paciente = det.id_paciente
    left join saude_mental sm on base.id_paciente = sm.id_paciente
    left join violacoes v on base.id_paciente = v.id_usuario
    where base.nome not like '%TESTE%'
)

select * from final
