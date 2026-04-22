-- Tabela responsável por trazer todos as famílias em acompanhamento PAIF e seus repesctivos membros, assim como suas informações e as unidades que foram responsável por inseri-los no acompanhamento PAIF.
-- Não contem unidades ou usuários testes

with paif_unidade as (
    select
        a.seqfamil,
        a.seqpac,
        a.nome_usuario,
        a.viol_direito,
        a.beneficio,
        a.seqvulnerab,
        c.flag_familia_menor_idade_serv_acolhimento,
        a.data_nascimento,
        a.dia_nascimento,
        a.mes_nascimento,
        a.idade,
        a.sexo,
        a.racacor,
        a.data_cadastro_paif,
        a.dia_cadastro_paif,
        a.mes_cadastro_paif,
        a.ano_cadastro_paif,
        a.seqlogincad,
        b.login_operador,
        b.operador,
        b.sequs
    from {{ ref('stg_famil_paif_info_membros') }} a
    inner join {{ ref('stg_contas_por_unidades') }} b on a.seqlogincad = b.seqlogin
    left join {{ ref('stg_pacientes_sm') }} c on a.seqpac = c.seqpac
)

select * from paif_unidade