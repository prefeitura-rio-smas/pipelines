-- Tabela para tratar as colunas em que precisa de macro. 

with tratar_raca_e_genero_cidadao_pac as (
    select
        seqpac,
        seqlogincad,
        nome_usuario,
        nome_social,
        filiacao_mae,
        estado_civil,
        data_nascimento,
        {{map_coluna_nacionalidade('nacionalidade')}} as nacionalidade,
        {{map_coluna_cond_estrangeiro('condicao_estrangeira')}} as condicao_estrangeira,
        pais_origem,
        bairro,
        {{ map_coluna_raca('raca') }} as raca,
        cpf,
        sexo,
        {{ map_coluna_genero('genero') }} as genero,
        prontuario,
        data_cadastro_usuario
    from {{ ref('stg_cidadao_pac_acolherio') }}
),

-- Cte para pegar os dados dos países 
cte_paises as (
    select 
        upper(cod_paises_alpha2) as abreviacao_paises,
        nome_paises
    from {{ ref('countries') }}
),

-- CTE com join seed país
tratar_raca_e_genero_cidadao_pac_final as (
    select
        a.seqpac,
        a.seqlogincad,
        a.nome_usuario,
        a.nome_social,
        a.filiacao_mae,
        a.estado_civil,
        a.data_nascimento,
        a.nacionalidade,
        a.condicao_estrangeira,
        b.nome_paises as pais_origem_descricao,
        a.bairro,
        a.raca,
        a.cpf,
        a.sexo,
        a.genero,
        a.prontuario,
        a.data_cadastro_usuario
    from tratar_raca_e_genero_cidadao_pac a
    left join cte_paises b on a.pais_origem = b.abreviacao_paises
),

tratar_escolaridade_serie_paciente_sm as(
    select 
        seqpac,
        codorigem,
        flag_trabalho,
        profissao,
        flag_frequencia_escola,
        {{ map_coluna_serie_escola('serie_escola') }} as serie_escola,
        {{ map_coluna_escolaridade('escolaridade') }} as escolaridade,
        flag_recebe_beneficio,
        tipo_beneficio,
        tipo_curatela,
        flag_curatela,
        {{ map_coluna_situacao_de_rua('flag_situacao_rua') }} as flag_situacao_rua,
        flag_deficiencia,
        tipo_deficiencia
    from  {{ ref ('stg_pacientes_sm_acolherio') }}
),


tratar_saude_mental_orientacao_sexual_vinculo_trabalhista as (
    select
        seqpac,
        flag_cadunico,
        numero_processo_decisao_apoiada,
        nome_apoiador,
        case
            when renda_ativa != 0 then "Sim"
            else "Não"
        end as flag_atvd_gera_renda,
        {{ map_coluna_saude_mental('saude_mental_comprometida') }}as saude_mental_comprometida,
        {{ map_coluna_tipo_motiv_acolhimento('motivo_acolhimento') }} as motivo_acolhimento,
        violacao_direito,
        pontuacao,
        {{ map_grau_dependencia('grau_dependencia') }} grau_dependencia,
        {{map_coluna_orientacao_sexual('orientacao_sexual')}} as orientacao_sexual,
        {{ map_coluna_vinculo_trabalhista('vinculo_trabalhista') }} as vinculo_trabalhista
    from {{ ref('stg_pac_dados_acolherio') }}
)

select
    a.seqpac,
    a.prontuario,
    a.pais_origem_descricao,
    a.nacionalidade,
    a.condicao_estrangeira,
    a.nome_usuario,
    a.nome_social,
    a.data_nascimento,
    a.bairro,
    a.cpf,
    a.sexo,
    c.orientacao_sexual,
    a.genero,
    a.raca,
    a.estado_civil,
    a.filiacao_mae,
    b.flag_trabalho,
    c.vinculo_trabalhista,
    b.profissao,
    c.flag_atvd_gera_renda,
    b.flag_frequencia_escola,
    b.serie_escola,
    b.escolaridade,
    b.flag_recebe_beneficio,
    b.tipo_beneficio,
    b.flag_deficiencia,
    b.tipo_deficiencia,
    c.saude_mental_comprometida,
    c.violacao_direito,
    b.flag_curatela,
    b.tipo_curatela,
    c.numero_processo_decisao_apoiada,
    c.nome_apoiador,
    b.flag_situacao_rua,
    c.flag_cadunico,
    c.grau_dependencia,
    c.pontuacao,
    c.motivo_acolhimento,
    b.codorigem,
    a.seqlogincad,
    a.data_cadastro_usuario
from tratar_raca_e_genero_cidadao_pac_final a
inner join tratar_escolaridade_serie_paciente_sm b on a.seqpac = b.seqpac
inner join tratar_saude_mental_orientacao_sexual_vinculo_trabalhista c on a.seqpac = c.seqpac

