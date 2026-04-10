-- Tabela responsável por retornar parte dos dados relacionados ao usuário
-- Contém usuários testes
-- Não contém usuários repetidos

with dados_pac as (
    select
        seqpac,
        {{ map_flag_cadunico('indcadunico') }} as flag_cadunico,
        datcadunico as data_cadunico,
        indgestante as flag_gestante,
        dsctomdecproces as numero_processo_decisao_apoiada,
        dsctomdecnome as nome_apoiador,
        indsmentcompr as saude_mental_comprometida,
        indmotivacol,
        indvioldir as violacao_direito,
        valpontos as pontuacao,
        indgraudepend as grau_dependencia,
        indorientsex as orientacao_sexual,
        valrendaativ as renda_ativa,
        valrendabenef as renda_beneficio,
        indtipovinc  as vinculo_trabalhista
    from  {{ source('source_dashboard_acolherio', 'gh_pac_dados') }}
)

select * from dados_pac