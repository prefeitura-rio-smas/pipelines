/*
    Tabela responsável por unir o SEQPROF com o CODCBO.

    Esta tabela contém dados repetidos. Um mesmo profissional pode ter
    mais de uma função (CBO).

    gh_prof_ocup tem mais dados. 1555. são 33 dados excedentes por repetição.
    gh_prof tem 1525 e 3 desses não aparecem no gh_prof_ocup
    gh_cbo tem 41 e é a tabela onde os dados são normalizados. são 41 cbos diferentes.
*/

with funcao_profissional as (
    select
        codcbo,
        seqprof
    from {{ source('source_dashboard_acolherio', 'gh_profocup') }}
)

select * from funcao_profissional

