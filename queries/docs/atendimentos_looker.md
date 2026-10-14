# Atendimentos no Looker

`mart_atendimentos` gera `rj-smas.marts.atendimentos` (`rj-smas-dev.relatorio.atendimentos` em dev).
São 83 colunas e uma linha por `ID_ATENDIMENTO_MODULO`, sem filtros de cancelamento, TESTE ou cadastro ausente.

## Contagem e filtros

- `COUNT(*)` conta atendimentos; `COUNT(DISTINCT ID_USUARIO)` conta pessoas.
- `ID_ATENDIMENTO` é igual a `ID_ATENDIMENTO_MODULO`; `ID_ATENDIMENTO_SK` é seu hash.
- Cancelamento, TESTE e ausência de cadastro são expostos em `FLAG_*` para filtros no Looker.
- `atendimento_unico_rank` deduplica por usuário, unidade, data, hora e nome original. O rank é calculado antes dos filtros do Looker; `cbo_unico_rank` é sempre 1.
- Ao abrir arrays, contar atendimentos distintos para evitar multiplicação por profissional, vínculo ou item social.

## Profissionais e família

`PROFISSIONAIS` contém principal e compartilhados. Os campos individuais representam o principal, inclusive quando ausente.
Compartilhados inválidos, zero, repetidos ou iguais ao principal são descartados; a posição principal ausente é preservada.
A classificação respeita Recepção explícita; nos demais casos, considera todos os CBOs e prioriza Técnico, depois recepção com CadÚnico, depois Outros.
O atendimento consolida Técnico se qualquer profissional for técnico; senão Recepção se houver profissional classificado como Recepção; senão Outros.

Serviços e projetos usam a família do atendimento familiar ou o vínculo priorizado do usuário: aberto primeiro, depois saída mais recente.
`VINCULOS_FAMILIARES` preserva todos os vínculos. Essa seleção não reconstrói a família na data do atendimento.
Os e-mails da planilha são agregados por nome de unidade e combinados com os da unidade/CAS, mesmo quando algum componente falta.

## Perfil social atual

| Tema | Origem e saída |
| --- | --- |
| Projetos | Cadastro da família: resumo dos ativos/não cancelados e histórico completo. Não garante cobertura de programas externos. |
| Serviços | Cadastro da família: resumo dos não cancelados e histórico completo. PAIF (1) e PAEFI (6) têm nomes validados; demais IDs permanecem visíveis como não mapeados. |
| Violações | Cadastro e evoluções do usuário; detalhes preservam código, descrição e origem. Qualquer item identificado prevalece sobre N cadastral. |
| Benefícios e deficiências | Indicadores explícitos e listas cadastrais; itens repetidos, vazios e N são removidos, códigos sem tradução são preservados. |
| Situação de rua | indmoradi=5: Sim; outras moradias conhecidas: Não; E, ausência ou desconhecido: Não Informado. |
| Comorbidades | gh_pac_info_med: indcomorb, dsccomorb, datcomorbinfo e dsclocaltratcomorb. Texto livre, data/local de tratamento e registro original nos detalhes. |

Os indicadores distinguem `Sim`, `Não` e `Não Informado`; ausência não é negativa.
Benefícios, deficiência e comorbidades não são inferidos da descrição: divergências entre indicador e texto são preservadas.
Os resumos em texto servem para exibição; filtros por item devem usar os códigos dos arrays.
O perfil é cadastral atual, incluindo evoluções posteriores ao atendimento, sem inferência clínica ou reconstrução histórica.
A data de informação de comorbidades não indica o início da condição.

A tabela legada `dev_atendimentos` e o contrato do fato legado são preservados.
Testes verificam chaves, cobertura de atendimentos/profissionais, serviços, comorbidades e os casos de ausência/negação.
