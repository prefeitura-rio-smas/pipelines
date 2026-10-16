# Matriz de cobertura dos RMAs

**Disponível** significa que o fato e seus atributos existem; não significa que
um indicador oficial tenha sido homologado. **Parcial** exige validação ou dispõe
somente de cadastro atual. **Pendente** não possui informação suficiente para a
regra oficial. Não converter desconhecido em zero, Não ou ausência de violação.

Referências primárias consultadas em 16/10/2026:
[orientação do MDS](https://www.gov.br/mds/pt-br/acoes-e-programas/suas/gestao-do-suas/vigilancia-socioassistencial-1/registro-mensal-de-atendimentos-2013-rma),
[manual CRAS](https://aplicacoes.mds.gov.br/sagi/atendimento/doc/Manual_de_Instrucoes_-_CRAS.pdf),
[manual CREAS 2022](https://aplicacoes.mds.gov.br/sagi/atendimento/doc/Manual_RMA_CREAS2022.pdf),
[manual Centro POP 2022](https://aplicacoes.mds.gov.br/sagi/atendimento/doc/Manual_RMA_CENTRO_POP2022.pdf),
[formulário CREAS](https://aplicacoes.mds.gov.br/sagi/atendimento/doc/Formulario_RMA_CREAS_2017.pdf).
A versão de formulário em uso deve ser homologada pela Vigilância. O manual CREAS
consultado também inclui B7, além dos itens solicitados.

## CRAS

A referência implementada no repositório é `mart_rma_cras__indicadores`. Ela não
foi substituída. A supertabela preserva o universo geral; o relatório aplica seus
filtros e contagens distintas. `M` abaixo significa os membros familiares cujas
datas de entrada/saída abrangem o fechamento da competência.

| Indicador | Fonte/caminho | Regra de extração | Situação/limite |
|---|---|---|---|
| A1 | ACOMPANHAMENTO_MENSAL, acompanhamento, familia.membros | Famílias distintas, serviço 1, flag_ativo_fechamento, unidade CRAS | Parcial: estoque disponível; origem sem unidade em parte dos vínculos. Legado escolhe um vínculo por família e atribui unidade por serviço → operador atual → último atendimento CRAS; nova tabela preserva cada unidade original e não atribui responsabilidade histórica por inferência. |
| A2 | Mesmo fato | Famílias distintas com flag_novo_no_mes, inclusive encerradas no mês | Parcial pelas mesmas diferenças de atribuição/seleção de vínculo de A1. |
| B1 | familia.renda_atual | Novas famílias PAIF com renda per capita no corte configurado; duas medidas, CadÚnico e prontuário | Parcial: reutiliza int_familias_extrema_pobreza; renda e composição utilizadas no cálculo são atuais, não renda histórica de ingresso. |
| B2 | familia.membros[].beneficios_cadastro_atual | Novas famílias com Bolsa Família em M; contar família distinta | Parcial: benefício atual sem vigência. |
| B3 | familia.vulnerabilidades + benefícios dos membros | Novas famílias com benefício e vulnerabilidade configurada, vigente no fechamento | Parcial: ID 1 e corte são parâmetros preexistentes que exigem validação; benefício atual. |
| B4 | Benefícios de M | Novas famílias com BPC; contar família distinta | Parcial: cadastro atual. |
| B5 | Violações de M + nascimento | Novas famílias com trabalho infantil em membro menor de 18 no fechamento, conforme implementação existente | Parcial: violações agregadas atuais não certificam ocorrência na competência nem vítima no ingresso. |
| B6 | familia.membros[].acolhimentos | Novas famílias com membro menor de 18 e ciclo de acolhimento vigente no fechamento | Disponível como evidência datada; respeitar limites de atribuição e membros de A2. |
| C1 | ATENDIMENTO, atendimento.nome_atendimento_original/flag_cancelado | Atendimentos distintos por chave com módulo, exceto recepção, não cancelados; não expandir equipe | Disponível; filtros do fato legado e da V2 diferem, não usar FLAG_ELEGIVEL_DATAVIZ como definição automática de RMA. |
| C2 | ENCAMINHAMENTO + categoria/destinos | Famílias distintas ou fallback pessoa, declaração Cadastro/Atualização Cadúnico | Parcial: não distingue inclusão; família sem indivíduo requer M vigente na data da evolução para indicadores individuais. |
| C3 | Mesma origem de C2 | Mesma declaração, sem separação artificial de atualização | Parcial: C2=C3 na implementação atual; não somar como eventos diferentes. |
| C4 | Encaminhamento BPC | Pessoas distintas; para evolução familiar, considerar membros vigentes na data, como no RMA existente | Parcial: declaração multisseleção não comprova envio efetivado/resultado. |
| C5 | Encaminhamento CREAS | Famílias distintas ou fallback pessoa; mesmo pool administrativo F/ficha CRAS e familiar do legado | Parcial: exige aplicar esse pool, não todas as evoluções. |
| C7 | Benefícios/formulários | Concessões de auxílio natalidade no mês | Pendente de evento de concessão/entrega; etiqueta cadastral de benefício não comprova concessão. |
| C8 | Benefícios/formulários | Concessões de auxílio funeral no mês | Pendente pelo mesmo motivo. |
| C9 | Benefícios/formulários | Outros benefícios eventuais concedidos | Pendente de eventos individualizados; não somar benefícios atuais. |
| C6 | ATENDIMENTO | Chave com módulo distinta, nome contendo domiciliar, não cancelado | Disponível; mantém regra nominal existente. |
| D1 | ATIVIDADE_COLETIVA + acompanhamento mensal | Famílias PAIF distintas presentes em grupo regular explicitamente identificado, com membros vigentes no fechamento | Parcial pela atribuição PAIF; flags da atividade reproduzem a regra existente. |
| D2 | atividade.flag_scfv + idade no fechamento | Pessoas distintas de 0–6 anos | Disponível com presenças registradas; ausência de coleta não é ausência de oferta. |
| D3 | atividade.id_tipo_atividade em 1,2 | Pessoas distintas de 7–14 anos | Disponível, regra atual preservada. |
| D4 | Tipo de atividade 3 | Pessoas distintas de 15–17 anos | Disponível. |
| D5 | Tipo de atividade 4 | Pessoas distintas de 60 anos ou mais | Disponível. |
| D6 | atividade.flag_nao_continuado | Pessoas distintas nas atividades classificadas pelos tipos 5–8 | Disponível, sujeito à qualidade do cadastro de atividades. |
| D8 | Presenças/atividade | Adultos de 18–59 em SCFV | Parcial: recorte existe, mas os tipos SCFV 1–4 usados pelo modelo atual não validam a modalidade adulta. Homologar cadastro de atividades; não assumir equivalência. |
| D7 | SCFV ou grupo PAIF + perfil_social.flag_deficiencia | Pessoas distintas com deficiência; união distinta dos dois universos | Parcial: deficiência atual e limite PAIF; não somar os ramos sobrepostos. |

Para D2–D7, reutilizar a prioridade de unidade CRAS da presença, senão da
atividade. A unidade geral da participação mantém a unidade informada na presença,
mesmo quando não é CRAS; `atividade.id_unidade_presenca` e
`atividade.id_unidade_atividade` permitem o recorte do relatório. Não restringir
todas as participações ao PAIF. Idade é calculada com `calc_idade` no fechamento.

## CREAS

| Indicador | Fonte/caminho | Regra exigida / unidade de contagem | Situação/limite |
|---|---|---|---|
| A1 | ACOMPANHAMENTO_MENSAL, serviço 6 | Casos em PAEFI na competência, distinguindo família/indivíduo | Parcial: vínculos familiares disponíveis; não há ciclo individual certificado nem conciliação de todos os casos/unidades. |
| A2 | Mesmo fato, flag_novo_no_mes | Novos casos, não pessoas multiplicadas pelos membros | Parcial: mesmo limite de A1. |
| B1 | Benefícios dos membros de novas famílias | Família distinta com Bolsa Família | Parcial: cadastro atual e universo PAEFI incompleto. |
| B2 | Benefícios dos membros | Família distinta com BPC | Parcial: snapshot atual. |
| B3 | Violações dos membros | Família com trabalho infantil no ingresso | Parcial: falta data/vítima certificada. |
| B4 | Ciclos de acolhimento dos membros | Família com criança/adolescente acolhido na referência | Parcial pelo universo PAEFI; ciclos datados disponíveis. |
| B5 | FORMULARIO, respostas de saúde/violência | Relação entre violação e uso abusivo de substâncias | Pendente de validação/coleta; uso de substância isolado não comprova associação causal. |
| B6 | FORMULARIO + violações | Vítimas distintas nos novos casos, por sexo/faixa etária | Pendente de identificação de vítima e ingresso individual; membro familiar não é automaticamente vítima. |
| B7 | FORMULARIO de MSE | Família com adolescente em medida em meio aberto | Pendente de medida e vigência certificadas; PMA/projeto 23 não equivale a LA/PSC. |
| C1 | Respostas/violações | Violência intrafamiliar em criança/adolescente | Parcial: preserva evidência; falta vínculo vítima/caso/data e tipificação homologada. |
| C2 | Respostas/violações | Abuso sexual em criança/adolescente | Parcial, mesmo limite. |
| C3 | Respostas/violações | Exploração sexual em criança/adolescente | Parcial, mesmo limite. |
| C4 | Respostas/violações | Negligência/abandono em criança/adolescente | Parcial, mesmo limite. |
| C5 | Respostas/violações | Trabalho infantil, recortes oficiais de idade | Parcial, mesmo limite. |
| D1 | Respostas/violações | Violência intrafamiliar em idosos | Parcial: falta temporalidade/vítima. |
| D2 | Respostas/violações | Negligência/abandono em idosos | Parcial: mesmo limite. |
| E1 | Deficiência + respostas/violações | Violência intrafamiliar em pessoa com deficiência | Parcial: deficiência atual, sem certificação de vítima/data. |
| E2 | Mesmas fontes | Negligência/abandono em pessoa com deficiência | Parcial: mesmo limite. |
| F1 | Respostas/violações + sexo/idade | Mulheres adultas e violência intrafamiliar | Parcial: sexo/idade não certificam violência no ingresso. |
| G1 | Respostas/violações | Tráfico de pessoas | Parcial: mapear evidência e homologar vítima/data. |
| H1 | Respostas/violações | Discriminação por orientação sexual | Parcial: orientação sexual não comprova discriminação. |
| I1 | Perfil/formulário de rua | Pessoa em situação de rua no ingresso PAEFI | Parcial: flag atual não certifica estado no ingresso. |
| M1 | ATENDIMENTO | Registros individualizados distintos, excluindo recepção | Disponível como base; homologar tipificação psicossocial e universo de serviço. |
| M2 | ATIVIDADE_COLETIVA | Sessões de grupo distintas, não soma de participantes | Parcial: atividade/data/hora permitem aproximação; falta identificador de sessão e classificação psicossocial homologada. |
| M3 | ENCAMINHAMENTO | Famílias distintas com declaração de encaminhamento CRAS | Parcial: formulário/multisseleção não comprova envio efetivado. |
| M4 | ATENDIMENTO | Visitas domiciliares distintas | Disponível pela regra nominal; homologar correspondência com o formulário. |
| J1 | FORMULARIO MSE | Estoque de adolescentes/medidas LA e PSC | Pendente: não gerar MEDIDA_SOCIOEDUCATIVA sem vínculo/período. |
| J2 | FORMULARIO MSE | Adolescentes em LA | Parcial: aplicação LA/PSC existe no formulário; falta homologar vínculo/ciclo e vigência. |
| J3 | FORMULARIO MSE | Adolescentes em PSC | Parcial: aplicação LA/PSC existe no formulário; falta homologar vínculo/ciclo e vigência. |
| J4 | FORMULARIO MSE | Novos adolescentes em LA e/ou PSC | Parcial: data de inclusão existe; falta conciliar formulários/retornos em ciclo individual sem duplicar adolescentes. |
| J5 | FORMULARIO MSE | Novos adolescentes em LA | Parcial: data de inclusão existe; falta conciliar formulários/retornos em ciclo individual sem duplicar adolescentes. |
| J6 | FORMULARIO MSE | Novos adolescentes em PSC | Parcial: data de inclusão existe; falta conciliar formulários/retornos em ciclo individual sem duplicar adolescentes. |
| K1 | ABORDAGEM | Pessoas distintas no período, por sexo/faixa | Parcial: cluster anual ArcGIS, sem conciliação entre anos/sistemas e contabilização oficial pendente. |
| K2 | abordagem.flag_trabalho_infantil | Pessoas distintas no recorte oficial | Parcial: indicador coletado no contato; validar elegibilidade e identidade. |
| K3 | abordagem.flag_exploracao_sexual | Pessoas distintas no recorte oficial | Parcial: mesmo limite. |
| K4 | abordagem.flag_uso_drogas_ilicitas | Crianças/adolescentes distintos | Parcial: flags de coleta não equivalem automaticamente à definição de uso abusivo. |
| K5 | Mesmas fontes de K4 | Adultos distintos | Parcial: validar recortes e domínio; não inferir condição sem resposta. |
| K6 | abordagem.e_migrante e flags de migração | Pessoas distintas conforme definição oficial | Parcial: validar domínio/migração e universo. |
| L1 | ABORDAGEM | Contatos contabilizáveis, não pessoas distintas | Pendente de validar o repeat como contato efetivo e os controles de exclusão; flag_contabilizavel_rma é NULL. |

As respostas originais são mantidas em FORMULARIO, inclusive cancelamentos e testes.
Não classificar todos os formulários MSE ou PMA como LA/PSC. Os itens de atendimento
individualizado, visitas, encaminhamentos e grupos de versões adicionais do CREAS
podem usar os mesmos fatos; seus códigos e universos precisam ser homologados antes
de publicar indicadores. Não há novos totais oficiais fabricados para esses itens.

## Centro POP

| Indicador | Fonte/caminho | Regra exigida | Situação/limite |
|---|---|---|---|
| A1 | ATENDIMENTO | Pessoas distintas por unidade/mês, sexo e idade no período | Disponível como base; aplicar universo de atendimento, sem contar equipe ou cadastros como pessoas adicionais. |
| B1 | FORMULARIO, respostas de saúde; planilha_centro_pop | Pessoas atendidas com uso de drogas ilícitas | Parcial: resposta sobre substâncias em geral não distingue álcool de drogas ilícitas; exige domínio e recorte temporal. |
| B2 | FORMULARIO, respostas de migração | Migrantes no universo atendido | Pendente: naturalidade/país de origem não equivalem a condição migrante; precisa pergunta validada. ArcGIS refere-se a abordados, outro universo. |
| B3 | perfil_social.flag_saude_mental_comprometida + FORMULARIO | Pessoas atendidas com doença/transtorno mental | Parcial: estado cadastral atual e respostas livres não certificam condição na competência; não usar qualquer comorbidade como transtorno mental. |
| C1 | perfil_social.flag_cadunico e documentos atuais | Inclusão efetiva ocorrida no mês | Pendente: cadastro atual e data declarada não comprovam evento de inclusão. |
| C2 | Atualização CadÚnico da planilha | Atualização efetiva ocorrida no mês | Pendente: snapshot não constitui histórico de eventos; prazo de validade não é data de atualização ocorrida. |
| D1 | ATENDIMENTO | Atendimentos distintos por chave com módulo, sem expandir profissionais | Disponível como volume de registros; homologar filtros do serviço. |
| E1 | ABORDAGEM | Pessoas distintas abordadas por sexo/faixa | Parcial: cluster anual e universo contabilizável pendentes. |
| E2 | Flag de trabalho infantil | Pessoas distintas elegíveis | Parcial: validar coleta/domínio/unidade. |
| E3 | Flag de exploração sexual | Pessoas distintas elegíveis | Parcial: validar coleta/domínio/unidade. |
| E4 | Flag de drogas ilícitas | Crianças/adolescentes distintos | Parcial: coleta não certifica uso abusivo. |
| E5 | Mesma flag | Adultos distintos | Parcial: mesmo limite. |
| E6 | Flags de migração | Pessoas distintas elegíveis | Parcial: homologar domínio e universo. |
| F1 | ABORDAGEM | Ocorrências contabilizáveis, não clusters distintos | Pendente: mesma validação de CREAS L1. |

## Regras transversais

- Restringir `tipo_registro` antes de contar. Não somar eventos, vínculos e estoques.
- Pessoas: DISTINCT id_usuario no prontuário; no ArcGIS, chave anual com namespace.
  Não somar esses dois universos como pessoas únicas sem resolução de identidade.
- Família: DISTINCT id_familia dentro de serviço/unidade/competência, conforme indicador.
- Vários profissionais, benefícios ou violações ficam em arrays e não multiplicam fatos.
- Presenças contam participações; DISTINCT pessoa ou atividade responde outra pergunta.
- Novos casos, vítimas e situações de violência são unidades distintas e sobrepostas.
- Uma família pode aparecer em unidades diferentes. Total municipal exige regra própria;
  a soma das unidades não é automaticamente o número municipal de famílias distintas.
- Cadastro atual, inclusive deficiência, renda e benefício, nunca certifica condição passada.

## Evidências de MSE verificadas na fonte

A auditoria encontrou campos reais de medida aplicada, data de inclusão, status e
desligamento em formulários MSE/RETORNO. Aplicação inclui LA, PSC, combinações e
remissão; os status combinam cumprimento, extinção e medidas previstas. O campo
Serviço de proteção... aparece como undefined nas 1.342 respostas observadas.
Esses dados permanecem disponíveis em FORMULARIO. A pendência é certificar a
ligação entre inclusão, retornos, medidas simultâneas e ciclos, não afirmar que
o sistema não registra qualquer informação de MSE. PMA permanece separada.
