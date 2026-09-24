# Relatório — `mart_planilha_unificada_centro_pop`

Atualizado em 2026-09-24. Este documento descreve a versão refatorada da mart e a
validação feita com as tabelas de desenvolvimento. Não contém identificadores nem
valores de documentos pessoais.

## Objetivo e granularidade

A mart tem **uma linha por atendimento não cancelado realizado em unidade Centro
POP**, identificada por `id_atendimento`. A CTE `atendimentos` é a base do `SELECT`
final; usuário, unidade e mês são atributos. A tabela não representa uma linha por
usuário/mês.

O filtro de cancelados e o filtro de usuários de teste permanecem. Usuários sem
correspondência cadastral ainda podem permanecer quando a chave do atendimento
existe e o nome está ausente.

As métricas com sufixo `_mes`, incluindo atendimentos, data do último atendimento e
oficinas, são calculadas por usuário, unidade e mês e repetidas em cada atendimento
do grupo. Não somar essas métricas entre as linhas da mart.

## Organização do modelo

1. `centro_pop` agrupa a dimensão por `id_unidade`, garantindo uma linha por unidade.
   Nomes distintos são ordenados alfabeticamente e reunidos com ` | `, para que uma
   divergência futura fique visível sem multiplicar atendimentos.
2. `atendimentos_candidatos` lê atendimentos e profissionais. O fato pode ter várias
   linhas para o mesmo atendimento quando profissionais compartilham o registro.
3. `atendimentos_eventos` deduplica somente cópias com os mesmos atributos do fato;
   `atendimentos_profissionais` reúne os profissionais em ordem estável. Conflitos
   nos atributos de um `id_atendimento` não são escondidos por `MIN`/`MAX`.
4. `atendimentos` reúne os atributos do evento e forma a população da mart.
5. `atendimentos_mes` e `oficinas` agregam os contextos mensais.
6. Os CTEs cadastrais, de formulários e do CadÚnico são associados sem multiplicar
   eventos; os formulários são selecionados até a data do atendimento.
7. `final_com_nao_informado` converte ausências textuais para `Não Informado`.
   Datas, valores numéricos e identificadores preservam o tipo e podem continuar
   `NULL` se a fonte não fornecer valor.

Na auditoria da dimensão havia 3 unidades Centro POP em 3 linhas, sem ID nulo e sem
nomes divergentes. A agregação continua determinística caso apareçam divergências.

## Seleção temporal dos formulários

Os formulários PAI, Atendimento Social, Desligamento PAI, Acolhimento, Documentação
Civil e Situação de Saúde, além das respostas estruturadas, usam a data do evento
como limite. Uma evolução posterior não é associada retroativamente a um
atendimento anterior.

A macro genérica `queries/macros/latest_record_as_of.sql` seleciona o registro mais
recente até a data do evento. Ela recebe por parâmetros as relações, pares de chaves,
colunas de data, chave de desempate e campos de saída. Não fixa títulos, códigos ou
regras do Centro POP. O desempate usa o identificador do registro em ordem
decrescente.

O modelo também reutiliza `extrair_formulario` para transformar os campos HTML,
`map_flag_boolean` para converter booleanos em rótulos e
`map_coluna_tipo_deficiencia` para traduzir a categoria de deficiência. As evoluções
de abrangência 29 compartilham uma CTE; as fontes de abrangência 1 também compartilham
uma base, preservando o filtro de módulo próprio do formulário Acolhimento.

## PAI: data do atendimento e fechamento mensal

| Coluna | Significado temporal |
|---|---|
| `data_inclusao_acompanhamento` | Primeira data de inclusão conhecida até a data do atendimento; data informada no formulário tem precedência, com data da evolução como fallback. |
| `flag_inserido_acompanhamento` | Indica inclusão PAI conhecida até a data do atendimento. |
| `flag_possui_plano_individual` | Mesma evidência temporal de inclusão PAI, até a data do atendimento. |
| `data_inclusao_acompanhamento_fim_mes` | Primeira inclusão PAI conhecida até o fechamento do mês. |
| `flag_inserido_acompanhamento_fim_mes` | Indica inclusão PAI conhecida até o fechamento do mês. |
| `flag_possui_plano_individual_fim_mes` | Indica a mesma evidência de inclusão no fechamento mensal. |
| `flag_atendido_pontualmente_fim_mes` | Preserva a implementação histórica: “Sim” quando não havia inclusão PAI conhecida até o fim do mês. |

A interpretação de negócio do termo “atendido pontualmente” ainda depende de
confirmação da área. Por isso, o campo mantém a regra mensal existente e explicita o
período no nome, sem afirmar que seja uma flag de inclusão na data do atendimento.

## Fontes e semântica documental

- **CPF:** cadastro do prontuário (`dim_usuarios` / `raw_usuarios.cpf`). O indicador
  de posse só informa `Sim` quando o número está cadastrado; a ausência não prova
  que a pessoa não possui CPF.
- **RG:** `raw_documento_pessoa.rg` do snapshot atual do CadÚnico, ligado por CPF
  normalizado. Padding de zeros à esquerda é removido antes de contar valores
  distintos. O campo continua texto; um valor composto apenas por zeros vira `0`,
  nunca uma string vazia. Variantes que diferem somente pelo padding convergem para
  o mesmo número. Se houver mais de um RG distinto após essa transformação para o
  mesmo CPF, nenhum é escolhido e o resultado fica `Não Informado`.
- **Certidão de nascimento/RCN:** `id_certidao_civil = 1` indica certidão de
  nascimento na fonte. O número vem de `id_termi_matricula_certidao`; a fonte não
  justifica inferir posse de RG ou certidão a partir de CPF, NIS ou formulário.
- **Formulário Documentação Civil:** `flag_registro_formulario_documentacao_civil`
  indica que há registro extraível do formulário (abrangência 24). Não significa
  posse de documento nem, por si só, existência de uma demanda.
- **NIS:** `raw_documento_pessoa.id_membro_familia` →
  `raw_identificacao_membro.id_membro_familia`, agrupado por CPF. A agregação usa
  `ANY_VALUE`; se um CPF tiver mais de um NIS conflitante na fonte, a escolha não é
  determinística.
- **Data CadÚnico:** CPF → família identificada em `raw_documento_pessoa` →
  `raw_identificacao_controle.id_familia`. O ID de família do prontuário não é usado
  como se fosse o ID da família CadÚnico; vínculos ambíguos não recebem data.
- **Dados cadastrais e documentos:** representam o snapshot atual das fontes; não há
  histórico suficiente para reconstruí-los na data passada de cada atendimento.

No retrato de fonte auditado, a transformação do RG não gerou valores vazios. Há
grupos de CPF com mais de um RG distinto depois de remover o padding; esses casos
continuam sem um número escolhido. Grupos em que a fonte continha somente zeros
permanecem representados pelo texto `0`.

## Outros grupos de campos

- **Atendimento:** `id_atendimento`, `data_atendimento` (`DATE`), `nome_atendimento`,
  `tipo_atendimento` e `profissionais_atendimento`. Profissionais são separados por
  vírgula; não há um “profissional de referência” mensal.
- **Métricas mensais:** `data_ultimo_atendimento_mes`,
  `qtd_atendimentos_total_mes`, `qtd_atendimentos_tecnico_mes`,
  `qtd_atendimentos_recepcao_mes`, `qtd_atendimentos_outros_mes` e
  `qtd_oficinas_participadas_mes`.
- **Perfil e saúde:** cadastro atual, idade calculada na data do atendimento, flags
  normalizadas para `Sim`/`Não`/`Não Informado` quando aplicável, escolaridade,
  deficiência e respostas de saúde.
- **Benefício, demanda e encaminhamento:** arrays de origem são convertidos em texto
  separado por vírgulas para facilitar filtros no Looker.
- **Campos ainda sem fonte:** território de referência, possibilidade de reinserção
  familiar, empregabilidade imediata, capacidade/habilidades, interesse em curso,
  resultados e observações aparecem como `Não Informado`.

## Refatoração e contrato

O SQL da mart passou de 848 para 813 linhas, redução de 35 linhas (cerca de 4%). As
seleções temporais repetidas foram substituídas por chamadas da macro genérica; CTEs
duplicadas de evoluções e leituras separadas dos documentos e NIS foram consolidadas.

Mudanças de contrato:

- `flag_atendido_pontualmente` foi renomeada para
  `flag_atendido_pontualmente_fim_mes` para explicitar que preserva uma regra mensal.
- `flag_inserido_acompanhamento` e `flag_possui_plano_individual` agora descrevem a
  data do atendimento. Seus equivalentes de fechamento usam o sufixo `_fim_mes`.
- Foi adicionada `data_inclusao_acompanhamento_fim_mes`.
- Os nomes de atendimento e lista de profissionais permanecem por evento; campos
  mensais continuam repetidos e identificados pelos sufixos `_mes` / `_fim_mes`.

## Validação e comparação

Foi executado `dbt compile` e `dbt build` do modelo. A materialização criou a tabela
de comparação refatorada com 150 linhas; passaram 21 testes de dados, incluindo
`not_null` e `unique` em `id_atendimento`.

Comparação com a tabela anterior, sem retornar valores pessoais:

- 150 linhas e 150 IDs distintos em cada versão; nenhum ID foi adicionado ou
  removido.
- Nenhuma diferença em data/nome do atendimento, lista de profissionais, data de
  inclusão PAI na data do evento, contagens mensais ou flags mensais de PAI.
- Cinco atendimentos tinham inclusão PAI conhecida no fechamento do mês, mas ainda
  não na data do atendimento. As flags de evento refletem essa diferença; as flags
  mensais preservam os valores anteriores.
- Os 2 atendimentos com mais de um profissional mantiveram as mesmas listas.
- Cinco usuários com múltiplos atendimentos foram comparados em uma amostra
  agregada: 11 linhas, sendo 3 técnicas e 8 de recepção; a amostra incluiu 2 casos
  de PAI posterior ao atendimento.
- Nove linhas passaram de RG não informado para um único RG após consolidar variantes
  que diferiam apenas pelo padding de zeros. Nenhum RG final ficou vazio ou começou
  por zeros de padding; nenhum valor RG previamente conhecido mudou.

A extração XLSX `job__9FL0fU5UlhQvmrvsxsvwxEhMs7D.xlsx` não estava disponível no
ambiente nesta revisão. A comparação foi feita com a tabela dev anterior, preservada
em `rj-smas-dev.relatorio.mart_planilha_unificada_centro_pop_comparacao`. A tabela
refatorada está em
`rj-smas-dev.relatorio.mart_planilha_unificada_centro_pop_refatorada`.

## Arquivos de implementação

- Modelo: `queries/models/marts/centro_pop/mart_planilha_unificada_centro_pop.sql`
- Descrições e testes dbt: `queries/models/marts/centro_pop/mart_planilha_unificada_centro_pop.yml`
- Seleção temporal genérica: `queries/macros/latest_record_as_of.sql`
- Extração de formulários reutilizada: `queries/macros/acolherio/extrair_formulario.sql`
