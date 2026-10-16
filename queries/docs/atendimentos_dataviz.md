# Atendimentos para o dataviz geral

`mart_atendimentos` (alias `atendimentos`) substitui a lógica analítica da
consulta programada V2, preservando uma linha por atendimento × profissional
após a deduplicação. Os profissionais compartilhados aparecem nas colunas
escalares, além do array da equipe original.

## Camadas

- Raw: leitura das fontes de atendimentos, famílias, profissionais e cadastro
  social. `raw_usuarios_informacoes_medicas` lê `gh_pac_info_med`.
- Intermediate: `int_atendimentos_completos` normaliza os profissionais sem
  repetir IDs compartilhados; `int_atendimentos_dataviz` enriquece cada
  atendimento com sua equipe, família, projetos, serviços e perfil social.
  `int_perfil_social_usuarios` reúne benefícios, deficiência, situação de rua,
  violações e comorbidades por usuário.
- Mart: expande a equipe, aplica o universo da V2 e deduplica por profissional,
  hora, data, nome original do atendimento, usuário e nome da unidade.

## Compatibilidade e desempates

Preserva `CPF informado`/`CPF não informado`, `Atendimento Recepção`,
`CRAS/CREAS - Recepção - Ação CadÚnico`, os tipos ESCRITÓRIO SOCIAL e LARES
CARIOCAS e as demais colunas da V2. O profissional é associado por seu ID,
nunca pelo login do cadastrante. A classificação usa o mesmo CBO mostrado
na linha, com a normalização e a precedência da V2.

Para unidades com CAS GE, mantém `gdados.smas@prefeitura.rio`, conforme a
tabela programada atual. Esse mapeamento não aparece no trecho V2 fornecido,
mas é necessário para preservar os e-mails do dataviz existente.

O universo mantém os filtros de nome de usuário, unidade e cadastrante
`NOT LIKE '%TESTE%'`, inclusive a exclusão de nomes nulos. No módulo usuário,
somente vínculos familiares ativos são usados; usuários sem vínculo também
permanecem, como no LEFT JOIN da V2. Não há exclusão adicional de atendimentos
cancelados ou profissionais cujo nome contenha TESTE.

A V2 não define completamente qual CBO, família ou duplicata escolher.
Para tornar a saída estável:

- CBO: primeiro código ordenado, com descrição como desempate. A equipe
  mantém todos os CBOs no array de detalhes; não classifica pelo conjunto.
- Família: prioriza vínculo ativo e, entre ativos, entrada mais recente e
  maior ID da família. O histórico completo continua disponível.
- Duplicatas analíticas: menor data de cadastro, menor sequência e módulo.
  O rank é calculado depois de expandir os profissionais, independentemente
  de quem é o principal.
- A idade usa mês e dia para não reproduzir o erro da V2 com DAYOFYEAR
  em aniversários de pessoas nascidas em anos bissextos.

Esses desempates podem escolher linhas e CBOs diferentes da consulta V2,
que varia entre execuções. As diferenças devem ser avaliadas por grupo
analítico e por indicador, e não somente pela sequência selecionada.

## Chaves e contagens

- `ID_ATENDIMENTO`: formato legado sequência-profissional. Não é chave única
  entre módulos e fica nulo para profissional inexistente no cadastro.
- `ID_ATENDIMENTO_SK`: chave da participação, incluindo módulo e profissional
  original, sem repetir a colisão entre módulos do rank de CBO da V2.
- `ID_ATENDIMENTO_MODULO`: identifica o atendimento, repetido para sua equipe.
- `FLAG_PROFISSIONAL_PRINCIPAL`: identifica o principal na linha atual.
- `PROFISSIONAIS` e `QUANTIDADE_PROFISSIONAIS`: equipe do atendimento de
  origem, antes da deduplicação por profissional. Não devem ser somados para
  contar participações da mart.

Use `COUNT(*)` para participações, `COUNT(DISTINCT ID_ATENDIMENTO_MODULO)` para
atendimentos e `COUNT(DISTINCT ID_USUARIO)` para pessoas. Os ranks publicados
são sempre 1; o CBO já é único antes de a equipe ser expandida.

## Enriquecimento social e de saúde

- `PROJETOS_SOCIAIS`: projetos/programas ativos da família, sem cancelamento,
  com nomes do catálogo e IDs preservados no histórico.
- `SERVICOS_SOCIOASSISTENCIAIS`: serviços da família sem cancelamento; nomes
  validados de PAIF e PAEFI. Outros serviços mantêm seu ID como não mapeados.
- Violações, benefícios, deficiência e situação de rua: flags e descrições,
  com códigos e procedência nos arrays de detalhes.
- Comorbidades: indicador, descrição livre, data da informação, local de
  tratamento e dados originais de `gh_pac_info_med`.

São atributos do cadastro atual, inclusive para atendimentos antigos.
Não descrevem necessariamente a situação na data do atendimento. As flags
distinguem Sim, Não e Não Informado; ausência não vira Não e o texto livre
de comorbidades não é convertido em diagnóstico.

Os campos sociais se repetem por profissional. Contagens de usuários com
alguma condição devem usar pessoas distintas, para não multiplicar o total
pela quantidade de atendimentos ou integrantes da equipe.

O alias segue a configuração medallion de schema/ambiente do dbt. Atualizar
a fonte do dataviz e retirar a consulta programada são etapas posteriores;
este modelo não sobrescreve `dashboard_acolherio.dev_atendimentos`.

## Validação em 16/10/2026

O SQL compilado foi executado no BigQuery em tabelas temporárias e comparado
com `rj-smas-dev.dashboard_acolherio.dev_atendimentos`:

- V2: 1.022.072 grupos analíticos; mart: 1.022.187 participações, com chave
  técnica única e preenchida em todas as linhas.
- Todos os grupos da V2 estão representados. Há 115 grupos adicionais:
  102 com ID legado que colide entre módulos e 13 com ID legado nulo.
- Comparando os 1.022.072 grupos, houve zero diferenças em documentação,
  tipo de unidade, classificação, nome de atendimento, profissional, CBO,
  número de documento e endereço. Projetos também coincidem para a mesma
  família.
- O desempate estável selecionou outro atendimento em 3.739 grupos, mudando
  cadastrante em 223 e família em 73. Em 650 participações a idade foi
  corrigida no aniversário. Essas diferenças não significam perda de grupos.
- Não há nomes nulos/de teste nos filtros da V2, vínculos encerrados no
  módulo usuário nem grupos analíticos duplicados.
- Há 32.199 participações de 1.715 usuários com indicador de comorbidade Sim.
- As 39 colunas anteriores mantêm nome e tipo; a saída tem 84 colunas.

Os oito testes dbt compilados foram executados sobre o resultado temporário,
sem falhas. Sete asserções adicionais com dados sintéticos verificaram
deduplicação independente dos compartilhados, inclusive quando o principal
é duplicata, exclusão de entradas inelegíveis e profissional desconhecido.

A credencial local não acessa a planilha do Drive. Na validação local,
o filtro de e-mails foi substituído pelos valores já materializados em
`relatorio.atendimentos`, preservando a agregação por unidade. O build do CI
usa a credencial do pipeline e a planilha original. A conferência de seu
resultado materializado identificou o mapeamento de CAS GE descrito acima.
