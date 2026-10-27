> Contrato atual: `por_assunto_v9_campos_diretos_formularios_texto`. Cadastro, documentos e endereço ficam diretamente em `usuario`; contagens por serviço ficam diretas nos blocos de eventos; formulários apresentam listas de textos `rótulo: valor`. Os 35 campos do Centro POP permanecem diretamente em `escopos.centro_pop.<campo>`.

> Complementos do BI: [por_assunto_v7_complementos_bi](complementos_bi_planilha_unificada.md), com os 13 complementos do BI e publicação por lote derivado.

> Atualização: dimensões independentes, contagens por serviço e publicação controlada estão descritas em [dimensoes_planilha_unificada.md](dimensoes_planilha_unificada.md). Os comandos diretos antigos de construção da tabela foram substituídos pelo roteiro de candidatos desse documento.

# Planilha unificada por usuário

Projeto: `/opt/pipelines/pipelines/queries`.
Destino de desenvolvimento: `rj-smas-dev.relatorio.planilha_unificada`.
Modelo: `models/marts/planilha_unificada/planilha_unificada.sql`.

## Contrato por assunto

Uma linha por `usuario.id_usuario`, com a mesma população de `dim_usuarios`.
A raiz tem exatamente seis colunas:

| Bloco | Estrutura e conteúdo |
|---|---|
| `usuario` | STRUCT com os atributos cadastrais anteriores, incluindo ID, nomes, filiação, documentos, nascimento, perfil, flags e `nome_unidade_referencia`; contém os campos de cadastro complementar, documentos e endereço diretamente, além das listas `beneficio`, `deficiencia` e `violacoes`. |
| `familias` | ARRAY de famílias atuais ou históricas associadas ao usuário, com responsável, estado cadastral atual, vínculos datados e serviços familiares. |
| `projetos_sociais` | ARRAY com nome do projeto, nome da unidade, indicador e datas. |
| `atendimentos` | STRUCT com `quantidade` INT64, `quantidade_cras`, `quantidade_creas`, `quantidade_centro_pop` e `quantidade_outros` e `registros` ARRAY de eventos, com profissionais. |
| `evolucoes` | STRUCT com `quantidade` INT64, `quantidade_cras`, `quantidade_creas`, `quantidade_centro_pop` e `quantidade_outros` e `registros` ARRAY de evoluções, cada qual com descrição e formulários associados. |
| `escopos` | Histórico, participações e perfil consolidado do Centro POP; contrato específico preservado. |

Os identificadores públicos incluem usuário, família, atendimento/evolução
qualificados por módulo, atividade do escopo Centro POP e, na linha profissional,
`id_profissional`, `id_familia_referencia` e `id_atendimento_legado`. Os três últimos
foram acrescentados pelo contrato v7 para conservar as relações da referência do BI.
Todos os escalares são STRING, exceto as quantidades totais e por serviço de
`atendimentos` e `evolucoes`, que são INT64. Ausências são `''`; contagens sem registros são 0.
STRUCTs ausentes apresentam campos vazios e coleções sem registros são `[]`,
sem criar itens para simular ausência. A interface pode mostrar campos de listas
vazias como NULL; isso não indica um registro com valor nulo na tabela.

A alteração substitui o contrato anterior. `id_usuario`, `formularios`, os
campos cadastrais e as duas quantidades não permanecem na raiz como aliases.
A consulta antiga `UNNEST(p.atendimentos)` deve usar
`UNNEST(p.atendimentos.registros)`; formulários são acessados por
`UNNEST(p.evolucoes.registros)` e depois `UNNEST(e.formularios)`.

## Cadastro, documentos e perfil

Os atributos cadastrais mantêm o significado e os valores existentes. Nenhum
atributo de outra fonte foi acrescentado. Benefícios, deficiências e violações
preservam suas descrições e multiplicidade; violações mantêm `origem`.
Os campos diretos `uf_nascimento`, `serie_escolar` e `data_cadunico_informada` incluem UF de nascimento, série escolar bruta e data de
CadÚnico declarada no cadastro. Documentos e controle do CadÚnico são associados
pelo CPF normalizado, com flags de ambiguidade por atributo. Nenhuma família ou
documento conflitante é escolhido arbitrariamente. Ausência não equivale a false.

Para detectar ausência, usar `NULLIF(TRIM(campo), '') IS NULL`; `COUNT(campo)`
inclui strings vazias. Para cálculos, usar `SAFE_CAST(NULLIF(campo, '') AS DATE)`
ou o tipo apropriado. Datas, booleanos e números mantêm a apresentação STRING
anterior; não há preenchimento automático de zeros, negativas ou datas padrão.

## Unidades

Os nomes são obtidos de `dim_unidades.nome_unidade` no lote de cada serviço, com join por ID antes
da agregação. A dimensão deve ter um único registro por ID; duplicações bloqueiam
o build. Unidades inativas disponíveis na dimensão continuam elegíveis.

`id_unidade_referencia` vira `usuario.nome_unidade_referencia`; `id_unidade`
vira `nome_unidade` em projetos, serviços familiares e registros de atendimentos
e evoluções. Os IDs originais continuam nos intermediários. Nomes iguais não
causam deduplicação. Nomes não são chaves técnicas únicas para joins de indicadores.

Referência preenchida sem nome na dimensão resulta em `''` e aviso de cobertura.
Não há fallback para cadastrar rótulos nem retirar registros. Os quatro
atendimentos da unidade 81 (`UNIDADE TESTE CRAS`, excluída da dimensão) permanecem
com nome vazio. A revisão não altera essa política de população.

## Famílias, serviços e projetos

`familias` mantém `id_familia`, nome do responsável, estado atual e vínculos datados.
Inclui responsáveis sem linha de membro. Os serviços são da família e se repetem
entre usuários associados; não comprovam atendimento individual. Cancelamentos
familiares permanecem no histórico. Não se aplica cruzamento de vigência entre
vínculo familiar do usuário e serviço. Contar famílias por ID distinto.

Os serviços 1 (PAIF) e 6 (PAEFI) possuem tradução confirmada. Os códigos 2, 3, 4,
5, 7, 8, 9 e 10 permanecem pendentes; nome vazio e aviso de catálogo preservam os
registros. Os 32 nomes de projetos vêm da referência auxiliar de 09/10/2026,
incorporada à macro, sem dependência de dataset de CI em runtime.

A regra compartilhada dos projetos permanece: membros familiares ativos e apenas
o registro mais recente por usuário/projeto. Não representa histórico completo,
nem resolve a precedência entre indicador ativo e data de cancelamento.

## Atendimentos

`atendimentos.quantidade = ARRAY_LENGTH(atendimentos.registros)`, para todo o
histórico disponível. Vários profissionais não multiplicam um atendimento.
Cada registro contém `id_atendimento_modulo`, origem, nome da unidade, descrição
do tipo, data, hora, flag de cancelamento e `profissionais` (nomes e descrições CBO).
O ID com módulo distingue números iguais entre módulos família e usuário.

Na publicação por referência congelada, os eventos de dev_atendimentos são
preservados sem filtro adicional de cancelamento. O modo nativo do intermediário
continua excluindo flag S antes de agregar. Horas HHMM ou H:MM/HH:MM válidas são exibidas
como HH:MM. Ausência ou entrada inválida fica vazia; entradas não vazias inválidas
geram aviso na origem. Ordenação e deduplicação usam as chaves originais.

## Evoluções e formulários

`evolucoes.quantidade = ARRAY_LENGTH(evolucoes.registros)`, para todas as evoluções
não canceladas associadas ao usuário, incluindo aquelas sem formulário extraível.
O filtro usa `data_cancelamento IS NULL` no campo tipado de `int_pu_evolucoes`.
Os módulos usuário, família, administrativo e grupo preservam as associações da
origem. Evoluções familiares pertencem apenas ao paciente explicitamente indicado;
evoluções de grupo aparecem uma vez por participante identificado.

Cada registro contém `id_evolucao_modulo`, origem, `id_familia`, `nome_unidade`,
datas, tipo, profissional, atividade, `descricao_evolucao` e `formularios`.
A descrição é texto legível: remove comentários e conteúdo técnico script/style,
converte separadores HTML em quebras de linha, remove tags e então decodifica
entidades. Essa ordem preserva texto escapado, como `&lt;exemplo&gt;`. Entidades
inválidas ou desconhecidas permanecem literais. O HTML original fica a montante.

Os formulários são agrupados pelo usuário e pelo ID qualificado da evolução,
com `titulo_formulario`, `observacoes` e `campos = ARRAY<STRING>`, com cada item
no formato `rótulo: valor`. Os pares `label`/`valor` permanecem no intermediário.
Títulos, observações e respostas mantêm o tratamento já existente, inclusive a
marcação preservada nas observações; a descrição legível não modifica esses campos.
A macro `decodificar_entidades_html` trata HTML 4, apos e referências numéricas.
Formulários adicionais não multiplicam a evolução e sua ordem é preservada.
Evolução sem formulário possui `formularios = []`. Usuários sem eventos continuam
na base com `quantidade = 0` e `registros = []` nos respectivos blocos.

## Consulta com mais campos preenchidos

Conta campos distintos por título e label; respostas Não, false e 0 são válidas.
Não conta atributos pessoais, atendimentos nem o texto livre da evolução.

```sql
select
    p.*,
    (
        select count(distinct to_json_string(struct(
            f.titulo_formulario as formulario,
            c.label as campo
        )))
        from unnest(p.evolucoes.registros) as e,
            unnest(e.formularios) as f,
            unnest(f.campos) as c
        where nullif(trim(c.label), '') is not null
            and lower(trim(coalesce(c.valor, ''))) not in (
                '', '-', 'não informado', 'nao informado',
                'não informada', 'nao informada', 'undefined', 'null'
            )
    ) as qtd_campos_preenchidos
from `rj-smas-dev.relatorio.planilha_unificada` as p
order by qtd_campos_preenchidos desc, p.usuario.id_usuario asc
limit 100;
```

Para ordenar pelos eventos, usar `ORDER BY atendimentos.quantidade DESC` ou
`ORDER BY evolucoes.quantidade DESC`, com `usuario.id_usuario` para desempate.

Exemplo de atendimentos mensais por nome da unidade:

```sql
select
    date_trunc(safe_cast(nullif(a.data_atendimento, '') as date), month) mes,
    a.nome_unidade,
    count(distinct a.id_atendimento_modulo) atendimentos
from `rj-smas-dev.relatorio.planilha_unificada` p,
    unnest(p.atendimentos.registros) a
group by mes, a.nome_unidade;
```

Nomes homônimos serão agregados nesse exemplo; para distinguir unidades técnicas,
usar os IDs nas camadas intermediárias. Abrir duas coleções independentes no mesmo
FROM produz combinações entre seus itens: agregar separadamente antes de juntar.

## Execução e validação

A tabela e as quatro dimensões são materializadas; os intermediários de preparação são ephemeral. A execução é explícita,
sem tag daily. A mart Centro POP existente e os modelos compartilhados permanecem
independentes. Atividades e questionários não possuem blocos na saída; evoluções
e atendimentos originados desses assuntos continuam elegíveis nas regras acima.

```bash
# Construção e publicação: seguir o roteiro em dimensoes_planilha_unificada.md.
.venv/bin/python queries/operations/planilha_unificada.py --help

RUN_BIGQUERY_TESTS=1 .venv/bin/python -m unittest discover \
  -s queries/tests/planilha_unificada -p test_modelos.py -v
```

Os testes conferem população, nomes de unidade, unicidade das unidades, IDs
permitidos, cinco blocos, tipos, ausência de NULL, identidade dos eventos,
quantidades, todas as evoluções, formulários e suas associações/posições,
vínculos familiares e profissionais. Catálogos e unidades sem correspondência
geram avisos sem excluir registros. Cenários sintéticos incluem homônimos,
usuários sem eventos, cancelamentos, texto HTML, entidades e evoluções sem campos.

## Limites para os produtos derivados

Esta base não implementa indicadores RMA, competência ou vigência passada.
Cadastro, documentos e nomes são atuais; as contagens são históricas dentro da
população de dim_usuarios, que já possui filtros da origem. Ausência em listas
não comprova negativa ou falta de oferta. A conversão de NULL para vazio e de
STRUCT ausente para campos vazios perde distinções na apresentação; os
intermediários mantêm tipos, chaves e valores originais para análises tipadas.
Famílias sem usuário associado e serviços sem pessoa identificada exigem bases
complementares. As ressalvas de projetos, CadÚnico, flags e inferência de violações
continuam presentes; esta reorganização não as modifica.

## Publicação e evidências de 23/10/2026

Contrato por assunto publicado em desenvolvimento às 20:02:26 UTC, com cinco
blocos e schema conferido recursivamente. A população passou da fotografia
anterior de 582.416 para 584.741 usuários distintos porque dim_usuarios foi
atualizada às 19:22:12 UTC; a nova tabela acompanha exatamente a fonte atual.
Contagens publicadas: 1.017.864 atendimentos, 648.549 evoluções, 386.255 formulários,
6.385.485 campos, 555.367 pares usuário/família e 5.688 itens de projetos.

A comparação com o contrato anterior recalculado sobre as mesmas fontes atuais,
após reagrupar dados pessoais, envolver atendimentos e traduzir unidades,
produziu o mesmo checksum para usuario, familias, projetos_sociais e atendimentos.
A publicação anterior não foi usada como referência imutável de população, pois
estava desatualizada. A redução de duas evoluções e um formulário em relação à
fotografia anterior foi conferida contra a população atual da origem.

Validação final: 31 testes dbt aprovados, nenhum erro, dois avisos (oito códigos
de serviço pendentes e seis IDs de unidade sem nome na dimensão); 17 cenários
sintéticos aprovados. SQLFluff, Ruff e git diff --check passaram. Os dois exemplos
SQL e a consulta parametrizada em /opt/pipelines/temp foram validados em dry run.
Os quatro atendimentos da unidade 81 continuam presentes com nome vazio.

A consulta parametrizada atualizada está em
`/opt/pipelines/temp/consulta-planilha-unificada-campos-preenchidos.sql`.
A documentação anterior à reorganização foi preservada como referência em
`/opt/pipelines/temp/planilha-unificada-documentacao-antes-blocos.md`.


### Contrato v9: campos diretos e formulários em texto

O contrato atual é `por_assunto_v9_campos_diretos_formularios_texto`. Os campos dos antigos
agrupadores `usuario.cadastro_complementar`, `usuario.documentos_cadunico` e
`usuario.endereco` ficam diretamente em `usuario`, com os mesmos nomes dos campos.
Em `atendimentos` e `evolucoes`, as contagens por serviço ficam em
`quantidade_cras`, `quantidade_creas`, `quantidade_centro_pop` e `quantidade_outros`.

`evolucoes.registros.formularios.campos` é `ARRAY<STRING>` com itens `rótulo: valor`,
sem deduplicar, reordenar, dividir respostas ou remover itens vazios. Título,
observações e vínculo com cada evolução permanecem. Sem campos, a lista é `[]`.
Os pares originais `label`/`valor` continuam no intermediário para auditoria.

Os seis blocos principais continuam nativos; selecionar um bloco inteiro pode
exibir um objeto na ferramenta de consulta. Exemplo de seleção dos campos:

```sql
SELECT p.usuario.id_usuario, p.usuario.logradouro,
       p.atendimentos.quantidade_cras, f.titulo_formulario, f.campos
FROM `rj-smas-dev.relatorio.planilha_unificada` p
LEFT JOIN UNNEST(p.evolucoes.registros) e ON TRUE
LEFT JOIN UNNEST(e.formularios) f ON TRUE
```

Esta consulta tem granularidade por formulário; a tabela permanece com uma linha
por usuário. A publicação v9 usa a mesma base congelada, backup, validação integral
e restauração em caso de falha. A consulta programada permanece pausada.

Evidências da rematerialização v9: [validação e backups](validacao_campos_diretos_formularios_20261027.json).
