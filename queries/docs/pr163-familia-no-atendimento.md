# Família diretamente no atendimento

Fonte: `rj-smas-dev.dashboard_acolherio.planilha_unificada`.
Campo recomendado: `atendimentos.registros.id_familia`.

## Regra

Uma família por usuário, módulo e ID de atendimento. A família vem da programada,
independentemente do profissional. Repetições idênticas por profissional são
agrupadas. Valores conhecidos devem concordar; NULL não cria uma família e pode
coexistir com a única família conhecida. Sem família ou sem evento na referência,
`id_familia` é STRING vazia, conforme o contrato público da supertabela.

Mais de uma família conhecida ou mais de um valor de email para a chave bloqueia
a construção com erro explícito. O uso de MAX só ocorre após verificar que os
valores concordam; não escolhe entre famílias conflitantes. Email NULL vira ''.

Família e email usam a mesma referência. Eventos sem correspondência nela mantêm
seu email anterior, com id_familia vazio. Quantidades, profissionais, datas,
unidades, famílias cadastrais, evoluções, formulários e demais campos não mudam
nesta publicação. Não se inferem famílias a partir do cadastro atual.

## Configuração do cartão existente

Atualizar os campos da fonte atual no Looker Studio e migrar o cartão para:

| Configuração | Campo/regra |
|---|---|
| Métrica | Contagem distinta de `atendimentos.registros.id_familia` |
| Família conhecida | Excluir `id_familia` vazio/nulo; evita contar '' como uma família |
| Data do período | `atendimentos.registros.data_atendimento`, convertida para Data AAAA-MM-DD |
| Serviços | `atendimentos.registros.tipo_servico` em cras, creas e centro_pop |
| Unidade | `atendimentos.registros.nome_unidade` diferente de Negado |
| Acesso | Configuração de identidade do relatório antigo aplicada a `atendimentos.registros.email` |

A identidade do visualizador deve ser fornecida pelo mecanismo já usado no
relatório antigo, nunca por um email fixo nem parâmetro livre controlado pelo leitor.
A regra legada é STRPOS(email_do_evento, email_do_visualizador)>0, exigindo identidade
não vazia. Família, data, email, unidade e serviço vêm do mesmo atendimento.
Não cruzar com a lista cadastral `familias` nem com outro array de eventos.

Consulta de conferência (parâmetro viewer vinculado à identidade real do visualizador):

```sql
select count(distinct nullif(a.id_familia, '')) as familias_atendidas
from `rj-smas-dev.dashboard_acolherio.planilha_unificada` p,
    unnest(p.atendimentos.registros) a
where safe_cast(a.data_atendimento as date)
    between date '2026-01-01' and date '2026-11-02'
    and a.tipo_servico in ('cras', 'creas', 'centro_pop')
    and a.nome_unidade != 'Negado'
    and nullif(@viewer, '') is not null
    and strpos(a.email, @viewer) > 0
```

## Compatibilidade e remoção posterior

`atendimentos.familias_atendidas` permanece temporariamente disponível e inalterado,
para não quebrar cartões existentes. Sua remoção, do intermediário e dos testes
exclusivos só deve acontecer após confirmar a migração do BI e a paridade do cartão.
Nesta sessão não há ferramenta de edição do Looker Studio; a migração do cartão
é manual. O campo direto já permite usar uma única lista de atendimentos.

## Evidências de 03/11/2026

Referência e tabela anterior congeladas em
`rj-smas-dev.ci_pr163_direto_20261103_202234`.

| Recorte no período 01/01/2026 a 02/11/2026 | Programada | Campo direto |
|---|---:|---:|
| Todas as famílias | 259499 | 259499 |
| CRAS, CREAS e Centro POP | 258913 | 258913 |
| Acesso do visualizador da validação | 255163 | 255163 |
| Indicador com todos os filtros | 254585 | 254585 |

Seis testes dbt aprovados: fixture do contexto escalar; paridade do campo direto;
paridade e unicidade do bloco de compatibilidade; unicidade dos complementos
por evento e por profissional. A paridade verifica cobertura integral da referência,
um único evento correspondente, família/email iguais e família vazia fora da referência.

Testes negativos reais no BigQuery confirmaram que famílias conflitantes e emails
conflitantes geram erro. A fixture positiva cobre profissionais repetidos, família
nula com uma conhecida, ausência de família/email e IDs iguais em módulos distintos.
Os testes de acesso com visualizador vazio/desconhecido retornaram zero famílias.
Compilação completa do modelo e SQLFluff passaram.

A candidata preservou 595064 usuários. Comparação integral excluindo somente o
novo id_familia e o email atualizado: zero diferenças. Foram atualizados 3741 emails
em eventos presentes na referência; nenhum evento fora dela foi alterado. O bloco
antigo e os campos por profissional foram preservados. O total de atendimentos
permaneceu intacto.

Publicação: snapshot de segurança, cópia da candidata validada, preservação dos
acessos e metadados do destino, remoção da expiração e comparação integral da cópia.
Não alterar agendamentos.
