# Atualização diária do Prontuário em staging

O deployment `dbt-transform-staging` reaproveita o flow dbt existente. O
agendamento é diário às 10h em São Paulo. A etapa do Prontuário roda após o
`dbt build` de staging e exige o sucesso da consulta programada das 05h que
atualiza `rj-smas-dev.dashboard_acolherio.dev_atendimentos`.

O flow captura os atendimentos e a planilha uma vez, reconstrói as dependências
materializadas em um dataset isolado e roda os testes de população, eventos,
perfis por unidade e histórico mensal. As diferenças entre a versão publicada
e a candidata ficam no artefato `prontuario-staging`. Variações de volume são
aceitas quando a candidata reconcilia com as fontes e os controles passam.

A promoção ocorre dentro de uma transação. O snapshot de recuperação fica
disponível por sete dias. Após verificar o destino, o flow remove o dataset
isolado. Em caso de falha ou validação sem promoção, as tabelas de trabalho
expiram em 24 horas. Se a execução falhar antes de criar o job de publicação,
use um novo run para reconstruir a candidata e preserve a anterior para
diagnóstico. Se o job já existir, um retry do mesmo run recupera seu resultado.

## Primeira execução no Prefect

1. Implantar a versão de staging com o agendamento inativo.
2. Disparar `dbt-transform-staging` depois da carga das 05h, com
   `atualizar_prontuario=true` e `publicar_prontuario=false`. Conferir a execução
   do dbt e os controles no artefato. A conta do worker precisa poder consultar
   os jobs do BigQuery e ler as fontes, além de criar datasets e snapshots e
   gravar no destino de desenvolvimento para poder publicar.
3. Após a validação, mudar `publicar_prontuario` e `schedule.active` para `true`
   no manifesto e acompanhar o primeiro ciclo até a confirmação e a limpeza.

## Execução manual fora do Prefect

Na raiz do repositório, com as dependências Python, os pacotes dbt e as
credenciais padrão de aplicação disponíveis:

```sh
uv run python queries/validation/publicar_prontuario_dev.py
uv run python queries/validation/publicar_prontuario_dev.py --publicar
```

Cada comando cria uma nova candidata. O primeiro valida e informa os jobs e
contagens. O segundo também promove a candidata após os mesmos controles.
