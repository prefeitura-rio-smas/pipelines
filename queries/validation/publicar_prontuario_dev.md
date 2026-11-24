# Materialização do prontuário em desenvolvimento

Na raiz do repositório, com as dependências Python e os pacotes dbt instalados,
execute com as credenciais padrão de aplicação do BigQuery:

```sh
uv run python queries/validation/publicar_prontuario_dev.py
uv run python queries/validation/publicar_prontuario_dev.py --publicar
```

O primeiro comando constrói e valida uma candidata. O segundo repete o processo
com uma nova candidata e promove o resultado validado em
`rj-smas-dev.abordagem.prontuario_eletronico`. A conta precisa poder ler as fontes
de desenvolvimento, criar datasets e executar jobs. Para publicar, também
precisa poder gravar na tabela de destino.

O comando lê a exportação da aba de e-mails configurada no BigQuery e reconstrói
os registros e o histórico do Centro POP antes de materializar o prontuário. As
tabelas de trabalho expiram em 24 horas. O histórico abrange todas as
competências com movimento e a tabela compartilhada `relatorio.centro_pop_mensal`
permanece disponível para seus consumidores.

Use o dataset e os IDs de jobs exibidos no terminal para investigar falhas. Uma
mudança de esquema, chaves novas ou removidas ou um controle inconsistente
bloqueia a promoção e deixa a candidata disponível até sua expiração.
