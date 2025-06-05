# Pipeline ArcGIS - Prefeitura Rio SMAS

Pipeline automatizado para extração, transformação e carregamento (ETL) de dados geoespaciais do ArcGIS para BigQuery, com foco em dados da Secretaria Municipal de Assistência Social (SMAS) da Prefeitura do Rio de Janeiro.

## 📋 Visão Geral

Este projeto implementa um pipeline incremental que:

1. **🔄 Extrai** dados de camadas (layers) do ArcGIS
2. **📦 Processa** e armazena temporariamente em formato Parquet
3. **☁️ Carrega** os dados no BigQuery (tabelas raw)
4. **✨ Transforma** os dados usando modelos dbt (camada gold)

## 🏗️ Arquitetura

```
ArcGIS → Parquet (staging) → BigQuery (raw) → dbt (gold) → Dashboard
```

### Componentes Principais

- **`pipeline/flows.py`**: Orquestrador principal do pipeline
- **`pipeline/tasks.py`**: Funções de extração, staging e carregamento
- **`pipeline/pipelines.yaml`**: Configurações das fontes de dados
- **`queries/`**: Projeto dbt com modelos de transformação

## 🚀 Como Usar

### Pré-requisitos

```bash
pip install --no-cache-dir -r requirements.txt
```

```bash
pip install dbt-bigquery
```

### Configuração

1. **Configure as credenciais do Google Cloud:**
```bash
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
```

2. **Configure o arquivo `pipeline/pipelines.yaml`:**
```yaml
- name: "exemplo_dataset"
  feature_id: "abc123def456"
  account: "siurb"  # opcional, default: siurb
  layers:
    camada_1: 0
    camada_2: 1
```

### Execução

**Executar pipeline completo:**
```bash
python -m pipeline.flows
```

**Executar apenas extração:**
```bash
python -c "from pipeline.tasks import extract_arcgis; df = extract_arcgis('feature_id', 'account', 0)"
```

## 📁 Estrutura do Projeto

```
pipeline_arcgis/
├── pipeline/
│   ├── __init__.py
│   ├── flows.py          # Orquestrador principal
│   ├── tasks.py          # Funções ETL
│   └── pipelines.yaml    # Configurações
├── queries/              # Projeto dbt
│   ├── dbt_project.yml
│   └── models/
│       └── dashboard_arcgis/
│           └── *.sql     # Modelos de transformação
└── README.md
```

## 🔧 Funcionalidades

### Pipeline Incremental (`incremental_flow`)

1. **Extração (ArcGIS)**
   - Conecta com APIs do ArcGIS
   - Suporte a múltiplas contas (siurb, etc.)
   - Extração por camadas (layers)

2. **Staging (Parquet)**
   - Adiciona timestamp automático
   - Armazenamento temporário otimizado
   - Validação de dados

3. **Carregamento (BigQuery)**
   - Tabelas raw com sufixo `_raw`
   - Carregamento incremental
   - Logs detalhados de progresso

4. **Transformação (dbt)**
   - Modelos gold para dashboards
   - Limpeza e padronização de dados
   - Mapeamento de códigos para nomes legíveis

### Exemplo de Transformação

O pipeline inclui transformações como mapeamento de equipamentos:

```sql
CASE
  WHEN equipamento_destino = 'crca_taiguara' 
  THEN 'CRCA Taiguara'
  WHEN equipamento_destino = 'assoc_maranatha_rj_sepetiba' 
  THEN 'Associação Maranatha RJ Sepetiba'
  -- ... outros mapeamentos
END AS equipamento_destino_tratada
```

## 📊 Dados Processados

O pipeline processa dados relacionados a:
- **Abordagens sociais** (população em situação de rua)
- **Equipamentos de assistência social**
- **Encaminhamentos para CREAS**
- **Dados geoespaciais** da cidade do Rio de Janeiro

## 🔍 Monitoramento

O pipeline fornece logs detalhados:
```
↳ Extraindo dataset_exemplo/camada_1 (layer 0)…
   • 1,234 linhas → dataset_exemplo_camada_1_raw
🔄 Executando dbt models (gold)...
✅ dbt concluído com sucesso.
```

## 🛠️ Desenvolvimento

### Adicionando Nova Fonte

1. Adicione configuração no `pipelines.yaml`
2. Crie modelos dbt correspondentes em `queries/models/`
3. Execute o pipeline para testar

### Estrutura de Dados

Todas as tabelas raw incluem automaticamente:
- Timestamp de ingestão
- Dados originais do ArcGIS
- Metadados de origem

## 📝 Licença

Este projeto é mantido pela Prefeitura do Rio de Janeiro - Secretaria Municipal de Assistência Social (SMAS).

## 🤝 Contribuição

Para contribuir com o projeto:
1. Faça fork do repositório
2. Crie uma branch para sua feature
3. Implemente as mudanças
4. Teste o pipeline completo
5. Abra um Pull Request

---

**Contato:** Equipe de Dados - SUBEX/SMAS Rio
**Email:** [gdados.smas@prefeitura.rio](mailto:gdados.smas@prefeitura.rio)