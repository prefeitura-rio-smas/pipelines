#!/usr/bin/env python
# coding: utf-8

# # Conexão com o BigQuery
# 
# - Objetivo: conectar ao bigquery, testar os comando de criação de tabela e overwrite e lapidar para a pipeline

# In[14]:


# 1. Importa o SDK e configura o projeto (opcional)
from google.cloud import bigquery
import os
from dotenv import load_dotenv
from pathlib import Path


# In[15]:


env_path = Path('/root/pipelines/arcgis/abordagem') / '.env'
load_dotenv(dotenv_path=env_path)


# In[16]:


# 2. Cria o cliente
client = bigquery.Client()


# In[17]:


# 3. Lista os datasets no projeto
datasets = list(client.list_datasets())
if datasets:
    print("✔️ Datasets encontrados no projeto:")
    for ds in datasets:
        print(f"  • {ds.dataset_id}")
else:
    print("⚠️ Nenhum dataset encontrado. Verifique PROJECT_ID e credenciais.")


# # Conexão com o Arcgis
# 
# - Objetivo: conectar a feature, analisar os dados e lapidar para a pipeline

# In[18]:


from arcgis.gis import GIS
from arcgis.features import FeatureLayer
import pandas as pd

ARCIS_PORTAL_URL_SIURB = os.getenv("ARCIS_PORTAL_URL_SIURB")
ARCIS_USER_SIURB = os.getenv("ARCIS_USER_SIURB")
ARCIS_PWD_SIURB = os.getenv("ARCIS_PWD_SIURB")

ARCIS_PORTAL_URL_AGOL = os.getenv("ARCIS_PORTAL_URL_AGOL")
ARCIS_USER_AGOL = os.getenv("ARCIS_USER_AGOL")
ARCIS_PWD_AGOL = os.getenv("ARCIS_PWD_AGOL")

ARCIS_ABORDAGEM_FEATURE_SIURB = os.getenv("ARCIS_ABORDAGEM_FEATURE_SIURB")


# In[19]:


# Exemplo para notebooks: input controlado
opcao = input("Deseja logar em qual conta? Digite 'siurb' ou 'agol': ").strip().lower()

if opcao == "siurb":
    print(f"🔐 Logando na conta {opcao}...")
    gis = GIS(ARCIS_PORTAL_URL_SIURB, ARCIS_USER_SIURB, ARCIS_PWD_SIURB)
elif opcao == "agol":
    print(f"🔐 Logando na conta {opcao}...")
    gis = GIS(ARCIS_PORTAL_URL_AGOL, ARCIS_USER_AGOL, ARCIS_PWD_AGOL)
else:
    raise ValueError("Opção inválida. Use 'siurb' ou 'agol'.")

# Confirmação da conta logada
print(f"✅ Logado como: {gis.users.me.username}")


# ***Listagem das camadas usadas até o momento***
# 
# *Abordagem SIURB ID = 6832ff4ca54c4608b169682ae3a5b088*
# 
# *Abordagem AGOL ID = 1ef5fb0ea56c42849d338bb30d796b0f*

# In[20]:


# Input do ID da camada
item_id = ARCIS_ABORDAGEM_FEATURE_SIURB
item = gis.content.get(item_id)

# Confirmação do item recuperado
if item:
    print("Conta:", opcao)
    print("Título:", item.title)
    print("Layers :", [lyr.properties.name   for lyr in item.layers])
    print("Tables :", [tbl.properties.name   for tbl in item.tables])
else:
    print("⚠️ Nenhum item encontrado com esse ID.")


# # 2.1 Ficha de Abordagem Social – SMAS
# 
# - Layer index 0: `"Ficha de Abordagem Social - SMAS"`

# ***Conectando para pegar os dados no arcgis***

# In[ ]:


# já temos `item = gis.content.get(...)`
layer_smas = item.layers[0]  
print("Conta:", opcao)
print("URL da layer:", layer_smas.url)

# consulta sem geometria, pegando só as colunas
fl = layer_smas.query(
    where="1=1",
    out_fields="*",
    return_geometry=False,
    max_records=5
)

# converte para pandas
df_smas = fl.sdf  
print("Linhas × Colunas:", df_smas.shape)
display(df_smas.head())


# In[ ]:


# Lista todos os campos definidos no serviço
fields = [fld["name"] for fld in layer_smas.properties.fields]
print("Total de campos no serviço:", len(fields))
print(fields)


# **Pontos de inspeção**  
# - Número de colunas (`df_smas.shape[1]`)  
# - Tipos de cada coluna (`df_smas.dtypes`)  
# - Valores nulos (`df_smas.isna().sum()`)
# 

# In[ ]:


# detalhes rápidos
print(df_smas.dtypes)
print("\nValores ausentes por coluna:")
print(df_smas.isna().sum())


# ## Processo de ELT
# 
# **Movimento dos dados para o Bigquery**
# 
# - Objetivo: Levar os dados para o Bigquery para serem tratados por lá

# ***Mini processo de tratamento, transformando tudo em string e incluindo o timestamp.***

# In[ ]:


import pandas as pd
from datetime import datetime
import re

df_smas["timestamp"] = datetime.now()
df_smas = df_smas.astype("string")


display(df_smas.head())
print(df_smas.dtypes)


# ***Subida dos dados para o Bucket***

# In[ ]:


from google.cloud import storage
import os

# gera string tipo '20250521_104500'
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Define o nome do arquivo e o path no bucket
bucket_name = "rj-smas-dev"
object_path = f"raw/arcgis/{opcao}/abordagem/ficha/ficha_{timestamp}.csv"

local_csv = "/tmp/ficha.csv"

# Salva o dataframe localmente como CSV
df_smas.to_csv(local_csv, index=False)

# Sobe pro bucket
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(object_path)
blob.upload_from_filename(local_csv)

print(f"✔️ CSV enviado ao bucket: gs://{bucket_name}/{object_path}")


# # 2.2 repeat_abordagem
# 
# - Layer index 1: `"repeat_abordagem"`

# In[ ]:


# já temos `item_x = gis.content.get(...)`
layer_smas = item.layers[1]  
print("Conta:", opcao)
print("URL da layer:", layer_smas.url)

# consulta sem geometria, pegando só as colunas
fl = layer_smas.query(
    where="1=1",
    out_fields="*",
    return_geometry=False,
)

# converte para pandas
df_smas = fl.sdf  
print("Linhas × Colunas:", df_smas.shape)
display(df_smas.head())


# In[ ]:


# Lista todos os campos definidos no serviço
fields = [fld["name"] for fld in layer_smas.properties.fields]
print("Total de campos no serviço:", len(fields))
print(fields)


# **Pontos de inspeção**  
# - Número de colunas (`df_smas.shape[1]`)  
# - Tipos de cada coluna (`df_smas.dtypes`)  
# - Valores nulos (`df_smas.isna().sum()`)
# 

# In[ ]:


# detalhes rápidos
print(df_smas.dtypes)
print("\nValores ausentes por coluna:")
print(df_smas.isna().sum())


# ## Processo de EL
# 
# **Movimento dos dados para o Bigquery**
# 
# - Objetivo: Levar os dados para o Bigquery para serem tratados por lá

# ### Mini processo de tratamento, transformando tudo em string e incluindo o timestamp.

# In[ ]:


import pandas as pd
from datetime import datetime
import re

df_smas["timestamp"] = datetime.now()
df_smas = df_smas.astype("string")


display(df_smas.head())
print(df_smas.dtypes)


# ### Subida dos dados para o Bucket

# In[ ]:


from google.cloud import storage
import os

# gera string tipo '20250521_104500'
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Define o nome do arquivo e o path no bucket
bucket_name = "rj-smas-dev"
object_path = f"raw/arcgis/{opcao}/abordagem/repeat/repeat_{timestamp}.csv"

local_csv = "/tmp/repeat.csv"

# Salva o dataframe localmente como CSV
df_smas.to_csv(local_csv, index=False)

# Sobe pro bucket
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(object_path)
blob.upload_from_filename(local_csv)

print(f"✔️ CSV enviado ao bucket: gs://{bucket_name}/{object_path}")


# ## Processo de T
# 
# **Tratando os dados do bucket e criando as camadas Bronze**
# 
# - Objetivo: Criar as Tabelas Externas no BigQuery

# In[ ]:


from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
import csv

PROJECT_ID  = "rj-smas-dev"
DATASET_ID  = "arcgis_raw"
BUCKET_NAME = "rj-smas-dev"

fontes  = ["siurb"]
tipos   = ["ficha", "repeat"]

bq  = bigquery.Client(project=PROJECT_ID)
gcs = storage.Client(project=PROJECT_ID)

dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"

def header_cols(prefix: str):
    """
    Abre o 1º CSV do prefixo e devolve a lista de colunas.
    Evita placeholder de 'pasta' (nome terminando em '/').
    """
    blobs = (b for b in gcs.list_blobs(BUCKET_NAME, prefix=prefix)
             if not b.name.endswith("/"))
    blob  = next(blobs)                              # pega o primeiro arquivo real
    # lê só a primeira linha
    with blob.open("r") as f:
        header_line = f.readline().strip("\n")
    return next(csv.reader([header_line]))

for fonte in fontes:
    for tipo in tipos:
        table_id = f"{fonte}_abordagem_{tipo}_raw"
        full_id  = f"{dataset_ref}.{table_id}"
        uri_glob = f"gs://{BUCKET_NAME}/raw/arcgis/{fonte}/abordagem/{tipo}/*.csv"
        prefix   = f"raw/arcgis/{fonte}/abordagem/{tipo}/"

        # --- schema: tudo STRING -------------------
        cols   = header_cols(prefix)
        schema = [bigquery.SchemaField(col, "STRING") for col in cols]

        cfg = bigquery.ExternalConfig("CSV")
        cfg.source_uris               = [uri_glob]
        cfg.autodetect                = False           # vamos indicar o schema manual
        cfg.schema                    = schema          # tudo STRING
        cfg.options.skip_leading_rows = 1
        cfg.options.quote_character   = '"'             # padrão ─ volta a ser válido
        cfg.options.allow_jagged_rows = True            # linhas mais curtas = NULL
        cfg.options.allow_quoted_newlines = True        # \n dentro de "campo"
        cfg.max_bad_records = 10                      # pula até 1000 linhas quebradas


        tbl = bigquery.Table(full_id)
        tbl.external_data_configuration = cfg
        bq.create_table(tbl, exists_ok=True)

        print(f"✅  {full_id} → EXTERNAL (all STRING)")

