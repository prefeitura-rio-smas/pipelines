# pipeline/utils.py
from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd
from google.cloud.storage.blob import Blob


def parse_partition(blob: Blob) -> str:
    """
    Faz o parsing da partição de um blob do GCS baseado no nome do arquivo.
    Este é um exemplo genérico - ajuste conforme a estrutura real dos arquivos do Bolsa Família.
    """
    # Exemplo de parsing baseado em um padrão no nome do arquivo
    # Ajuste conforme o formato real dos arquivos do Bolsa Família
    name_parts = blob.name.split(".")
    date_part = None
    
    # Procura por uma parte do nome que pareça com uma data
    for part in name_parts:
        if len(part) >= 6 and part.isdigit():  # YYYYMM ou YYYYMMDD
            if len(part) == 6:
                # Assume formato YYMMDD
                date_part = part
                break
            elif len(part) == 8:
                # Assume formato YYYYMMDD
                date_part = part
                break
    
    if date_part:
        if len(date_part) == 6:
            # YYMMDD format
            parsed_date = datetime.strptime(date_part, "%y%m%d").strftime("%Y-%m-%d")
        else:  # 8 digits
            # YYYYMMDD format
            parsed_date = datetime.strptime(date_part, "%Y%m%d").strftime("%Y-%m-%d")
        return parsed_date
    else:
        # Retorna uma data padrão se não encontrar no nome
        return datetime.now().strftime("%Y-%m-%d")


def parse_txt_first_line(filepath: str) -> Tuple[str, str]:
    """
    Faz o parsing da primeira linha do arquivo TXT para extrair informações de layout e data.
    Ajuste os índices conforme a estrutura real dos arquivos do Bolsa Família.
    """
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        first_line = f.readline()
    
    # Estes são valores padrão - ajuste conforme o formato real dos arquivos
    # Por exemplo, se os arquivos do Bolsa Família tiverem um cabeçalho com layout específico
    if len(first_line) >= 90:
        # Exemplo de parsing de layout - ajuste conforme necessário
        txt_layout_version = first_line[69:74].strip().replace(".", "")
        dta_extracao_dados_hdr = first_line[82:90].strip()
        txt_date = datetime.strptime(dta_extracao_dados_hdr, "%d%m%Y").strftime("%Y-%m-%d")
        return txt_layout_version, txt_date
    else:
        # Retorna valores padrão se o formato não for reconhecido
        return "1.0", datetime.now().strftime("%Y-%m-%d")


def get_bolsa_familia_schema() -> dict:
    """
    Retorna o schema padrão para os dados do Bolsa Família.
    Ajuste conforme a estrutura real dos dados.
    """
    # Este é um schema exemplo - substitua conforme a estrutura real dos dados do Bolsa Família
    schema = {
        "controle": ["cod_orgao", "nom_orgao", "cod_programa", "nom_programa"],
        "familia": ["num_nis_fam", "cod_situacao", "municipio", "uf"],
        "beneficiario": ["num_nis_ben", "nome_beneficiario", "cpf_beneficiario", "data_inicio"],
        "pagamento": ["num_parcela", "vlr_parcela", "mes_ano_referencia", "data_pgto"]
    }
    return schema