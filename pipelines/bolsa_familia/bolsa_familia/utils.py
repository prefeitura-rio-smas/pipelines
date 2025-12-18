# pipeline/utils.py
from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd
from google.cloud.storage.blob import Blob


def parse_partition(blob: Blob) -> str:
    """
    Faz o parsing da partição de um blob do GCS baseado no nome do arquivo.
    Para Bolsa Família, o formato esperado é como: BEC.E2.BOD1.F03.C2508.P1466.A3304557.C.ZIP
    Onde C2508 indica o mês de referência (C + YYMM, onde 25 = ano 2025, 08 = mês 08).
    """
    name_parts = blob.name.split(".")

    # Procura por uma parte que comece com "C" seguido de 6 dígitos (CYYMM)
    for part in name_parts:
        if part.startswith("C") and len(part) == 6 and part[1:].isdigit():
            # Extrai YYMM do formato CYYMM
            date_part = part[1:]  # Remove o "C", ex: "2508"
            year = date_part[:2]   # "25"
            month = date_part[2:]  # "08"

            # Monta uma data no formato YYYY-MM-DD (primeiro dia do mês)
            full_year = f"20{year}"  # Assume século 21, ex: "2025"
            parsed_date = f"{full_year}-{month}-01"
            return parsed_date

    # Se não encontrar o padrão CYYMM, tenta o padrão do CadUnico (A + YYMMDD)
    for part in name_parts:
        if part.startswith("A") and len(part) >= 7:  # AYYMMDD ou A + mais dígitos
            partition_info = part[1:]  # Remove o "A"
            # Tenta encontrar um padrão YYMMDD dentro do restante
            # Pode haver mais dígitos, então pegamos os 6 primeiros após A
            date_str = partition_info[:6]
            if len(date_str) == 6 and date_str.isdigit():
                parsed_date = datetime.strptime(date_str, "%y%m%d").strftime("%Y-%m-%d")
                return parsed_date

    # Se não encontrar nenhum padrão conhecido, retorna uma data padrão
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