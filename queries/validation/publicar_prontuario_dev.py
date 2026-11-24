"""Executa manualmente a mesma reconstrução usada pelo daily de staging."""
import argparse
import json
import logging
from uuid import uuid4

from pipelines.datalake.transform.dbt.prontuario import refresh_prontuario


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--publicar", action="store_true", help="Promove a candidata após os controles")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    report = {}
    try:
        refresh_prontuario(uuid4(), publish=args.publicar, report=report)
    finally:
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
