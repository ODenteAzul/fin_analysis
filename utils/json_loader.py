import json
from pathlib import Path
from utils.utils import logging


def carregar_lista_json(caminho_arquivo):
    try:
        with open(Path(caminho_arquivo), 'r', encoding='utf-8') as f:
            return json.load(f)

    except Exception as e:
        logging.error(
            f"""Não foi possível extrair os dados do
            aquivo:{caminho_arquivo}, erro: {e}""")
