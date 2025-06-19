import requests
from io import StringIO
import json
import pandas as pd


class APIData():
    def __init__(self,
                 logger):
        self.logger = logger

    def get_from_api(self, url, lista_variaveis):
        df = None
        if url:
            try:
                response = requests.get(url)
                if response.status_code != 200:
                    try:
                        response_json = json.loads(response.text)
                    except Exception:
                        response_json = {}
                    if "error" in response_json:
                        raise Exception("BCB error: {}".format(
                            response_json["error"]))
                    elif "erro" in response_json:
                        raise Exception("BCB error: {}".format(
                            response_json["erro"]["detail"]))

                df = pd.read_json(StringIO(response.text))

                if lista_variaveis:
                    try:
                        df = df[lista_variaveis]
                    except KeyError as e:
                        self.logger.error(f"Coluna não encontrada: {e}")
                        return None

                if df is not None and not df.empty:
                    return df

            except Exception as e:
                self.logger.error(
                    f"Erro ao obter dados SGS: {e}")
                return None
