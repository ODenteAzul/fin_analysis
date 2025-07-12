import yfinance as yf
from datetime import time, datetime
from zoneinfo import ZoneInfo
from dateutil.relativedelta import relativedelta
import pandas as pd
from config.json_loader import carregar_lista_json


class ScrappIntra():
    def __init__(self,
                 logger,
                 db,
                 table_checker,
                 ddl_creator):
        self.logger = logger
        self.db = db
        self.table_checker = table_checker
        self.ddl_creator = ddl_creator

    def _to_float(self, valor):
        try:
            return float(str(valor).replace(",", "."))
        except (ValueError, TypeError):
            return None

    def _gerar_grade_pregao(data_referencia, intervalo='2min'):
        inicio = datetime.combine(data_referencia, time(10, 0))
        fim = datetime.combine(data_referencia, time(17, 0))

        return pd.date_range(start=inicio, end=fim, freq=intervalo)

    def colheita_cotacao_atual(self):

        ls_empresas = carregar_lista_json("config/empresas.json")

        ls_moedas = carregar_lista_json("config/moedas_intra.json")

        ls_indices_globais = carregar_lista_json(
            "config/indices_globais_intra.json")

        ls_commodities = carregar_lista_json("config/commodities_intra.json")

        ls_cripto = carregar_lista_json("config/crypto_intra.json")

        ls_titulos = carregar_lista_json("config/titles_intra.json")

        # cotação das empresas e moedas importantes
        ls_combined = ls_empresas + ls_moedas + \
            ls_indices_globais + ls_commodities + ls_cripto + ls_titulos

        for tik in ls_combined:

            self.logger.info(
                f"Verificando cotação B2 para {tik['tabela'].upper()}...")

            atualizar = self.table_checker.last_pop(
                camada='meta',
                tabela='controle_populacao',
                nome_serie=tik["tabela"])

            if atualizar is None:

                self.logger.info(
                    f"""Sem dados para {tik['tabela'].upper()},
                    Buscando primeira carga...""")

                self._cotacao_pregao(
                    hoje=False,
                    sigla=tik["ticker"],
                    camada='silver',
                    tabela=tik["tabela"]
                )
            else:

                if atualizar:

                    self._cotacao_pregao(
                        hoje=True,
                        sigla=tik["ticker"],
                        camada='silver',
                        tabela=tik["tabela"]
                    )

                else:

                    self.logger.info(
                        f"""Dados do {tik['tabela'].upper()}:
                        Estão atualizados.""")

    def _cotacao_pregao(self, hoje, sigla, camada, tabela):

        if sigla is None:
            raise TypeError(
                "O parâmetro 'sigla' deve ser informado, não deve ser None")

        if camada is None:
            raise TypeError(
                "O parâmetro 'camada' deve ser informado, não deve ser None")

        if tabela is None:
            raise TypeError(
                "O parâmetro 'tabela' deve ser informado, não deve ser None")

        try:
            ibov = yf.Ticker(sigla)
            br_time = datetime.now(ZoneInfo("America/Sao_Paulo"))
            hoje_data = br_time.date()

            if hoje:
                data_inicial = None
                pop_string = "Atualização do valor de fechamento IBOVESPA"

                df_ibov = ibov.history(period="1d")

                df_ibov.index = pd.to_datetime(df_ibov.index)
                df_ibov.reset_index(inplace=True)

                if not df_ibov.empty:
                    linha = df_ibov.iloc[-1]
                    query = """INSERT INTO silver.cotacao_pregao (
                                                datatime,
                                                ativo,
                                                preco_abertura,
                                                preco_minimo,
                                                preco_maximo,
                                                preco_fechamento,
                                                volume_negociado,
                                                media_movel_50,
                                                media_movel_200,
                                                origem)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                    valores = [(
                        br_time,
                        tabela,
                        self._to_float(float(linha["Open"])),
                        self._to_float(float(linha["Low"])),
                        self._to_float(float(linha["High"])),
                        self._to_float(float(linha["Close"])),
                        int(linha["Volume"]) if not pd.isna(
                            linha["Volume"]) else 0,
                        None,
                        None,
                        'yfinance'
                    )]

                    self.db.executa_query(query, valores[0], commit=True)

                    try:
                        query = """WITH subquery AS (
                                        SELECT datatime,
                                            AVG(preco_fechamento) OVER
                                            (ORDER BY datatime ROWS BETWEEN
                                            49 PRECEDING AND CURRENT ROW)
                                            AS media_50,
                                            AVG(preco_fechamento) OVER
                                            (ORDER BY datatime ROWS BETWEEN
                                            199 PRECEDING AND CURRENT ROW)
                                            AS media_200
                                        FROM silver.cotacao_pregao
                                        WHERE media_movel_50 IS NULL OR
                                        media_movel_200 IS NULL
                                    )
                                    UPDATE silver.cotacao_pregao AS p
                                    SET media_movel_50 =
                                    COALESCE(subquery.media_50,
                                    p.media_movel_50),
                                        media_movel_200 =
                                        COALESCE(subquery.media_200,
                                        p.media_movel_200)
                                    FROM subquery
                                    WHERE p.datatime = subquery.datatime
                                    AND ativo = %s;"""

                        self.db.executa_query(
                            query, valores=[sigla], commit=True)
                    except Exception as e:
                        self.logger.error(
                            (f"Não foi possível calcular as médias"
                             f" móveis para: {tabela}, erro: {e}"))

            else:
                data_inicial = hoje_data
                pop_string = f"Obtendo a carga inicial para {tabela}..."

                Dia_inicio_1h = hoje_data - relativedelta(days=700)
                Dia_fim_1h = hoje_data - relativedelta(days=58)
                Dia_inicio_2m = hoje_data - relativedelta(days=57)

                df_pregao_1h = ibov.history(
                    start=Dia_inicio_1h,
                    end=Dia_fim_1h,
                    interval='60m',
                    auto_adjust=True,
                    prepost=False)
                df_pregao_2m = ibov.history(
                    start=Dia_inicio_2m,
                    end=hoje_data,
                    interval='2m',
                    auto_adjust=True,
                    prepost=False)
                df_pregao_1h.index = pd.to_datetime(df_pregao_1h.index)
                df_pregao_1h.reset_index(inplace=True)
                df_pregao_2m.index = pd.to_datetime(df_pregao_2m.index)
                df_pregao_2m.reset_index(inplace=True)

                df_final = pd.concat(
                    [df_pregao_2m, df_pregao_1h], ignore_index=True)

                if df_final is not None and not df_final.empty:
                    query = """INSERT INTO silver.cotacao_pregao (
                                                    datatime,
                                                    ativo,
                                                    preco_abertura,
                                                    preco_minimo,
                                                    preco_maximo,
                                                    preco_fechamento,
                                                    volume_negociado,
                                                    media_movel_50,
                                                    media_movel_200,
                                                    origem)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                    valores = [
                        (row["Datetime"],
                         tabela,
                         self._to_float(float(row["Open"])),
                         self._to_float(float(row["Low"])),
                         self._to_float(float(row["High"])),
                         self._to_float(float(row["Close"])),
                         int(row["Volume"]) if not pd.isna(
                            row["Volume"]) else 0,
                            None,
                            None,
                            'yfinance')
                        for _, row in df_final.iterrows()]

                    self.db.executa_query(
                        query, valores, commit=True, many=True)

                    try:
                        query = """UPDATE silver.cotacao_pregao AS p
                                        SET media_movel_50 =
                                 COALESCE(subquery.media_50, p.media_movel_50),
                                            media_movel_200 = COALESCE(
                                                subquery.media_200,
                                 p.media_movel_200)
                                        FROM (
                                            SELECT datatime,
                                                AVG(preco_fechamento)
                                 OVER (ORDER BY datatime ROWS BETWEEN 49
                                 PRECEDING AND CURRENT ROW) AS media_50,
                                                AVG(preco_fechamento)
                                 OVER (ORDER BY datatime ROWS BETWEEN 199
                                 PRECEDING AND CURRENT ROW) AS media_200
                                        FROM silver.cotacao_pregao
                                        ) AS subquery
                                        WHERE p.datatime = subquery.datatime
                                 AND ativo = %s;"""

                        self.db.executa_query(
                            query, valores=[sigla], commit=True)

                    except Exception as e:
                        self.logger.error(
                            (f"Não foi possível calcular as médias"
                             f" móveis para: {tabela}, erro: {e}"))

                    self.logger.info(
                        f"Valores para {tabela} obtido com sucesso.")

            self.table_checker.register_populated(
                camada='silver',
                tabela='cotacao_pregao_'+tabela,
                nome_serie=tabela,
                inicial=data_inicial,
                data_exec=hoje_data,
                prox_data=hoje_data,
                obs=pop_string
            )

        except Exception as e:
            self.logger.error(
                (f"Não foi possível obter os dados"
                 f" intradiários para {tabela}, erro: {e}"))
