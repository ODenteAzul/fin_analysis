import yfinance as yf
from datetime import time, datetime, timedelta
from zoneinfo import ZoneInfo
from dateutil.relativedelta import relativedelta
import pandas as pd


class ScrappIntra():
    def __init__(self,
                 logger,
                 db,
                 conn,
                 cursor,
                 ls_empresas,
                 table_checker):
        self.logger = logger
        self.db = db
        self.conn = conn
        self.cursor = cursor
        self.ls_empresas = ls_empresas
        self.table_checker = table_checker

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

        ls_moedas = [{"ticker": "USDBRL=X", "tabela": "dolar"},
                     {"ticker": "CADBRL=X", "tabela": "canadian dolar"},
                     {"ticker": "EURBRL=X", "tabela": "euro"},
                     {"ticker": "AUDUSD=X", "tabela": "australian dolar"}]

        # cotação das empresas e moedas importantes
        ls_combined = self.ls_empresas + ls_moedas

        for tik in ls_combined:

            self.logger.info(
                f"Verificando cotação B2 para {tik['tabela'].upper()}...")

            atualizar = self.table_checker.last_pop(
                camada='meta', tabela='controle_populacao', nome_serie=tik["tabela"])

            if atualizar is None:

                self.logger.info(
                    f"Sem dados para {tik['tabela'].upper()}, buscando primeira carga...")

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
                        f"Dados do {tik['tabela'].upper()}, estão atualizados.")

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

            sucesso = False

            if hoje:
                hoje_str = hoje_data.strftime("%Y-%m-%d")
                data_inicial = None
                pop_string = f"Atualização do valor de fechamento IBOVESPA"

                df_ibov = ibov.history(
                    start=hoje_str, end=hoje_str, interval="1d")

                if not df_ibov.empty and df_ibov.index[-1].date() == br_time.date() and not df_ibov is None:
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
                        linha['Datetime'],
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

                    self.db.executa_query(query, valores, commit=True)

                    sucesso = True

                    try:
                        query = f"""WITH subquery AS (
                                        SELECT datatime,
                                            AVG(preco_fechamento) OVER (ORDER BY datatime ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS media_50,
                                            AVG(preco_fechamento) OVER (ORDER BY datatime ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS media_200
                                        FROM silver.cotacao_pregao
                                        WHERE media_movel_50 IS NULL OR media_movel_200 IS NULL
                                    )
                                    UPDATE silver.cotacao_pregao AS p
                                    SET media_movel_50 = COALESCE(subquery.media_50, p.media_movel_50),
                                        media_movel_200 = COALESCE(subquery.media_200, p.media_movel_200)
                                    FROM subquery
                                    WHERE p.datatime = subquery.datatime AND ativo = %s;"""

                        self.db.executa_query(
                            query, valores=[sigla], commit=True)
                    except Exception as e:
                        self.logger.error(
                            f"Não foi possível calcular as médias móveis para: {tabela}, erro: {e}")

            else:
                data_inicial = hoje_data
                pop_string = f"Obtendo a carga inicial para {tabela}..."

                Dia_inicio_1h = hoje_data - relativedelta(days=700)
                Dia_fim_1h = hoje_data - relativedelta(days=58)
                Dia_inicio_2m = hoje_data - relativedelta(days=57)

                df_pregao_1h = ibov.history(
                    start=Dia_inicio_1h, end=Dia_fim_1h, interval='60m', auto_adjust=True, prepost=False)
                df_pregao_2m = ibov.history(
                    start=Dia_inicio_2m, end=hoje_data, interval='2m', auto_adjust=True, prepost=False)
                df_pregao_1h.index = pd.to_datetime(df_pregao_1h.index)
                df_pregao_1h.reset_index(inplace=True)
                df_pregao_2m.index = pd.to_datetime(df_pregao_2m.index)
                df_pregao_2m.reset_index(inplace=True)

                df_final = pd.concat(
                    [df_pregao_2m, df_pregao_1h], ignore_index=True)

                if not df_final is None and not df_final.empty:
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

                    sucesso = True

                    try:
                        query = f"""UPDATE silver.cotacao_pregao AS p
                                        SET media_movel_50 = COALESCE(subquery.media_50, p.media_movel_50),
                                            media_movel_200 = COALESCE(
                                                subquery.media_200, p.media_movel_200)
                                        FROM (
                                            SELECT datatime,
                                                AVG(preco_fechamento) OVER (ORDER BY datatime ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS media_50,
                                                AVG(preco_fechamento) OVER (ORDER BY datatime ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS media_200
                                        FROM silver.cotacao_pregao
                                        ) AS subquery
                                        WHERE p.datatime = subquery.datatime AND ativo = %s;"""

                        self.db.executa_query(
                            query, valores=[sigla], commit=True)

                    except Exception as e:
                        self.logger.error(
                            f"Não foi possível calcular as médias móveis para: {tabela}, erro: {e}")

                    self.logger.info(
                        f"Valores para {tabela} obtido com sucesso.")

            if sucesso:

                proxima = hoje_data + timedelta(days=1)

                self.table_checker.register_populated(
                    camada='silver',
                    tabela='cotacao_pregao_'+tabela,
                    nome_serie=tabela,
                    inicial=data_inicial,
                    data_exec=hoje_data,
                    prox_data=proxima,
                    obs=pop_string
                )

            else:

                if hoje:

                    self.logger.warning(
                        f"{tabela} não retornou dados do fechamento atual — agendada para retry.")

                    self.table_checker.register_populated(
                        camada='silver',
                        tabela='cotacao_pregao_'+tabela,
                        nome_serie=tabela,
                        inicial=data_inicial,
                        data_exec=hoje_data,
                        prox_data=hoje_data,
                        obs="Dado não retornado — agendado para retry."
                    )

                else:

                    self.logger.warning(
                        f"Não foi possível obter a carga inicial para {tabela}— agendada para retry.")

        except Exception as e:
            self.logger.error(
                f"Não foi possível obter os dados intradiários para {tabela}, erro: {e}")
