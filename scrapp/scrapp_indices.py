import requests
import yfinance as yf
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from utils.api_client import APIDataParser
from dateutil.relativedelta import relativedelta
import pandas as pd


class ScrappIndices():
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

    def busca_histórico_macroeconomia(self):

        self.logger.info(
            "Verificando a presença de dados históricos de macro economia...")

        try:

            indicadores = [
                {"codigo": 11, "tabela": "selic"},
                {"codigo": 433, "tabela": "ipca"},
                {"codigo": 12, "tabela": "cdi"},
                {"codigo": 189, "tabela": "igpm"}
            ]

            pares_moeda = {
                "USD-BRL": "Dólar Americano",
                "EUR-BRL": "Euro",
                "GBP-BRL": "Libra Esterlina",
                "ARS-BRL": "Peso Argentino",
                "CNY-BRL": "Yuan Chinês"
            }

            juros_eua = [
                {"serie": "EFFR", "tabela": "juros_usa_effr"},
                {"serie": "FEDFUNDS", "tabela": "juros_usa_fedfunds"}
            ]

            for ind in indicadores:

                self.logger.info(
                    f"Verificando {ind['tabela'].upper()}...")

                if not self.table_checker.check_populated(camada='silver', tabela=ind["tabela"], empresa='macro', resposta='bool'):
                    self._atualiza_sgs_bacen(
                        codigo_sgs=ind["codigo"],
                        hoje=False,
                        camada='silver',
                        tabela=ind["tabela"]
                    )
                else:
                    self.logger.info(f"Dados da {ind['tabela'].upper()}: OK.")

            if not self.table_checker.check_populated(camada='silver', tabela='cambio_diario', empresa='macro', resposta='bool'):

                for par, nome in pares_moeda.items():

                    self.logger.info(
                        f"Verificando histórico de cotação: {par} ...")

                    self._atualiza_cambio(par_moeda=par, hoje=False)

                self.table_checker.register_populated(
                    camada='silver',
                    tabela='cambio_diario',
                    empresa="macro",
                    status=True,
                    data_populated=datetime.today().date(),
                    observation="Carga cambio_diario histórico"
                )

            else:
                self.logger.info(f"Dados de Cambio Diário: OK.")

            for juros in juros_eua:

                self.logger.info(
                    f"Verificando {juros["serie"]} (juros americano)...")

                if not self.table_checker.check_populated(camada='silver', tabela=juros["tabela"], empresa='macro', resposta='bool'):
                    self._atualiza_juros_eua(
                        serie=juros["serie"],
                        hoje=False,
                        camada='silver',
                        tabela=juros["tabela"]
                    )
                else:
                    self.logger.info(
                        f"Dados da {juros["serie"]} (juros americano): OK.")

            self.logger.info(
                "Coleta de dados históricos efetuada com sucesso.")

        except Exception as e:
            self.logger.error(
                f"Não foi possível obter o histórico de dados macro econômicos: {e}")

    def _atualiza_serie_ibovespa(self, hoje=True):
        try:
            ibov = yf.Ticker("^BVSP")

            if hoje:
                br_time = datetime.now(ZoneInfo("America/Sao_Paulo"))
                hoje_data = br_time.date()
                hoje_str = hoje_data.strftime("%Y-%m-%d")

                df_ibov = ibov.history(
                    start=hoje_str, end=hoje_str, interval="1d")

                if not df_ibov.empty and df_ibov.index[-1].date() == br_time.date():
                    linha = df_ibov.iloc[-1]
                    query = """INSERT INTO silver.ibovespa_diario (
                                                data,
                                                preco_abertura,
                                                preco_minimo,
                                                preco_maximo,
                                                preco_fechamento,
                                                volume_negociado,
                                                media_movel_50,
                                                media_movel_200)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                    valores = [(
                        linha.name.date(),
                        float(linha["Open"]),
                        float(linha["Low"]),
                        float(linha["High"]),
                        float(linha["Close"]),
                        int(linha["Volume"]) if not pd.isna(
                            linha["Volume"]) else 0,
                        None,
                        None
                    )]

                    self.db.executa_query(query, valores, commit=True)
                else:
                    print("O dado de fechamento de hoje ainda não foi disponibilizado.")
            else:
                historico = ibov.history(period="10y")
                historico.index = pd.to_datetime(historico.index)
                historico.reset_index(inplace=True)

                query = """INSERT INTO silver.ibovespa_diario (
                                                data,
                                                preco_abertura,
                                                preco_minimo,
                                                preco_maximo,
                                                preco_fechamento,
                                                volume_negociado,
                                                media_movel_50,
                                                media_movel_200)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                valores = [
                    (row["Date"].date(),
                     float(row["Open"]),
                     float(row["Low"]),
                     float(row["High"]),
                     float(row["Close"]),
                     int(row["Volume"]) if not pd.isna(
                        row["Volume"]) else 0,
                        None,
                        None)
                    for _, row in historico.iterrows()]

                self.db.executa_query(query, valores, commit=True, many=True)

                query = """UPDATE silver.ibovespa_diario AS p
                                            SET media_movel_50 = COALESCE(subquery.media_50, p.media_movel_50),
                                                media_movel_200 = COALESCE(
                                                    subquery.media_200, p.media_movel_200)
                                            FROM (
                                                SELECT data_historico,
                                                    AVG(preco_fechamento) OVER (ORDER BY data_historico ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS media_50,
                                                    AVG(preco_fechamento) OVER (ORDER BY data_historico ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS media_200
                                                FROM preco_acoes_diario
                                            ) AS subquery
                                            WHERE p.data_historico = subquery.data_historico;"""

                self.db.executa_query(query, commit=True)

                br_time = datetime.now(ZoneInfo("America/Sao_Paulo"))
                hoje_data = br_time.date()
                self.table_checker.register_populated(self,
                                                      camada='silver',
                                                      tabela='ibovespa_diario',
                                                      empresa='macro',
                                                      status=True,
                                                      data_populated=hoje_data,
                                                      observation='Carga Inicial 10 anos')

                print(
                    "Hisatórico dos valores de fechamento do Ibovespa foi criado com sucesso.")

        except Exception as e:
            self.logger.error(
                f"Não foi possível obter o fechamento do Ibovespa para {hoje_data}, erro: {e}")

    def busca_dados_macro_atuais(self, coleta_diaria):

        self.logger.info("Coletando dados diários...")

        def precisa_verificar(indicador):
            hoje = datetime.today().date()

            # Regras específicas para cada indicador
            regras = {
                "selic": hoje.weekday() in [2, 3],  # diário
                "ipca": 8 <= hoje.day <= 12,  # diário
                "dolar": True,  # Diário
                "ibovespa": True,  # Diário
                # Começa em março e espera dois meses após mudança
                "pib": hoje.month == 3 or hoje.month % 2 == 1,
                # Exemplo: verificar sempre no dia 15 de cada mês (ajustável)
                "juros_eua": hoje.day == 15,
            }

            return regras.get(indicador, False)

        try:
            # alguns dados devem ser coletados apenas ao final do dia: IPCA e SELIC por ex
            dados_atuais = {
                "selic": self._atualiza_selic(atual=True),
                "ipca": self.buscar_ipca(atual=True),
                "dolar": self.buscar_dolar(atual=True),
                "juros_eua": self.busca_juros_eua(atual=True),
                "ibovespa": self.busca_ibovespa(atual=True),
                "juros_eua": self.busca_juros_eua(atual=True)
            }

            print(dados_atuais)

            self.logger.info("Dados diários coletados com sucesso!")

        except Exception as e:
            self.logger.error(
                f"Não foi possível coletar os dados diários: {e}")

    def _to_float(self, valor):
        try:
            return float(str(valor).replace(",", "."))
        except (ValueError, TypeError):
            return None

    def _atualiza_cambio(self, par_moeda, hoje=True):

        try:
            if hoje:
                is_list = False
                many = False
                url = f"https://economia.awesomeapi.com.br/json/last/{par_moeda}?token=9c91ad4e0c552bcc5498d2ceb84f3ba60c60bdddd56fce886511979fa28b0b12"
                self.logger.info(
                    f"Iniciando coleta do fechamento do Dólar de hoje...")
            else:
                is_list = True
                many = True
                hoje_data = datetime.today()
                dias_passados = (
                    hoje_data - (hoje_data - relativedelta(years=10))).days
                start_date = hoje_data - relativedelta(years=10)
                final_date = hoje_data .strftime("%Y%m%d")
                start_date = start_date.strftime("%Y%m%d")
                url = f"https://economia.awesomeapi.com.br/json/daily/{par_moeda}/{dias_passados}?token=9c91ad4e0c552bcc5498d2ceb84f3ba60c60bdddd56fce886511979fa28b0b12&start_date={start_date}&end_date={final_date}"
                self.logger.info(
                    f"Iniciando coleta do fechamento do Dólar dos últimos 10 anos...")

            self.logger.info(f"URL gerada para o par {par_moeda}: {url}")

            api_client = APIDataParser(self.logger)

            df_cambio = api_client.get_from_api(
                url, ['high', 'low', 'varBid', 'pctChange',
                      'bid', 'ask', 'timestamp'],
                is_list=is_list,
                convert_timestamp=True,
                sanitize=True,
                frequency='daily')

            df_cambio.sort_values("data", inplace=True)

            df_cambio = df_cambio.drop_duplicates(
                subset=["data"], keep="last"
            )

            colunas_numericas = [
                "high", "low", "bid", "ask", "varBid", "pctChange",
                "var_bid", "pct_change", "timestamp"
            ]

            for col in colunas_numericas:
                if col in df_cambio.columns:
                    df_cambio[col] = df_cambio[col].apply(self._to_float)

            df_cambio.sort_values("data", inplace=True)
            df_cambio["preco_medio"] = (
                df_cambio["high"] + df_cambio["low"]) / 2
            df_cambio["spread"] = df_cambio["ask"] - df_cambio["bid"]
            df_cambio["amplitude_pct"] = (
                (df_cambio["high"] - df_cambio["low"]) / df_cambio["low"]) * 100
            df_cambio["fechamento_anterior"] = df_cambio["bid"].shift(1)
            df_cambio["var_dia_real"] = df_cambio["bid"] - \
                df_cambio["fechamento_anterior"]
            df_cambio["var_dia_pct"] = (
                (df_cambio["bid"] - df_cambio["fechamento_anterior"]) / df_cambio["fechamento_anterior"]) * 100

            query = """INSERT INTO silver.cambio_diario (data, par_moeda, bid, ask,
                        high, low, var_bid, pct_change, preco_medio, spread, 
                        amplitude_pct, fechamento_anterior, var_dia_real, var_dia_pct)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            valores = [
                (row["data"].date(),
                 par_moeda,
                 self._to_float(row["bid"]),
                 self._to_float(row["ask"]),
                 self._to_float(row["high"]),
                 self._to_float(row["low"]),
                 self._to_float(row.get("var_bid") or row.get("varBid")),
                 self._to_float(row.get("pct_change") or row.get("pctChange")),
                 self._to_float(row["preco_medio"]),
                 self._to_float(row["spread"]),
                 self._to_float(row["amplitude_pct"]),
                 self._to_float(row["fechamento_anterior"]),
                 self._to_float(row["var_dia_real"]),
                 self._to_float(row["var_dia_pct"]))
                for _, row in df_cambio.iterrows()]

            self.db.executa_query(query, valores, commit=True, many=many)

            self.logger.info(
                f"Coleta do par {par_moeda} obtida e gravada com sucesso.")

        except Exception as e:
            self.logger.error(
                f"Erro ao obter câmbio {par_moeda}: {e}")

    def _atualiza_sgs_bacen(self, codigo_sgs=None, hoje=True, camada=None, tabela=None):

        if codigo_sgs is None:
            raise TypeError(
                "O parâmetro 'codigo_sgs' deve ser informado, não deve ser None")

        if camada is None:
            raise TypeError(
                "O parâmetro 'camada' deve ser informado, não deve ser None")

        if tabela is None:
            raise TypeError(
                "O parâmetro 'tabela' deve ser informado, não deve ser None")

        SERIES = {
            11: "SELIC Diária",
            433: "IPCA Mensal",
            12: "CDI Diária",
            189: "IGP-M Mensal",
            10844: "IPCA Serviços",
            1: "Taxa de Câmbio USD (PTAX)",
            10880: "Reservas Internacionais"
        }

        nome_serie = SERIES.get(codigo_sgs, f"SGS {codigo_sgs}")

        try:
            if hoje:
                only_date = datetime.today()
                http_get_timeout = 10
                pop_string = f"Atualização de {nome_serie}"
                only_date = only_date.strftime("%d/%m/%Y")

                url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_sgs}/dados?formato=json&dataInicial={only_date}&dataFinal={only_date}"

                self.logger.info(
                    f"Iniciando coleta da {nome_serie} atual")
            else:
                hoje_data = datetime.today()
                http_get_timeout = 150
                pop_string = f"Carga inicial: {nome_serie}"
                start_date = hoje_data - relativedelta(years=10)
                final_date = hoje_data .strftime("%d/%m/%Y")
                start_date = start_date.strftime("%d/%m/%Y")

                url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_sgs}/dados?formato=json&dataInicial={start_date}&dataFinal={final_date}"

                self.logger.info(
                    f"Iniciando coleta da {nome_serie} dos últimos 10 anos.")

            self.logger.info(f"URL gerada para {nome_serie}: {url}")

            api_client = APIDataParser(self.logger)

            df_indice = api_client.get_from_api(
                url, ['data', 'valor'], is_list=True, convert_timestamp=False, sanitize=True, frequency='daily', http_get_timeout=http_get_timeout)

            if df_indice is not None and not df_indice.empty:
                nome_tabela = f"{camada}.{tabela}"
                query = f"INSERT INTO {nome_tabela} (data, valor) VALUES (%s, %s);"
                valores = [
                    (row["data"].date(), self._to_float(row["valor"]))
                    for _, row in df_indice.iterrows()
                    if self._to_float(row["valor"]) is not None
                ]

                self.db.executa_query(query, valores, commit=True, many=True)

                self.logger.info(
                    f"{nome_serie} histórico obtido e gravado com sucesso.")

                self.table_checker.register_populated(
                    camada=camada,
                    tabela=tabela,
                    empresa='macro',
                    status=True,
                    data_populated=datetime.today().date(),
                    observation=f"Atualização de {pop_string}"
                )

        except Exception as e:
            self.logger.error(
                f"Erro ao obter dados do {nome_serie}: {e}")
            return None

    def busca_pib(self, atual=True):
        try:
            url = "https://servicodados.ibge.gov.br/api/v3/agregados/5932/periodos/all"
            response = requests.get(url)
            dados_pib = response.json()

            self.logger.info(f"PIB obtido com sucesso.")

            return dados_pib

        except Exception as e:
            self.logger.error(
                f"Erro ao obter o PIB: {e}")
            return None

    def _atualiza_juros_eua(self, serie=None, hoje=True, camada=None, tabela=None):
        try:
            if hoje:
                start_date = datetime.today()
                only_date = start_date.strftime("%Y-%m-%d")
                http_get_timeout = 10
                pop_string = f"atualizacao: {serie}"
                url = f"https://api.stlouisfed.org/fred/series_observations?series_id={serie}&api_key=f308c54585d765845e4c89ca7a010c3a&file_type=json&observation_start={only_date}&observation_end={only_date}"
            else:
                hoje = datetime.today()
                final_date = hoje.strftime("%Y-%m-%d")
                start_date = hoje - relativedelta(years=10)
                start_date = start_date.strftime("%Y-%m-%d")
                http_get_timeout = 150
                pop_string = f"Carga inicial: {serie}"
                url = f"https://api.stlouisfed.org/fred/series_observations?series_id={serie}&api_key=f308c54585d765845e4c89ca7a010c3a&file_type=json&observation_start={start_date}&observation_end={final_date}"

            self.logger.info(f"URL gerada para Juros EUA ({serie}): {url}")

            api_client = APIDataParser(self.logger)

            df_juros = api_client.get_from_api(
                url,
                ['date', 'value'],
                is_list=True,
                convert_timestamp=False,
                sanitize=True,
                frequency='monthly',
                http_get_timeout=http_get_timeout,
                data_key='observations',
                col_freq='date',
                date_format='%d/%m/%Y')

            df_juros.sort_values("date", inplace=True)

            df_juros = df_juros.drop_duplicates(
                subset=["date"], keep="last"
            )

            if df_juros is not None and not df_juros.empty:
                nome_tabela = f"{camada}.{tabela}"
                query = f"INSERT INTO {nome_tabela} (data, valor) VALUES (%s, %s);"
                valores = [
                    (row["date"].date(), self._to_float(row["value"]))
                    for _, row in df_juros.iterrows()
                    if self._to_float(row["value"]) is not None
                ]

                self.db.executa_query(query, valores, commit=True, many=True)

                self.logger.info(
                    f"{serie} histórico obtido e gravado com sucesso.")

                self.table_checker.register_populated(
                    camada=camada,
                    tabela=tabela,
                    empresa='macro',
                    status=True,
                    data_populated=datetime.today().date(),
                    observation=f"Atualização de {pop_string}"
                )

            self.logger.info(f"Juros ({serie}) EUA obtido com sucesso.")

        except Exception as e:
            self.logger.error(
                f"Erro ao obter os Juros ({serie}) EUA: {e}")
            return None

    def sentimento_financeiro(self, ticker="EMBR3.SA"):
        try:
            ativo = yf.Ticker(ticker)
            historico = ativo.history(period="1mo")

            variacao_pct = (
                (historico["Close"].iloc[-1] / historico["Close"].iloc[-10]) - 1) * 100
            volume_medio = historico["Volume"].mean()

            if variacao_pct > 5 and volume_medio > historico["Volume"].iloc[-10]:
                sentimento = "Positivo"
            elif variacao_pct < -5 and volume_medio > historico["Volume"].iloc[-10]:
                sentimento = "Negativo"
            else:
                sentimento = "Neutro"

            self.logger.info(f"Sentimento Financeiro obtido com sucesso.")

            return sentimento

        except Exception as e:
            self.logger.error(
                f"Erro ao obter o Sentimento Financeiro para{ticker}: {e}")
            return None

    def busca_valores_intra_diarios(inicio=None, fim=None):
        """
        Baixa os dados históricos da ação EMBR3 do site Investing.com.

        Parameters
        ----------
        inicio : str
            Data de início no formato 'dd/mm/yyyy'.
        fim : str ou None
            Data final no formato 'dd/mm/yyyy'. Se None, usa a data atual.

        Returns
        -------
        pd.DataFrame
            DataFrame com colunas como Data, Último, Abertura, Máxima, Mínima, Var%
        """
        if inicio is None:
            hoje = datetime.datetime.today()
            inicio = hoje - relativedelta(years=10)
            inicio = inicio.strftime("%d/%m/%Y")
        if fim is None:
            fim = datetime.datetime.today().strftime("%d/%m/%Y")

        url_calendario = (
            "https://www.investing.com/instruments/HistoricalDataAjax"
        )
        headers = {
            "User-Agent": "Mozilla/5.0",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.investing.com/equities/embraer-on-nm-historical-data",
        }

        payload = {
            "curr_id": 10409,
            "smlID": 205086,
            "header": "Embraer ON Histórico de Dados",
            "st_date": inicio,
            "end_date": fim,
            "interval_sec": "Daily",
            "sort_col": "date",
            "sort_ord": "DESC",
            "action": "historical_data"
        }

        response = requests.post(url_calendario, headers=headers, data=payload)
        df = pd.read_html(response.text, thousands='.', decimal=',')[0]

        # Tratamento e ordenação da tabela
        df['Data'] = pd.to_datetime(df['Data'], format='%d.%m.%Y')
        df.sort_values('Data', inplace=True)
        df.reset_index(drop=True, inplace=True)

        return df

    def busca_valores_fechamento(self, days=1):

        # Coletar preços históricos (somente na primeira execução)
        self.logger.info("Vericando presença de preços históricos... ")

        try:
            for sigla, empresa in self.ls_empresas:

                # primeiro o dólar diário
                if not self.table_checker.check_populated(camada='silver', tabela='dolar_diario', empresa=sigla, resposta='bool'):
                    query = "SELECT COUNT(*) FROM preco_acoes_diario;"
                    dados_dolar = self.db.fetch_data(query, tipo_fetch="one")

                    if dados_dolar and dados_dolar[0] == 0:
                        pass  # criando...

                # fechamento bolsa
                if not self.table_checker.check_populated(camada='silver', tabela='ibovespa_diario', empresa=sigla, resposta='bool'):
                    query = "SELECT COUNT(*) FROM preco_acoes_diario;"
                    dados_bolsa = self.db.fetch_data(query, tipo_fetch="one")

                    # Se o banco estiver vazio
                    if dados_bolsa and dados_bolsa[0] == 0:

                        self.logger.info(
                            f"Não encontrados para a sigla: {sigla}")

                        acao = yf.Ticker(sigla)

                        historico = acao.history(period="10y")
                        historico.index = pd.to_datetime(historico.index)
                        historico.reset_index(inplace=True)
                        print(historico.head())
                        # print(historico.info())

                        for date, row in historico.iterrows():
                            query = """INSERT INTO ibovespa_diario (
                                                cod_bolsa,
                                                data_historico,
                                                preco_abertura,
                                                preco_minimo,
                                                preco_maximo,
                                                preco_fechamento,
                                                volume_negociado,
                                                media_movel_50,
                                                media_movel_200)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                            valores = ("EMBR3.SA",
                                       row["Date"].date(),
                                       float(row["Open"]),
                                       float(row["Low"]),
                                       float(row["High"]),
                                       float(row["Close"]),
                                       int(row["Volume"]) if not pd.isna(
                                           row["Volume"]) else 0,
                                       None,
                                       None)

                            self.db.executa_query(query, valores, commit=True)

                        self.logger.info(
                            f"Dados encontrados e inseridos para: 'EMBR3.SA'")

                        self.logger.info(f"Calculando médias móveis...")

                        query = """UPDATE preco_acoes_diario AS p
                                            SET media_movel_50 = COALESCE(subquery.media_50, p.media_movel_50),
                                                media_movel_200 = COALESCE(
                                                    subquery.media_200, p.media_movel_200)
                                            FROM (
                                                SELECT data_historico,
                                                    AVG(preco_fechamento) OVER (ORDER BY data_historico ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS media_50,
                                                    AVG(preco_fechamento) OVER (ORDER BY data_historico ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS media_200
                                                FROM preco_acoes_diario
                                            ) AS subquery
                                            WHERE p.data_historico = subquery.data_historico;"""

                        self.db.executa_query(query, commit=True)

                        self.logger.info(
                            f"Médias móveis calculadas com sucesso.")
                    else:
                        self.logger.info(f"Preços históricos já presentes.")

        except Exception as e:
            self.logger.error(
                f"Houve um problema ao adquirir os dados históricos: {e}")
            raise

        self.logger.info(
            f"Consulta de histórico de valores finalizada com sucesso.")

    def busca_cotacao_atual(self, hora_abertura_bolsa,
                            hora_fechamento_bolsa,
                            dolar_inicio,
                            dolar_fim):

        self.logger.info(
            "Verificando o horários para cotação atual...")

        hora_atual = datetime.now().time()

        if hora_abertura_bolsa <= hora_atual <= hora_fechamento_bolsa:
            self.logger.info(
                "Dentro do horário da BOVESPA: Iniciando a coleta de informações...")

            # **Obter cotações atual**
            self.logger.info(f"Obtendo a cotaçao na bolsa...")

            acao = yf.Ticker("EMBR3.SA")
            preco = float(acao.history(period="1d")["Close"].iloc[-1])

            scraper = ScrappMacro(
                logger=self.logger,
                db=self.db,
                conn=self.conn,
                cursor=self.cursor)
            ibovespa = scraper.busca_ibovespa()

            query = "INSERT INTO precos_embraer_pregao (data_historico, cod_bolsa, preco_acao, ibovespa) VALUES (%s,%s,%s,%s)"
            valores = (datetime.now(), 'EMBR3.SA', preco, ibovespa)

            try:
                self.db.executa_query(query, valores, commit=True)

                self.logger.info(f"Dados da BOVESPA gravados para: 'EMBR3.SA'")

            except Exception as e:
                self.logger.error(
                    f"Houve um problema ao obter os valores na bolsa: {e}")
                raise

        else:
            self.logger.info(
                "Fora do horário de funcionamento da Bolsa de Valores.")

        if dolar_inicio <= hora_atual <= dolar_fim:

            self.logger.info(
                "Dentro do horário: Iniciando a coleta da cotação do Dólar...")

            # **Obter cotações atual**
            self.logger.info(f"Obtendo a cotaçao do Dólar...")

            scraper = ScrappMacro(
                logger=self.logger,
                db=self.db,
                conn=self.conn,
                cursor=self.cursor)

            df_dolar = scraper.buscar_dolar(atual=True)

            self.logger.info(f"Salvando a cotaçao do Dólar...")

            try:
                # Garante que as colunas estão na ordem correta
                colunas = ['data', 'bid', 'ask', 'high',
                           'low', 'varBid', 'pctChange']
                df_insert = df_dolar[colunas].copy()

                df_insert['data'] = pd.to_datetime(df_insert['data'])

                valores = tuple(df_dolar.iloc[0][[
                                'data', 'bid', 'ask', 'high', 'low', 'varBid', 'pctChange']])

                query = """
                INSERT INTO dolar_diario (
                    data_historico, bid, ask, high, low, varBid, pctChange
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """

                self.db.executa_query(query, valores, commit=True)

                self.logger.info(f"Dados do Dolar gravados com sucesso.")

            except Exception as e:
                self.logger.error(
                    f"Houve um problema ao salvar os valores do Dólar: {e}")
                raise

        else:
            self.logger.info(
                "Fora do horário de coleta do Dólar.")
