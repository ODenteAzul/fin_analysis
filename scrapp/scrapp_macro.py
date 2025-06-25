import requests
import yfinance as yf
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from utils.api_client import APIDataParser
from dateutil.relativedelta import relativedelta
import pandas as pd


class ScrappMacro():
    def __init__(self,
                 logger,
                 db,
                 conn,
                 cursor):
        self.logger = logger
        self.db = db
        self.conn = conn
        self.cursor = cursor

    def busca_histórico_macroeconomia(self):

        self.logger.info(
            "Verificando a presença de dados históricos de macro economia...")

        try:
            query = "SELECT COUNT(*) FROM macroeconomia;"
            dados = self.db.fetch_data(query, tipo_fetch="one")

            if dados and dados[0] == 0:

                self.logger.info(
                    "Dados ausentes, buscando informações...")

                dados_historicos = {
                    "selic": self.buscar_selic(atual=False),
                    "ipca": self.buscar_ipca(atual=False),
                    "dolar": self.buscar_dolar(atual=False),
                    "juros_eua": self.busca_juros_eua(atual=False)
                }

                print(dados_historicos)

            self.logger.info(
                "Coleta de dados históricos efetuada com sucesso.")

        except Exception as e:
            self.logger.error(
                f"Não foi possível obter o histórico de dados macro econômicos: {e}")

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
                "selic": self.buscar_selic(atual=True),
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

    def buscar_ipca(self, atual=True):
        # URL da API do Banco Central para o IPCA -> sgs code 433

        try:
            if atual:
                only_date = datetime.today()
                only_date = only_date.strftime("%d/%m/%Y")
                url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados?formato=json&dataInicial=" + \
                    only_date+"&dataFinal="+only_date
                self.logger.info(
                    f"Iniciando coleta do IPCA atual.")
            else:
                hoje = datetime.today()
                start_date = hoje - relativedelta(years=10)
                final_date = hoje.strftime("%d/%m/%Y")
                start_date = start_date.strftime("%d/%m/%Y")
                url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados/?formato=json&dataInicial=" + \
                    start_date+"&dataFinal="+final_date
                self.logger.info(
                    f"Iniciando coleta do IPCA dos últimos 10 anos.")

            self.logger.info(f"URL gerada para IPCA: {url}")

            if atual:
                http_get_timeout = 10
            else:
                http_get_timeout = 150

            api_client = APIDataParser(self.logger)

            df_ipca = api_client.get_from_api(
                url, ['data', 'valor'], is_list=True, convert_timestamp=False, sanitize=True, frequency='daily', http_get_timeout=http_get_timeout),

            self.logger.info(f"IPCA obtido com sucesso.")

            return df_ipca

        except Exception as e:
            self.logger.error(
                f"Erro ao obter dados do IPCA: {e}")
            return None

    # URL da API para taxa de câmbio
    def buscar_dolar(self, atual=True):

        try:
            if atual:
                url = "https://economia.awesomeapi.com.br/json/last/USD-BRL"
                self.logger.info(
                    f"Iniciando coleta do Dólar atual")
            else:
                dias_passados = (
                    datetime.today() - (datetime.today() - relativedelta(years=10))).days
                url = f"https://economia.awesomeapi.com.br/json/daily/USD-BRL/{dias_passados}"
                self.logger.info(
                    f"Iniciando coleta do Dólar dos últimos 10 anos.")

            self.logger.info(f"URL gerada para o Dólar: {url}")

            api_client = APIDataParser(self.logger)
            if atual:
                is_list = False
            else:
                is_list = True

            df_dolar = api_client.get_from_api(
                url, ['high', 'low', 'varBid', 'pctChange', 'bid', 'ask', 'timestamp'], is_list=is_list, convert_timestamp=True, sanitize=True, frequency='daily')

            self.logger.info(f"Cotação do Dólar obtido com sucesso.")

            return df_dolar

        except Exception as e:
            self.logger.error(
                f"Erro ao obter dados do Dólar: {e}")
            return None

    def buscar_selic(self, atual=True):
       # URL da API do Banco Central para o SELIC -> sgs code 11

        try:
            if atual:
                only_date = datetime.today()
                only_date = only_date.strftime("%d/%m/%Y")
                url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial=" + \
                    only_date+"&dataFinal="+only_date
                self.logger.info(
                    f"Iniciando coleta da SELIC atual")
            else:
                hoje = datetime.today()
                start_date = hoje - relativedelta(years=10)
                final_date = hoje.strftime("%d/%m/%Y")
                start_date = start_date.strftime("%d/%m/%Y")
                url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial=" + \
                    start_date+"&dataFinal="+final_date
                self.logger.info(
                    f"Iniciando coleta da SELIC dos últimos 10 anos.")

            self.logger.info(f"URL gerada para SELIC: {url}")

            api_client = APIDataParser(self.logger)

            if atual:
                http_get_timeout = 10
            else:
                http_get_timeout = 150

            df_selic = api_client.get_from_api(
                url, ['data', 'valor'], is_list=True, convert_timestamp=False, sanitize=True, frequency='daily', http_get_timeout=http_get_timeout),

            self.logger.info(f"SELIC obtido com sucesso.")

            return df_selic

        except Exception as e:
            self.logger.error(
                f"Erro ao obter dados do SELIC: {e}")
            return None

    def busca_ibovespa(self, atual=True):
        try:
            ibovespa = yf.Ticker("^BVSP")
            if atual:
                valor = ibovespa.history(period="1d")
                valor = float(ibovespa.history(period="1d")["Close"].iloc[-1])
            else:
                valor = ibovespa.history(period="10y")

            self.logger.info(f"BVSP obtido com sucesso.")

            return valor
        except Exception as e:
            self.logger.error(
                f"Erro ao obter a BVSP: {e}")
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

    def busca_juros_eua(self, atual=True):
        try:
            if atual:
                start_date = datetime.today()
                only_date = start_date.strftime("%Y-%m-%d")
                url = "https://api.stlouisfed.org/fred/series_observations?series_id=FEDFUNDS&api_key=f308c54585d765845e4c89ca7a010c3a&file_type=json&observation_start=" + \
                    only_date+"&observation_end="+only_date
            else:
                hoje = datetime.today()
                final_date = hoje.strftime("%Y-%m-%d")
                start_date = hoje - timedelta(years=10)
                start_date = start_date.strftime("%Y-%m-%d")
                url = "https://api.stlouisfed.org/fred/series_observations?series_id=FEDFUNDS&api_key=f308c54585d765845e4c89ca7a010c3a&file_type=json&observation_start=" + \
                    start_date+"&observation_end="+final_date

            response = requests.get(url)
            dados_fed = response.json()

            self.logger.info(f"Juros EUA obtido com sucesso.")

            return dados_fed

        except Exception as e:
            self.logger.error(
                f"Erro ao obter os Juros EUA: {e}")
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
