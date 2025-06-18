import requests
import yfinance as yf
from datetime import datetime, timedelta


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

    def verifica_tabelas(self):
        query = """
            CREATE TABLE IF NOT EXISTS macroeconomia (
                data DATE PRIMARY KEY,
                cod_bolsa VARCHAR(10) NOT NULL,
                selic DECIMAL(5,2),
                ipca DECIMAL(5,2),
                pib DECIMAL(10,2),
                juros_eua DECIMAL(5,2),
                dolar_fechamento DECIMAL(10,2),
                ibovespa_fechamento DECIMAL(10,2)
            );
        """

        self.db.executa_query(query, commit=True)

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
                    "dolar": self.buscar_dolar(days=3650),
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
                "selic": hoje.weekday() in [2, 3],  # Quartas e quintas-feiras
                "ipca": 8 <= hoje.day <= 12,  # Entre dias 8 e 12 de cada mês
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
                "dolar": self.buscar_dolar(days=1),
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
        # URL da API do Banco Central parao IPCA

        try:

            if atual:
                only_date = datetime.today()
                only_date = only_date.strftime("%d/%m/%Y")
                url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados?formato=json&dataInicial=" + \
                    only_date+"&dataFinal="+only_date
            else:
                final_date = datetime.today()
                start_date = final_date - timedelta(days=10*365)
                final_date = final_date.strftime("%d/%m/%Y")
                start_date = start_date.strftime("%d/%m/%Y")
                url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados/?formato=json&dataInicial=" + \
                    start_date+"&dataFinal="+final_date

            response = requests.get(url)
            dados = response.json()

            self.logger.info(f"IPCA: {dados}")

            self.logger.info(f"IPCA obtido com sucesso.")

            return dados

        except Exception as e:
            self.logger.error(
                f"Erro ao obter o IPCA: {e}")
            return None

    # URL da API para taxa de câmbio
    def buscar_dolar(self, days=1):

        try:
            if days == 1:
                url = "https://economia.awesomeapi.com.br/json/last/USD-BRL"
            else:
                url = "https://economia.awesomeapi.com.br/json/daily/USD-BRL/3650"

            response = requests.get(url)
            dados = response.json()

            self.logger.info(f"Dólar obtido com sucesso.")

            return dados

        except Exception as e:
            self.logger.error(
                f"Erro ao obter o Dólar: {e}")
            return None

    def buscar_selic(self, atual=True):
        # URL da API do Banco Central para a taxa Selic
        try:
            if atual:
                only_date = datetime.today()
                only_date = only_date.strftime("%d/%m/%Y")
                url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial=" + \
                    only_date+"&dataFinal="+only_date
            else:
                final_date = datetime.today()
                start_date = final_date - timedelta(days=10*365)
                final_date = final_date.strftime("%d/%m/%Y")
                start_date = start_date.strftime("%d/%m/%Y")
                url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados/?formato=json&dataInicial=" + \
                    start_date+"&dataFinal="+final_date

            self.logger.info(f"URL: {url}")
            response = requests.get(url)
            dados = response.json()

            self.logger.info(f"SELIC obtido com sucesso.")

            self.logger.info(f"SELIC: {dados}")

            return dados

        except Exception as e:
            self.logger.error(
                f"Erro ao obter a SELIC: {e}")
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
                url = f"https://api.stlouisfed.org/fred/series_observations?series_id=FEDFUNDS&api_key={'f308c54585d765845e4c89ca7a010c3a'}&file_type=json&observation_start={only_date}&observation_end={only_date}"
            else:
                hoje = datetime.today()
                final_date = hoje.strftime("%Y-%m-%d")
                start_date = hoje - timedelta(years=10)
                start_date = start_date.strftime("%Y-%m-%d")
                url = f"https://api.stlouisfed.org/fred/series_observations?series_id=FEDFUNDS&api_key={'f308c54585d765845e4c89ca7a010c3a'}&file_type=json&observation_start={start_date}&observation_end={final_date}"

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
