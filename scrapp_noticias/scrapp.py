import requests
import yfinance as yf
import pandas as pd
import numpy as np
from gdeltdoc import GdeltDoc, Filters
import re
from fuzzywuzzy import fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from datetime import datetime
from bs4 import BeautifulSoup
from scrapp_valores.scrapp_macro import ScrappMacro


class Scrapping():
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

        self.logger.info(
            "Verificando a presença das tabelas de scrapping...")

        query = """
            CREATE TABLE IF NOT EXISTS preco_acoes_diario (
                id SERIAL PRIMARY KEY,
                cod_bolsa VARCHAR(10) NOT NULL,
                data_historico DATE NOT NULL,
                preco_abertura NUMERIC(10,2),
                preco_minimo NUMERIC(10,2),
                preco_maximo NUMERIC(10,2),
                preco_fechamento NUMERIC(10,2),
                volume_negociado BIGINT,
                media_movel_50 NUMERIC(10,2),
                media_movel_200 NUMERIC(10,2),
                UNIQUE (cod_bolsa, data_historico)
            );
        """

        self.db.executa_query(query, commit=True)

        query = """
            CREATE TABLE IF NOT EXISTS noticias (
                id SERIAL PRIMARY KEY,
                cod_bolsa TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                titulo TEXT,
                descricao TEXT,
                data_historico DATE NOT NULL,
                url TEXT,
                sentimento TEXT);
        """

        self.db.executa_query(query, commit=True)

        query = """
            CREATE TABLE IF NOT EXISTS precos_embraer_pregao (
                id SERIAL PRIMARY KEY,
                cod_bolsa TEXT,
                data_historico TIMESTAMP NOT NULL,
                preco_acao DECIMAL(10,2),
                ibovespa DECIMAL(10,2));
        """

        self.db.executa_query(query, commit=True)

        query = """
            CREATE TABLE IF NOT EXISTS dolar_diario (
                id SERIAL PRIMARY KEY,
                data_historico TIMESTAMP NOT NULL,
                bid DECIMAL(10,4),
                ask DECIMAL(10,4),
                high DECIMAL(10,4),
                low DECIMAL(10,4),
                varBid DECIMAL(10,4),
                pctChange DECIMAL(10,4));
        """

        self.db.executa_query(query, commit=True)

        self.logger.info("Tabelas verificadas.")

    def converter_para_nativo(df):
        """
        Converte colunas numéricas do DataFrame para tipos nativos do Python (float).
        """
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for coluna in numeric_cols:
            # Aplicando a conversão para cada valor da coluna
            df[coluna] = df[coluna].apply(lambda x: float(x))

        return df

    def titulos_sao_similares(self, titulo1, titulo2, limite=60):
        try:
            if isinstance(titulo1, str) and isinstance(titulo2, str):
                return fuzz.ratio(titulo1.lower(), titulo2.lower()) > limite

            return False

        except Exception as e:
            self.logger.error(
                f"Houve um problema ao testar a similaridades de títulos de notícias: {e}")
            raise

    def verificar_similaridade(self, noticia1, noticia2):
        try:
            vetorizer = TfidfVectorizer()
            vetorizer = TfidfVectorizer(stop_words=None)
            matriz = vetorizer.fit_transform([noticia1, noticia2])
            similaridade = cosine_similarity(matriz[0], matriz[1])

            return similaridade[0][0]

        except Exception as e:
            self.logger.error(
                f"Houve um problema ao testar a similaridades do texto das notícias: {e}")
            raise

    def verificar_relevancia(self, texto, palavra="Embraer", limite=0.005):
        try:
            if isinstance(texto, str):
                total_palavras = len(texto.split())
                freq_relativa = texto.lower().count(palavra.lower()) / max(total_palavras, 1)

                return freq_relativa < limite

        except Exception as e:
            self.logger.error(
                f"Houve um problema aoverificar a relevância da notícia {e}")
            raise

    def verificar_noticia(self, titulo, texto, noticias_salvas):
        for noticia in noticias_salvas:
            if texto == "texto_indisponivel":
                if self.titulos_sao_similares(titulo, noticia["titulo"]) or self.verificar_relevancia(titulo):
                    self.logger.info(
                        "Notícia duplicada ou não relevante encontrada. Ignorando...")
                    return False
            else:
                if isinstance(noticia["titulo"], str) and isinstance(noticia["texto"], str):
                    if self.titulos_sao_similares(titulo, noticia["titulo"]) or self.verificar_similaridade(texto, noticia["texto"]) > 0.70 or self.verificar_relevancia(texto):
                        self.logger.info(
                            "Notícia duplicada ou não relevante encontrada. Ignorando...")
                        return False
                else:
                    self.logger.info(
                        "Notícia com má formação ou incorreta...")
                    return False

        self.logger.info("Nova notícia armazenada.")
        return True

    def busca_valores_fechamento(self, days=1):

        # **Coletar preços históricos (somente na primeira execução)**
        self.logger.info("Vericando presença de preços históricos... ")

        try:
            query = "SELECT COUNT(*) FROM preco_acoes_diario;"
            dados = self.db.fetch_data(query, tipo_fetch="one")

            if dados and dados[0] == 0:  # Se o banco estiver vazio

                self.logger.info(f"Não encontrados para a empresa 'EMBR3.SA'")

                acao = yf.Ticker("EMBR3.SA")

                historico = acao.history(period="10y")
                historico.index = pd.to_datetime(historico.index)
                historico.reset_index(inplace=True)
                print(historico.head())
                # print(historico.info())

                for date, row in historico.iterrows():
                    query = """INSERT INTO preco_acoes_diario (
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

                self.logger.info(f"Médias móveis calculadas com sucesso.")
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
            "Verificando o horários...")

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
            dolar = scraper.buscar_dolar()

            query = "INSERT INTO dolar_diario (data_historico, bid, ask, high, low, varBid, pctChange) VALUES (%s,%s,%s,%s,%s,%s,%s)"
            valores = (datetime.now(), dolar["USDBRL"]["bid"], dolar["USDBRL"]["ask"], dolar["USDBRL"]
                       ["high"], dolar["USDBRL"]["low"], dolar["USDBRL"]["varBid"], dolar["USDBRL"]["pctChange"])

            try:
                self.db.executa_query(query, valores, commit=True)

                self.logger.info(f"Dados do Dolar gravados.")

            except Exception as e:
                self.logger.error(
                    f"Houve um problema ao obter os valores na bolsa: {e}")
                raise

        else:
            self.logger.info(
                "Fora do horário de coleta do Dólar.")

    def obter_texto_noticia(self, url):

        if not isinstance(url, str):
            self.logger.error(f"Erro: URL inválida recebida {url}")
            return ""

        html = None

        texto_noticia = ""

        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
            response = requests.get(url, headers=headers)

            if response.status_code == 403 or response.status_code == 429:
                raise Exception("Acesso bloqueado, tentando Selenium...")

            soup = BeautifulSoup(response.text, "html.parser")
            container = soup.find("div", class_="article-content")
            article_body = container.find_all("p") if container else []

            if article_body:
                texto_noticia = " ".join([p.get_text() for p in article_body])
            else:
                return "texto_indisponivel"

        except Exception as e:
            print(f"Erro: {e}")

        #    options = webdriver.ChromeOptions()
        #    options.add_argument("--headless")  # Executa sem interface gráfica
        #    options.add_argument("--disable-gpu")  # Bloqueia uso da GPU
            # Evita fallback problemático
        #    options.add_argument("--disable-software-rasterizer")
            # Remove possíveis conflitos de extensões
        #    options.add_argument("--disable-extensions")
            # Corrige problemas de memória compartilhada
        #    options.add_argument("--disable-dev-shm-usage")
        #    options.add_argument("--disable-popup-blocking")
        #    options.add_argument("--ignore-certificate-errors")
        #    options.add_argument(
         #       "--disable-blink-features=AutomationControlled")

         #   driver = webdriver.Chrome(options=options)
        #    driver.get(url)
        #    try:
        #        WebDriverWait(driver, 3).until(
        #            # Confirme o nome correto da classe
        #            EC.presence_of_element_located(
       #                 (By.CLASS_NAME, "article-content"))
       #         )
            # Pega HTML após renderização
        #        html = driver.execute_script(
        #            "return document.documentElement.outerHTML")
        #    except Exception as e:
        #        self.logger.error(
         #           f"Erro ao esperar carregamento via Selenium: {e}")
        # html = None
        #    driver.quit()

        #    if not html or html.strip() == "":
        #        self.logger.error(
        #            "Erro crítico: HTML vazio. Nenhum conteúdo extraído.")
        #        return " "

         # soup = BeautifulSoup(html, "html.parser")
          #  container = soup.find("div", class_="article-content")
          #  if not container:
            # Testa variação de nome do elemento
         #       container = soup.find("div", class_="post-content")

         #   article_body = container.find_all("p") if container else []#3

        #    texto_noticia = " "  # Inicializa com um valor seguro

        #    if article_body:
        #        texto_noticia = " ".join(
        #            [p.get_text() for p in article_body]) if article_body else " "

         #   texto_noticia = texto_noticia.strip() if texto_noticia else " "

        # Limpeza do texto extraído
        if texto_noticia:
            # Remove espaços extras
            texto_noticia = re.sub(r"\s+", " ", texto_noticia)
            texto_noticia = re.sub(
                r"\b(Publicado em|Fonte|Leia mais em|Promoção|Todos os Direitos Reservados)\b.*", "", texto_noticia)
            # Remove caracteres especiais
            texto_noticia = re.sub(r"[^a-zA-Z0-9À-ÿ\s]", "", texto_noticia)

        if not texto_noticia.strip():
            self.logger.error(
                "Texto está vazio ou contém apenas stop words. Ignorando processamento.")
            return "texto_indisponivel"

        return texto_noticia

    def busca_noticias_historicas(self):

        # **Coletar notícias históricas (somente na primeira execução)**
        self.logger.info("Vericando presença de notícias históricas... ")

        try:
            query = "SELECT COUNT(*) FROM noticias;"
            dados = self.db.fetch_data(query, tipo_fetch="one")

            if dados and dados[0] == 0:

                self.logger.info(f"Buscando notícias inicias para: 'EMBR3.SA'")

                data_inicial = datetime.today().replace(day=1)
                data_final = data_inicial.replace(year=data_inicial.year - 10)
                while data_inicial >= data_final:
                    start_date = data_inicial.strftime("%Y-%m-%d")
                    from calendar import monthrange
                    end_date = data_inicial.replace(day=monthrange(
                        data_inicial.year, data_inicial.month)[1]).strftime("%Y-%m-%d")

                    self.logger.info(
                        f"Buscando notícias de {start_date} até {end_date}")

                    filtros = Filters(
                        keyword="Embraer",
                        start_date=start_date,
                        end_date=end_date,
                        num_records=100,
                        language='portuguese'
                    )

                    # Criar instância do cliente GDELT
                    gdelt = GdeltDoc()
                    # Buscar artigos que correspondem aos filtros
                    artigos = gdelt.article_search(filtros)

                    noticias_salvas = []
                    # Exibir os resultados
                    for artigo in artigos.itertuples():
                        noticia = {}

                        texto_noticia = self.obter_texto_noticia(artigo.url)

                        if artigo.title:
                            if texto_noticia.strip() and texto_noticia != "texto_indisponivel":
                                if isinstance(artigo.title, str) and isinstance(texto_noticia, str) and texto_noticia != "":
                                    if self.verificar_noticia(artigo.title, texto_noticia, noticias_salvas):
                                        self.logger.info(
                                            f"Título: {artigo.title}")
                                        noticia = {
                                            "titulo": artigo.title,
                                            "texto": texto_noticia
                                        }

                                        query = "INSERT INTO noticias (cod_bolsa, titulo, descricao, data_historico, url) VALUES (%s, %s, %s, %s, %s)"
                                        valores = ('EMBR3.SA', artigo.title, texto_noticia, datetime.strptime(
                                            artigo.seendate, '%Y%m%dT%H%M%SZ').date(), artigo.url)

                                        try:
                                            self.db.executa_query(
                                                query, valores, commit=True)
                                        except Exception as e:
                                            self.logger.error(
                                                f"Erro ao inserir notícia: {artigo.title}. Detalhes: {e}")
                                        finally:
                                            noticias_salvas.append(noticia)
                            else:
                                if isinstance(artigo.title, str):
                                    if self.verificar_noticia(artigo.title, texto_noticia, noticias_salvas):
                                        self.logger.info(
                                            f"Título: {artigo.title}")
                                        noticia = {
                                            "titulo": artigo.title,
                                            "texto": texto_noticia
                                        }

                                        query = "INSERT INTO noticias (cod_bolsa, titulo, descricao, data_historico, url) VALUES (%s, %s, %s, %s, %s)"
                                        valores = ('EMBR3.SA', artigo.title, texto_noticia, datetime.strptime(
                                            artigo.seendate, '%Y%m%dT%H%M%SZ').date(), artigo.url)

                                        try:
                                            self.db.executa_query(
                                                query, valores, commit=True)
                                        except Exception as e:
                                            self.logger.error(
                                                f"Erro ao inserir notícia: {artigo.title}. Detalhes: {e}")
                                        finally:
                                            noticias_salvas.append(noticia)

                    mes_anterior = (data_inicial.month - 1) or 12
                    ano_atualizado = data_inicial.year if data_inicial.month > 1 else data_inicial.year - 1
                    data_inicial = data_inicial.replace(
                        year=ano_atualizado, month=mes_anterior)

                    self.logger.info(f"Notícias históricas cadastradas...")
            else:
                self.logger.info(f"Notícias históricos já presentes.")

        except Exception as e:
            self.logger.error(
                f"Houve um problema ao adquirir os dados históricos: {e}")
            raise

        self.logger.info(
            f"Consulta de notícias históricas finalizada com sucesso.")

# **Buscar notícias recentes e verificar duplicatas**

    def buscar_noticias(self):

        self.logger.info(
            f"Buscando notícias recentes para a empresa 'EMBR3.SA'")

        API_NEWS = "47f7a2378b0a4a6c9096689cf1956945"
        url = f"https://newsapi.org/v2/everything?q=Embraer&language=pt&apiKey={API_NEWS}"

        response = requests.get(url)
        data = response.json()

        for artigo in data.get("articles", [])[:10]:
            titulo = artigo["title"]
            url = artigo["url"]
            conteudo = artigo["content"]
            data_pub = artigo["publishedAt"]
            data_pub = datetime.strptime(data_pub, '%Y-%m-%dT%H:%M:%SZ').date()

            # Verificar se a notícia já está no banco
            query = "SELECT COUNT(*) FROM noticias WHERE titulo = %s"
            valores = (titulo,)
            news = self.db.fetch_data(query, valores, tipo_fetch="one")

            if news and news[0] == 0:

                # se não está no banco ainda, verificamos se não é repetida dentro do lote
                noticias_salvas = []
                # verifica se a notícia não é reperida dentro desse lote recebido.
                if self.verificar_noticia(titulo, conteudo, noticias_salvas):
                    self.logger.info(
                        f"Título: {artigo.title}")
                    noticia = {
                        "titulo": artigo.title,
                        "texto": conteudo
                    }

                    query = "INSERT INTO noticias (cod_bolsa, titulo, descricao, data_historico, url) VALUES (%s, %s, %s, %s, %s)"
                    valores = ('EMBR3.SA', titulo, conteudo, data_pub, url)

                    try:
                        self.db.executa_query(
                            query, valores, commit=True)
                        print(f"Nova notícia adicionada: {titulo}")
                    except Exception as e:
                        self.logger.error(
                            f"Erro ao inserir notícia: {artigo.title}. Detalhes: {e}")
                    finally:
                        noticias_salvas.append(noticia)

        self.logger.info(f"Novas notícias verificas com sucesso.")
