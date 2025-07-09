import requests
import numpy as np
from datetime import date
from gdeltdoc import GdeltDoc, Filters
import re
from fuzzywuzzy import fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from datetime import datetime
from bs4 import BeautifulSoup
from config.ambience import EnvConfig


class ScrappingNoticias():
    def __init__(self,
                 logger,
                 db,
                 conn,
                 cursor,
                 table_checker):
        self.logger = logger
        self.db = db
        self.conn = conn
        self.cursor = cursor
        self.table_checker = table_checker

    def _converter_para_nativo(df):
        """
        Converte colunas numéricas do DataFrame
        para tipos nativos do Python (float).
        """
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for coluna in numeric_cols:
            # Aplicando a conversão para cada valor da coluna
            df[coluna] = df[coluna].apply(lambda x: float(x))

        return df

    def _titulos_sao_similares(self, titulo1, titulo2, limite=80):
        try:
            if isinstance(titulo1, str) and isinstance(titulo2, str):
                return fuzz.token_set_ratio(titulo1.lower(),
                                            titulo2.lower()) > limite

            return False

        except Exception as e:
            self.logger.error(
                f"Problema ao testar a similaridades dos títulos: {e}")
            raise

    def _verificar_similaridade(self, noticia1, noticia2):

        if len(noticia1.split()) < 20 or len(noticia2.split()) < 20:
            return False

        else:
            try:
                vetorizer = TfidfVectorizer()
                vetorizer = TfidfVectorizer(stop_words=None)
                matriz = vetorizer.fit_transform([noticia1, noticia2])
                similaridade = cosine_similarity(matriz[0], matriz[1])

                return similaridade[0][0]

            except Exception as e:
                self.logger.error(
                    f"Problema ao testar similaridades do texto das notícias: {e}")
                raise

    def _verificar_relevancia(self, texto, empresa, limite=0.005):
        try:
            if isinstance(texto, str):
                total_palavras = len(texto.lower().split())
                freq_relativa = (texto.lower().count(empresa.lower())
                                 / max(total_palavras, 1))

                return freq_relativa < limite

        except Exception as e:
            self.logger.error(
                f"Houve um problema aoverificar a relevância da notícia {e}")
            raise

    def _verificar_noticia(self, titulo, texto, noticias_salvas, empresa):

        for noticia in noticias_salvas:
            if texto == "texto_indisponivel":
                if (
                    self._titulos_sao_similares(titulo, noticia["titulo"])
                    or self._verificar_relevancia(titulo, empresa)
                ):
                    self.logger.info(
                        "Notícia duplicada ou não relevante encontrada:"
                        "Ignorando...")
                return False
            else:
                if (isinstance(noticia["titulo"], str)
                        and isinstance(noticia["texto"], str)):
                    if (self._titulos_sao_similares(titulo, noticia["titulo"])
                        or self._verificar_similaridade(
                        texto,
                        noticia["texto"]) > 0.70
                            or self._verificar_relevancia(texto, empresa)):
                        self.logger.info(
                            "Notícia duplicada ou não"
                            " relevante encontrada. Ignorando...")
                        return False
                else:
                    self.logger.info(
                        "Notícia com má formação ou incorreta...")
                    return False

        self.logger.info("Nova notícia armazenada.")
        return True

    def _obter_texto_noticia(self, url):

        if not isinstance(url, str):
            self.logger.error(f"Erro: URL inválida recebida {url}")
            return ""

        texto_noticia = ""

        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0"
                "Safari/537.36"
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
            print(f"Erro ao obter o texto da notícia: {e}")

        #    options = webdriver.ChromeOptions()
        #    options.add_argument("--headless")
        #    options.add_argument("--disable-gpu")
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
        #            [p.get_text() for p in article_body])
        # if article_body else " "

        #   texto_noticia = texto_noticia.strip() if texto_noticia else " "

        # Limpeza do texto extraído
        if texto_noticia:
            # Remove espaços extras
            texto_noticia = re.sub(r"\s+", " ", texto_noticia)
            texto_noticia = re.sub(
                r"\b(Publicado em|Fonte|Leia mais em|Promoção|Todos os"
                r"Direitos Reservados)\b.*",
                "",
                texto_noticia)
            # Remove caracteres especiais
            texto_noticia = re.sub(r"[^a-zA-Z0-9À-ÿ\s]", "", texto_noticia)

        if not texto_noticia.strip():
            self.logger.error(
                ("Texto está vazio ou contém apenas stop words."
                 "Ignorando processamento."))
            return "texto_indisponivel"

        return texto_noticia

    def busca_noticias_historicas(self):

        # Coletar notícias históricas (somente se tabela não estiver populada)
        self.logger.info("Vericando presença de notícias históricas... ")

        try:

            for sigla, nome in self.ls_empresas:
                if not self.table_checker.check_populated(camada='silver',
                                                          tabela='noticias',
                                                          empresa=sigla,
                                                          resposta='bool'):

                    self.logger.info(
                        f"Buscando notícias inicias para: '{nome}:{sigla}'")

                    data_inicial = datetime.today().replace(day=1)
                    data_final = data_inicial.replace(
                        year=data_inicial.year - 10)

                    while data_inicial >= data_final:
                        start_date = data_inicial.strftime("%Y-%m-%d")
                        from calendar import monthrange
                        end_date = data_inicial.replace(day=monthrange(
                            data_inicial.year, data_inicial.month)[1]). \
                            strftime("%Y-%m-%d")

                        self.logger.info(
                            (f"Buscando notícias de"
                             f" {start_date} até {end_date}"))

                        filtros = Filters(
                            keyword=nome,
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

                            texto_noticia = self._obter_texto_noticia(
                                artigo.url)

                            if artigo.title:
                                if (texto_noticia.strip()
                                        and texto_noticia
                                        != "texto_indisponivel"):
                                    if (isinstance(artigo.title, str)
                                        and isinstance(texto_noticia, str)
                                            and texto_noticia != ""):
                                        if self._verificar_noticia(
                                                artigo.title,
                                                texto_noticia,
                                                noticias_salvas,
                                                nome):
                                            self.logger.info(
                                                f"Título: {artigo.title}")
                                            noticia = {
                                                "titulo": artigo.title,
                                                "texto": texto_noticia
                                            }

                                            query = ("""INSERT INTO
                                                     silver.noticias
                                                     (cod_bolsa,
                                                     titulo,
                                                     descricao,
                                                     data_historico,
                                                     url)
                                                     VALUES
                                                     (%s, %s, %s, %s, %s)""")
                                            valores = (sigla,
                                                       artigo.title,
                                                       texto_noticia,
                                                       datetime.strptime(
                                                           artigo.seendate,
                                                           '%Y%m%dT%H%M%SZ')
                                                       .date(),
                                                       artigo.url)

                                            try:
                                                self.db.executa_query(
                                                    query,
                                                    valores,
                                                    commit=True)
                                            except Exception as e:
                                                self.logger.error(
                                                    (f"Erro ao inserir"
                                                     f" notícia:"
                                                     f" {artigo.title}."
                                                     f" Detalhes: {e}"))
                                            finally:
                                                noticias_salvas.append(noticia)
                                else:
                                    if isinstance(artigo.title, str):
                                        if self._verificar_noticia(
                                                artigo.title,
                                                texto_noticia,
                                                noticias_salvas,
                                                nome):
                                            self.logger.info(
                                                f"Título: {artigo.title}")
                                            noticia = {
                                                "titulo": artigo.title,
                                                "texto": texto_noticia
                                            }

                                            query = "INSERT INTO " \
                                                "silver.noticias (cod_bolsa," \
                                                "titulo, " \
                                                "descricao, " \
                                                "data_historico, " \
                                                "url) " \
                                                "VALUES (%s, %s, %s, %s, %s)"

                                            valores = (sigla,
                                                       artigo.title,
                                                       texto_noticia,
                                                       datetime.strptime(
                                                           artigo.seendate,
                                                           '%Y%m%dT%H%M%SZ').
                                                       date(), artigo.url)

                                            try:
                                                self.db.executa_query(
                                                    query,
                                                    valores,
                                                    commit=True)
                                            except Exception as e:
                                                self.logger.error(
                                                    (f"Erro ao inserir"
                                                     f" notícia: "
                                                     f"{artigo.title}. "
                                                     f"Detalhes: {e}"))
                                            finally:
                                                noticias_salvas.append(noticia)

                        mes_anterior = (data_inicial.month - 1) or 12
                        ano_atualizado = (data_inicial.year
                                          if data_inicial.month
                                          > 1 else
                                          data_inicial.year - 1)
                        data_inicial = data_inicial.replace(
                            year=ano_atualizado, month=mes_anterior)

                    query = "Select cod_bolsa " \
                        "FROM silver.noticias " \
                        "WHERE cod_bolsa = %s"
                    valores = sigla

                    resultado = self.db.fetch_data(
                        self, query, valores=valores, tipo_fetch='one')

                    if resultado is not None:

                        hoje = date.today()
                        self.table_checker.register_populated(
                            self,
                            camada='meta',
                            tabela='controle_populacao',
                            empresa=sigla,
                            status=True,
                            data_populated=hoje,
                            observation='Carga Inicial')

                    self.logger.info(
                        (f"Notícias históricas"
                         f" cadastradas para '{nome}:{sigla}'..."))
                else:
                    self.logger.info("Notícias históricas já presentes.")

        except Exception as e:
            self.logger.error(
                f"Houve um problema ao adquirir as notícias históricas: {e}")
            raise

        self.logger.info(
            "Consulta de notícias históricas finalizada com sucesso.")

# **Buscar notícias recentes e verificar duplicatas**

    def buscar_noticias(self):

        try:

            for tik in self.ls_empresas:

                self.logger.info(
                    (f"Buscando notícias recentes"
                     f" para a empresa '{tik['tabela']}'"))

                API_NEWS = EnvConfig.NEWS_API_KEY
                url = (
                    f"https://newsapi.org/v2/everything?q={tik['tabela']}&language=pt&apiKey={API_NEWS}")

                response = requests.get(url)
                data = response.json()

                noticias_salvas = []

                for artigo in data.get("articles", [])[:10]:
                    titulo = artigo["title"]
                    url = artigo["url"]
                    conteudo = artigo["content"]
                    data_pub = artigo["publishedAt"]
                    data_pub = datetime.strptime(
                        data_pub, '%Y-%m-%dT%H:%M:%SZ').date()

                    # Verificar se a notícia já está no banco
                    query = """SELECT COUNT(*)
                    FROM silver.noticias
                    WHERE titulo = %s AND cod_bolsa = %s"""
                    valores = (titulo, tik['ticker'])
                    news = self.db.fetch_data(query, valores, tipo_fetch="one")

                    if news and news[0] == 0:
                        # verifica se a notícia não é reperida
                        # dentro desse lote recebido.

                        if len(noticias_salvas) > 0:

                            if self._verificar_noticia(
                                    titulo,
                                    conteudo,
                                    noticias_salvas,
                                    tik['tabela']):

                                noticia = {
                                    "titulo": titulo,
                                    "texto": conteudo
                                }

                                query = "INSERT INTO silver.noticias" \
                                    "(cod_bolsa," \
                                    "titulo," \
                                    "descricao," \
                                    "data_historico," \
                                    "url)" \
                                    "VALUES (%s, %s, %s, %s, %s)"
                                valores = (tik['ticker'], titulo,
                                           conteudo, data_pub, url)

                                self.db.executa_query(
                                    query, valores, commit=True)

                                print(f"Nova notícia adicionada: {titulo}")

                                noticias_salvas.append(noticia)

                        else:

                            noticia = {
                                "titulo": titulo,
                                "texto": conteudo
                            }

                            query = "INSERT INTO silver.noticias" \
                                "(cod_bolsa, titulo, descricao," \
                                "data_historico, url)" \
                                "VALUES (%s, %s, %s, %s, %s)"
                            valores = (tik['ticker'], titulo,
                                       conteudo, data_pub, url)

                            self.db.executa_query(
                                query, valores, commit=True)

                            print(f"Nova notícia adicionada: {titulo}")

                            noticias_salvas.append(noticia)

                self.logger.info(
                    (f"Novas notícias verificas"
                     f" com sucesso para '{tik['tabela']}'."))

        except Exception as e:
            self.logger.error(
                (f"Houve um problema ao adquirir novas"
                 f" notícias para {tik['tabela']}, erro: {e}"))
