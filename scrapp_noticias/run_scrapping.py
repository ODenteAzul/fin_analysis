from scrapp_noticias.scrapp import Scrapping
from utils.table_checker import TableChecker


class Scrapper():
    def __init__(self,
                 logger,
                 db,
                 conn,
                 cursor,
                 table_checker,
                 ls_empresas):
        self.logger = logger
        self.db = db
        self.conn = conn
        self.cursor = cursor
        self.table_checker = table_checker
        self.ls_empresas = ls_empresas

    def executa_scrapping(self,
                          hora_atual,
                          hora_abertura_bolsa,
                          hora_fechamento_bolsa,
                          dolar_inicio,
                          dolar_fim):

        scrap = Scrapping(
            logger=self.logger,
            db=self.db,
            conn=self.conn,
            cursor=self.cursor)

        tables = TableChecker(
            logger=self.logger,
            db=self.db,
            conn=self.conn,
            cursor=self.cursor)

        try:

            tables.check_tables()

            scrap.busca_noticias_historicas(ls_empresas=self.ls_empresas)

            scrap.buscar_noticias(ls_empresas=self.ls_empresas)

            scrap.busca_valores_fechamento(ls_empresas=self.ls_empresas)

            scrap.busca_cotacao_atual(hora_abertura_bolsa=hora_abertura_bolsa,
                                      hora_fechamento_bolsa=hora_fechamento_bolsa,
                                      dolar_inicio=dolar_inicio,
                                      dolar_fim=dolar_fim,
                                      ls_empresas=self.ls_empresas)

        except Exception as e:
            self.logger.error(
                f"Houve um problema ao adquirir os dados via scrapping: {e}")
            with open("scraping_errors.log", "a") as f:
                f.write(f"{hora_atual} - Erro: {e}\n")
