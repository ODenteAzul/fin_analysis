from scrapp_noticias.scrapp import Scrapping


class Scrapper():
    def __init__(self,
                 logger,
                 db,
                 conn,
                 cursor):
        self.logger = logger
        self.db = db
        self.conn = conn
        self.cursor = cursor

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

        try:

            scrap.verifica_tabelas()

            scrap.busca_noticias_historicas()

            # scrap.buscar_noticias()

            scrap.busca_valores_fechamento()

            scrap.busca_cotacao_atual(hora_abertura_bolsa=hora_abertura_bolsa,
                                      hora_fechamento_bolsa=hora_fechamento_bolsa,
                                      dolar_inicio=dolar_inicio,
                                      dolar_fim=dolar_fim)

        except Exception as e:
            self.logger.error(
                f"Houve um problema ao adquirir os dados via scrapping: {e}")
            with open("scraping_errors.log", "a") as f:
                f.write(f"{hora_atual} - Erro: {e}\n")
