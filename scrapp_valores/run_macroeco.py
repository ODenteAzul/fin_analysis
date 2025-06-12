from scrapp_valores.scrapp_macro import ScrappMacro
from datetime import datetime


class MacroEconomics():
    def __init__(self,
                 logger,
                 db,
                 conn,
                 cursor):
        self.logger = logger
        self.db = db
        self.conn = conn
        self.cursor = cursor

    def executa_macro(self, coleta_diaria):

        scrap = ScrappMacro(
            logger=self.logger,
            db=self.db,
            conn=self.conn,
            cursor=self.cursor)

        try:
            self.logger.info(
                "Verificando a presença das tabelas de macro economia...")

            scrap.verifica_tabelas()

            self.logger.info("Tabelas verificadas.")

            scrap.busca_histórico_macroeconomia()

            scrap.busca_dados_macro_atuais(coleta_diaria)

        except Exception as e:
            hora_atual = datetime.now().time()
            self.logger.error(
                f"Houve um problema ao adquirir os dados via scrapping: {e}")
            with open("scraping_errors.log", "a") as f:
                f.write(f"{hora_atual} - Erro: {e}\n")
