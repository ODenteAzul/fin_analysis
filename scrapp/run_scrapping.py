from scrapp.scrapp_noticias import ScrappingNoticias
from scrapp.scrapp_macro import ScrappMacro
from utils.table_checker import TableChecker


class ScrapperRun():
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

    def executa_scrapping(self):

        try:
            title = r"""
            #########################################
            #        )                              #
            #     ( /(        )           (         #
            #     )\())    ( /((     (     )        #
            #    ((_)\  (  )\())\  ( )\ ( /( (      #
            #    _((_) )\(_))((_) )((_))(_)))\      #
            #    | \| |((_) |_ (_)((_|_|(_)_((_)    #
            #    | .` / _ \  _|| / _|| / _` (_-<    #
            #    |_|\_\___/\__||_\__||_\__,_/__/    #
            #                                       #
            #########################################
            """
            self.logger.info(title)

            tables = TableChecker(
                logger=self.logger,
                db=self.db,
                conn=self.conn,
                cursor=self.cursor)

            tables.check_tables()

            scrap = ScrappingNoticias(
                logger=self.logger,
                db=self.db,
                conn=self.conn,
                cursor=self.cursor,
                ls_empresas=self.ls_empresas,
                table_checker=tables)

            scrap.busca_noticias_historicas()

            scrap.buscar_noticias()

            title = r"""
            #####################################################
            #    (                       )    (          )      #
            #    )\   (  (   (     (    (     )\  (  (  (       #
            #    ((_)  )\ )\  )\ )  )\   )\  ((_) )\ )\ )\      #
            #    | __|((_|(_)_(_/( ((_)_((_)) (_)((_|(_|(_)     #
            #    | _|/ _/ _ \ ' \)) _ \ '  \()| |/ _/ _ (_-<    #
            #    |___\__\___/_||_|\___/_|_|_| |_|\__\___/__/    #
            #                                                   #
            #####################################################
            """
            self.logger.info(title)

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

                scrap.busca_valores_fechamento()

                scrap.busca_cotacao_atual()

        except Exception as e:
            self.logger.error(
                f"Houve um problema ao adquirir os dados via scrapping: {e}")
            with open("scraping_errors.log", "a") as f:
                f.write(f"{hora_atual} - Erro: {e}\n")
