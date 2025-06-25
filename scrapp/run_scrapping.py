from scrapp.scrapp_noticias import ScrappingNoticias
from scrapp.scrapp_indices import ScrappIndices
from utils.table_checker import TableChecker

from zoneinfo import ZoneInfo
from datetime import datetime


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
            tz_brasil = ZoneInfo("America/Sao_Paulo")
            hora_inicio = datetime.now(tz=tz_brasil)

            title = r"""
            #################################
            #   +-+-+-+-+-+-+ +-+-+-+-+-+   #
            #   |T|a|b|l|e|s| |C|h|e|c|k|   #
            #   +-+-+-+-+-+-+ +-+-+-+-+-+   #
            #################################
            """
            self.logger.info(title)

            tables = TableChecker(
                logger=self.logger,
                db=self.db,
                conn=self.conn,
                cursor=self.cursor)

            tables.check_tables()

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

            scrap = ScrappingNoticias(
                logger=self.logger,
                db=self.db,
                conn=self.conn,
                cursor=self.cursor,
                ls_empresas=self.ls_empresas,
                table_checker=tables)

            scrap.busca_noticias_historicas()

            scrap.buscar_noticias()

            hora_fim = datetime.now(tz=tz_brasil)
            self.logger.info(
                f"Busca de notícias terminada com sucesso em : {hora_fim-hora_inicio}")

            hora_inicio = datetime.now(tz=tz_brasil)

            title = r"""
            ######################################
            #     __                             #
            #    |_  _  _ __  _ __  o  _  _  _   #
            #    |__(_ (_)| |(_)||| | (_ (_)_>   #
            #                                    # 
            ######################################
            """
            self.logger.info(title)

            scrap = ScrappIndices(
                logger=self.logger,
                db=self.db,
                conn=self.conn,
                cursor=self.cursor,
                ls_empresas=self.ls_empresas,
                table_checker=tables)

            try:
                scrap.busca_histórico_macroeconomia()

                scrap.busca_dados_macro_atuais(coleta_diaria)

                scrap.busca_valores_fechamento()

                scrap.busca_cotacao_atual()

            except Exception as e:
                hora_atual = datetime.now().time()
                self.logger.error(
                    f"Houve um problema ao adquirir os dados via scrapping: {e}")
                with open("scraping_errors.log", "a") as f:
                    f.write(f"{hora_atual} - Erro: {e}\n")

            hora_fim = datetime.now(tz=tz_brasil)
            self.logger.info(
                f"Busca de dados economicos terminada com sucesso em : {hora_fim-hora_inicio}")

        except Exception as e:
            self.logger.error(
                f"Houve um problema ao adquirir os dados via scrapping: {e}")
            with open("scraping_errors.log", "a") as f:
                f.write(f"{hora_atual} - Erro: {e}\n")
