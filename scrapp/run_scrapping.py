from scrapp.scrapp_noticias import ScrappingNoticias
from scrapp.scrapp_fechamentos import ScrappIndices
from scrapp.scrapp_cotacoes_intra import ScrappIntra

from zoneinfo import ZoneInfo
from datetime import datetime


class ScrapperRun():
    def __init__(self,
                 logger,
                 db,
                 conn,
                 cursor,
                 table_checker,
                 controle):
        self.logger = logger
        self.db = db
        self.conn = conn
        self.cursor = cursor
        self.table_checker = table_checker
        self.controle = controle

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

            self.table_checker.check_tables()

            if self.controle == 'news':
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
                    table_checker=self.table_checker)

                # scrap.busca_noticias_historicas()

                scrap.buscar_noticias()

                hora_fim = datetime.now(tz=tz_brasil)
                self.logger.info(
                    f"""Busca de notícias terminada com sucesso em :
                    {hora_fim-hora_inicio}""")

            elif self.controle == 'indices' or self.controle == 'fechamentos':

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

                scrap_f = ScrappIndices(
                    logger=self.logger,
                    db=self.db,
                    conn=self.conn,
                    cursor=self.cursor,
                    table_checker=self.table_checker,
                    controle=self.controle)

                scrap_f.colheira_diaria()

                hora_fim = datetime.now(tz=tz_brasil)
                self.logger.info(
                    f"""Busca de dados economicos diários:
                     Terminada com sucesso em : {hora_fim-hora_inicio}""")

            elif self.controle == 'cotacoes':

                title = r"""
                ######################################
                #     __                             #
                #    |_  _  _ __  _ __  o  _  _  _   #
                #    |__(_ (_)| |(_)||| | (_ (_)_>   #
                #                                    #
                ######################################
                """
                self.logger.info(title)

                scrap_intra = ScrappIntra(
                    logger=self.logger,
                    db=self.db,
                    conn=self.conn,
                    cursor=self.cursor,
                    table_checker=self.table_checker)

                scrap_intra.colheita_cotacao_atual()

                hora_fim = datetime.now(tz=tz_brasil)
                self.logger.info(
                    f"""Busca de dados de cotação:
                    Terminada com sucesso em : {hora_fim-hora_inicio}""")

        except Exception as e:
            self.logger.error(
                f"Houve um problema ao adquirir os dados via scrapping: {e}")
