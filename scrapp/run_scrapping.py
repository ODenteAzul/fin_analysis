from scrapp.scrapp_noticias import ScrappingNoticias
from scrapp.scrapp_fechamentos import ScrappIndices
from scrapp.scrapp_cotacoes_intra import ScrappIntra
from utils.ddl_loader import CriadorDDL
from utils.conn_pg import PostGreSQL
from utils.table_checker import TableChecker

from zoneinfo import ZoneInfo
from datetime import datetime


class ScrapperRun():
    def __init__(self,
                 logger,
                 controle):
        self.logger = logger
        self.controle = controle

    def executa_scrapping(self):

        try:

            # Obtendo a conexão com o PostgreSQL
            db = PostGreSQL(logger=self.logger)
            db.conectar()

            # criação dinâmica de SQL
            ddl_creator = CriadorDDL("sql/ddl")

            # verificação de tabelas e controle de meta dados
            table_checker = TableChecker(
                self.logger,
                db,
                ddl_creator
            )

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

            table_checker.check_tables()

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
                    db=db,
                    table_checker=table_checker,
                    ddl_creator=ddl_creator
                )

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
                    controle=self.controle,
                    db=db,
                    table_checker=table_checker,
                    ddl_creator=ddl_creator
                )

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
                    db=db,
                    table_checker=table_checker,
                    ddl_creator=ddl_creator
                )

                scrap_intra.colheita_cotacao_atual()

                hora_fim = datetime.now(tz=tz_brasil)
                self.logger.info(
                    f"""Busca de dados de cotação:
                    Terminada com sucesso em : {hora_fim-hora_inicio}""")

            db.fechar_conexao()

        except Exception as e:
            self.logger.error(
                f"Houve um problema ao adquirir os dados via scrapping: {e}")
