import argparse
from datetime import datetime
from zoneinfo import ZoneInfo

from utils.utils import LoggerCustomizado
from utils.conn_pg import PostGreSQL
from scrapp.run_scrapping import ScrapperRun
from utils.table_checker import TableChecker


def run_processes(controle=None):

    log = LoggerCustomizado()

    title = r"""
    ######################################################
    #     _____                                          #
    #    / ___/______________ _____  ____  ___  _____    #
    #    \__ \/ ___/ ___/ __ `/ __ \/ __ \/ _ \/ ___/    #
    #    ___/ / /__/ /  / /_/ / /_/ / /_/ /  __/ /       #
    #    /____/\___/_/   \__,_/ .___/ .___/\___/_/       #
    #                        /_/   /_/                   #
    ######################################################
    """

    if controle is not None:
        log.info(title)

        # Estabelecendo horário padrão para controle
        tz_brasil = ZoneInfo("America/Sao_Paulo")
        hora_inicio = datetime.now(tz=tz_brasil)
        hora_inicio_geral = datetime.now(tz=tz_brasil)

        # Obtendo a conexão com o PostgreSQL
        db = PostGreSQL(logger=log)
        conn, cursor = db.conectar()

        # verificação de tabelas e meta dados
        table_checker = TableChecker(log,
                                     db,
                                     conn,
                                     cursor)

        # empresas que serão analisadas
        ls_empresas = [{"ticker": "EMBR3.SA", "tabela": "Embraer"},
                       {"ticker": "WEGE3.SA", "tabela": "WEG"},
                       {"ticker": "KLBN4.SA", "tabela": "Klabin"},
                       {"ticker": "PETR4.SA", "tabela": "Petrobras"},
                       {"ticker": "ABEV3.SA", "tabela": "Ambev"},
                       {"ticker": "ITUB4.SA", "tabela": "Itaú Unibanco"},
                       {"ticker": "BPAC11.SA", "tabela": "BTG Pactual"},
                       {"ticker": "KEPL3.SA", "tabela": "Kepler Weber"}]

        log.info(f"Iniciando o processamento: '{hora_inicio_geral}'")
        log.info("Scrapping de informações...")

        go_scrapp = ScrapperRun(log,
                                db,
                                conn,
                                cursor,
                                table_checker,
                                ls_empresas,
                                controle)

        go_scrapp.executa_scrapping()

        end_time = datetime.now(tz=tz_brasil)
        log.info(
            f"Scrapping finalizado com sucesso em: {end_time - hora_inicio}")

        db.fechar_conexao()

        end_time = datetime.now(tz=tz_brasil)
        log.info(f"Processamento finalizado em: {end_time - hora_inicio}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--news", action="store_true")
    parser.add_argument("--indices", action="store_true")
    parser.add_argument("--cotacoes", action="store_true")
    parser.add_argument("--fechamentos", action="store_true")
    args = parser.parse_args()

    if args.news:
        run_processes("news")
    elif args.indices:
        run_processes("indices")
    elif args.cotacoes:
        run_processes("cotacoes")
    elif args.fechamentos:
        run_processes("fechamentos")
