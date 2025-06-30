from utils.utils import LoggerCustomizado
from utils.conn_pg import PostGreSQL
from datetime import datetime
from zoneinfo import ZoneInfo
from scrapp.run_scrapping import ScrapperRun
from utils.table_checker import TableChecker


def run_processes():
    log = LoggerCustomizado()

    # Obtendo a conexão com o PostgreSQL
    db = PostGreSQL(logger=log)
    conn, cursor = db.conectar()

    # verificação de tabelas e meta dados
    table_checker = TableChecker(log,
                                 db,
                                 conn,
                                 cursor)

    # Estabelecendo horário padrão para controle
    tz_brasil = ZoneInfo("America/Sao_Paulo")
    hora_inicio = datetime.now(tz=tz_brasil)
    hora_inicio_geral = datetime.now(tz=tz_brasil)

    # empresas que serão analisadas
    ls_empresas = [{"ticker": "EMBR3.SA", "tabela": "Embraer"},
                   {"ticker": "WEGE3.SA", "tabela": "WEG"},
                   {"ticker": "KLBN4.SA", "tabela": "Klabin"},
                   {"ticker": "PETR4.SA", "tabela": "Petrobras"},
                   {"ticker": "ABEV3.SA", "tabela": "Ambev"},
                   {"ticker": "ITUB4.SA", "tabela": "Itaú Unibanco"},
                   {"ticker": "BPAC11.SA", "tabela": "BTG Pactual"},
                   {"ticker": "KEPL3.SA", "tabela": "Kepler Weber"}]

    title = r"""
    #######################################################################################
    #     ______                                                                          #
    #    /      \                                                                         # 
    #    /$$$$$$  |  _______   ______   ______    ______    ______    ______    ______    # 
    #    $$ \__$$/  /       | /      \ /      \  /      \  /      \  /      \  /      \   # 
    #    $$      \ /$$$$$$$/ /$$$$$$  |$$$$$$  |/$$$$$$  |/$$$$$$  |/$$$$$$  |/$$$$$$  |  #
    #    $$$$$$  |$$ |      $$ |  $$/ /    $$ |$$ |  $$ |$$ |  $$ |$$    $$ |$$ |  $$/    #
    #    /  \__$$ |$$ \_____ $$ |     /$$$$$$$ |$$ |__$$ |$$ |__$$ |$$$$$$$$/ $$ |        #
    #    $$    $$/ $$       |$$ |     $$    $$ |$$    $$/ $$    $$/ $$       |$$ |        #
    #    $$$$$$/   $$$$$$$/ $$/       $$$$$$$/ $$$$$$$/  $$$$$$$/   $$$$$$$/ $$/          #
    #                                        $$ |      $$ |                               #
    #                                        $$ |      $$ |                               #
    #                                        $$/       $$/                                #
    #######################################################################################
    """
    log.info(title)

    log.info(f"Iniciando o processamento: '{hora_inicio_geral}'")
    log.info("Scrapping de informações...")

    go_scrapp = ScrapperRun(log,
                            db,
                            conn,
                            cursor,
                            table_checker,
                            ls_empresas)

    go_scrapp.executa_scrapping()

    end_time = datetime.now(tz=tz_brasil)
    log.info(f"Scrapping finalizado com sucesso em: {end_time - hora_inicio}")

    db.fechar_conexao()

    end_time = datetime.now(tz=tz_brasil)
    log.info(f"Processamento finalizado em: {end_time - hora_inicio}")


if __name__ == "__main__":
    run_processes()

# if __name__ == "__main__":
#    executa_scrapping = True
#    executa_macro = True

#    if len(sys.argv) > 1:
#        arg = sys.argv[1].lower()
#        if arg == "scrapping":
#            executa_macro = False
#        elif arg == "macro":
#            executa_scrapping = False

#    run_processes(executa_scrapping=executa_scrapping, executa_macro=executa_macro)
