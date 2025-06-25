from utils.utils import LoggerCustomizado
from conn_pg import PostGreSQL
from datetime import datetime
from zoneinfo import ZoneInfo
from scrapp.run_scrapping import ScrapperRun
from scrapp_valores.run_macroeco import MacroEconomics
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
    ls_empresas = {"EMBR3.SA": "Embraer",
                   "WEG3.SA": "WEG",
                   "KLBN4.SA": "Klabin",
                   "PETR4.SA": "Petrobras"}

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
    log.info("Etapa 1: Scrapping de informações...")

    go_scrapp = ScrapperRun(log,
                            db,
                            conn,
                            cursor,
                            table_checker,
                            ls_empresas)

    go_scrapp.executa_scrapping()

    end_time = datetime.now()
    log.info(f"Etapa 1: Finalizada com sucesso, em: {end_time - hora_inicio}")

    hora_inicio = datetime.now(tz=tz_brasil)

    log.info("Etapa 2: Coletando dados Macro Econômicos...")

    go_me = MacroEconomics(log,
                           db,
                           conn,
                           cursor)

    go_me.executa_macro(coleta_diaria_macro)

    end_time = datetime.now()
    log.info(f"Etapa 2: Finalizada com sucesso, em: {end_time - hora_inicio}")

    db.fechar_conexao()

    end_time = datetime.now()
    log.info(f"Processamento finalizado em: {end_time - hora_inicio}")


if __name__ == "__main__":
    run_processes()
