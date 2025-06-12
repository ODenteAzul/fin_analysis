from utils import LoggerCustomizado
from conn_pg import PostGreSQL
from datetime import datetime, time
from scrapp_noticias.run_scrapping import Scrapper
from scrapp_valores.run_macroeco import MacroEconomics


def run_processes():
    log = LoggerCustomizado()

    # Obtendo a conexão com o PostgreSQL
    db = PostGreSQL(logger=log)
    conn, cursor = db.conectar()

    hora_inicio_geral = datetime.now().time()
    dia_atual = datetime.now().date()

    log.info(f"Iniciando os processos: '{hora_inicio_geral}'")

    # horários de controle para ações
    hora_abertura_bolsa = time(10, 0, 0)
    hora_fechamento_bolsa = time(16, 57, 0)
    dolar_inicio = time(9, 0, 0)
    dolar_fim = time(17, 2, 0)
    novo_dia = time(1, 0, 0)
    coleta_diaria_macro = time(23, 0, 0)

    hora_inicio = datetime.now()

    log.info("Etapa 1: Scrapping de informações...")

    go_scrapp = Scrapper(log,
                         db,
                         conn,
                         cursor)

    go_scrapp.executa_scrapping(hora_inicio,
                                hora_abertura_bolsa, hora_fechamento_bolsa,
                                dolar_inicio, dolar_fim)

    end_time = datetime.now()
    log.info(f"Etapa 1: Finalizada com sucesso, em: {end_time - hora_inicio}")

    hora_inicio = datetime.now()

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
    log.info(f"Processo finalizado em: {end_time - hora_inicio}")


if __name__ == "__main__":
    run_processes()
