from utils.utils import LoggerCustomizado
from conn_pg import PostGreSQL
from datetime import datetime, time
from indices.run_technical_indexes import TechCalcs


def run_processes():
    log = LoggerCustomizado()

    # Obtendo a conexão com o PostgreSQL
    db = PostGreSQL(logger=log)
    conn, cursor = db.conectar()

    hora_inicio_geral = datetime.now()
    dia_atual = datetime.now().date()

    log.info(f"Iniciando os processos: '{hora_inicio_geral}'")

    # horários de controle para ações
    hora_abertura_bolsa = time(10, 0, 0)
    hora_fechamento_bolsa = time(16, 57, 0)
    novo_dia = time(1, 0, 0)
    coleta_diaria_macro = time(23, 0, 0)

    hora_inicio = datetime.now()

    log.info("Iniciando Cálculo de índices técnicos...")

    go_tc = TechCalcs(log,
                      db,
                      conn,
                      cursor)

    go_tc.executa_ti(dia_atual, novo_dia,
                     coleta_diaria_macro, hora_abertura_bolsa,
                     hora_fechamento_bolsa)

    end_time = datetime.now()
    log.info(f"Finalizado com sucesso, em: {end_time - hora_inicio}")

    hora_inicio = datetime.now()

    db.fechar_conexao()

    end_time = datetime.now()
    log.info(f"Processo finalizado em: {end_time - hora_inicio}")


if __name__ == "__main__":
    run_processes()
