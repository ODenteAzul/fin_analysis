import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


class TableChecker():
    def __init__(self,
                 logger,
                 db,
                 conn,
                 cursor):
        self.logger = logger
        self.db = db
        self.conn = conn
        self.cursor = cursor

    def check_tables(self):

        try:

            self.logger.info(
                f"Iniciando verificação das tabelas e SCHEMAS...")

            # -- Criação dos Schemas
            query = "CREATE SCHEMA IF NOT EXISTS silver;"

            self.db.executa_query(query, commit=True)

            query = "CREATE SCHEMA IF NOT EXISTS gold;"

            self.db.executa_query(query, commit=True)

            query = "CREATE SCHEMA IF NOT EXISTS meta;"

            self.db.executa_query(query, commit=True)

            # -- Tabela: Dólar diário (coleta diária)
            query = """
            CREATE TABLE IF NOT EXISTS silver.cambio_diario_yuan (
                data DATE PRIMARY KEY,
                par_moeda TEXT NOT NULL,
                bid NUMERIC,
                ask NUMERIC,
                high NUMERIC,
                low NUMERIC,
                var_bid NUMERIC,
                pct_change NUMERIC,
                preco_medio NUMERIC,
                spread NUMERIC,
                amplitude_pct NUMERIC,
                fechamento_anterior NUMERIC,
                var_dia_real NUMERIC,
                var_dia_pct NUMERIC
            );"""

            self.db.executa_query(query, commit=True)

            # -- Tabela: Dólar diário (coleta diária)
            query = """
            CREATE TABLE IF NOT EXISTS silver.cambio_diario_peso (
                data DATE PRIMARY KEY,
                par_moeda TEXT NOT NULL,
                bid NUMERIC,
                ask NUMERIC,
                high NUMERIC,
                low NUMERIC,
                var_bid NUMERIC,
                pct_change NUMERIC,
                preco_medio NUMERIC,
                spread NUMERIC,
                amplitude_pct NUMERIC,
                fechamento_anterior NUMERIC,
                var_dia_real NUMERIC,
                var_dia_pct NUMERIC
            );"""

            self.db.executa_query(query, commit=True)

            # -- Tabela: Dólar diário (coleta diária)
            query = """
            CREATE TABLE IF NOT EXISTS silver.cambio_diario_libra (
                data DATE PRIMARY KEY,
                par_moeda TEXT NOT NULL,
                bid NUMERIC,
                ask NUMERIC,
                high NUMERIC,
                low NUMERIC,
                var_bid NUMERIC,
                pct_change NUMERIC,
                preco_medio NUMERIC,
                spread NUMERIC,
                amplitude_pct NUMERIC,
                fechamento_anterior NUMERIC,
                var_dia_real NUMERIC,
                var_dia_pct NUMERIC
            );"""

            self.db.executa_query(query, commit=True)

            # -- Tabela: Dólar diário (coleta diária)
            query = """
            CREATE TABLE IF NOT EXISTS silver.cambio_diario_euro (
                data DATE PRIMARY KEY,
                par_moeda TEXT NOT NULL,
                bid NUMERIC,
                ask NUMERIC,
                high NUMERIC,
                low NUMERIC,
                var_bid NUMERIC,
                pct_change NUMERIC,
                preco_medio NUMERIC,
                spread NUMERIC,
                amplitude_pct NUMERIC,
                fechamento_anterior NUMERIC,
                var_dia_real NUMERIC,
                var_dia_pct NUMERIC
            );"""

            self.db.executa_query(query, commit=True)

            # -- Tabela: Dólar diário (coleta diária)
            query = """
            CREATE TABLE IF NOT EXISTS silver.cambio_diario_dolar (
                data DATE PRIMARY KEY,
                par_moeda TEXT NOT NULL,
                bid NUMERIC,
                ask NUMERIC,
                high NUMERIC,
                low NUMERIC,
                var_bid NUMERIC,
                pct_change NUMERIC,
                preco_medio NUMERIC,
                spread NUMERIC,
                amplitude_pct NUMERIC,
                fechamento_anterior NUMERIC,
                var_dia_real NUMERIC,
                var_dia_pct NUMERIC
            );"""

            self.db.executa_query(query, commit=True)

            # -- Tabela: SELIC (coleta diária ou mensal, dependendo da fonte)
            query = """
            CREATE TABLE IF NOT EXISTS silver.selic (
                data DATE PRIMARY KEY,
                valor NUMERIC
            );"""

            self.db.executa_query(query, commit=True)

            # -- Tabela: cdi (coleta diária ou mensal, dependendo da fonte)
            query = """
            CREATE TABLE IF NOT EXISTS silver.cdi (
                data DATE PRIMARY KEY,
                valor NUMERIC
            );"""

            self.db.executa_query(query, commit=True)

            # -- Tabela: igpm (coleta diária ou mensal, dependendo da fonte)
            query = """
            CREATE TABLE IF NOT EXISTS silver.igpm (
                data DATE PRIMARY KEY,
                valor NUMERIC
            );"""

            self.db.executa_query(query, commit=True)

            # -- Tabela: IPCA (índice de preços ao consumidor - mensal)
            query = """
            CREATE TABLE IF NOT EXISTS silver.ipca (
                data DATE PRIMARY KEY,
                valor NUMERIC
            );"""

            self.db.executa_query(query, commit=True)

            # -- Tabela: PIB (trimestral ou anual, dependendo da granularidade)
            query = """
            CREATE TABLE IF NOT EXISTS silver.pib_trimestral (
                data DATE PRIMARY KEY,
                valor NUMERIC
            );"""

            self.db.executa_query(query, commit=True)

            # -- Tabela: Juros EUA (Fed Funds Rate - coleta mensal)
            query = """
            CREATE TABLE IF NOT EXISTS silver.juros_usa_fedfunds (
                data DATE PRIMARY KEY,
                valor NUMERIC
            );"""

            self.db.executa_query(query, commit=True)

            # -- Tabela: Juros EUA (Fed Funds Rate - coleta diária)
            query = """
            CREATE TABLE IF NOT EXISTS silver.juros_usa_effr (
                data DATE PRIMARY KEY,
                valor NUMERIC
            );"""

            self.db.executa_query(query, commit=True)

            # -- Tabela: Ibovespa (índice diário de fechamento)
            query = """
            CREATE TABLE IF NOT EXISTS silver.ibovespa_diario (
                data DATE PRIMARY KEY,
                preco_abertura NUMERIC,
                preco_minimo NUMERIC,
                preco_maximo NUMERIC,
                preco_fechamento NUMERIC,
                volume_negociado NUMERIC,
                media_movel_50 NUMERIC,
                media_movel_200 NUMERIC
            );"""

            self.db.executa_query(query, commit=True)

            # -- Versões gold (tabelas preparadas, normalizadas e prontas para consumo/ML)
            query = """
            CREATE TABLE IF NOT EXISTS gold.macro_indicadores (
                data DATE PRIMARY KEY,
                selic NUMERIC,
                ipca NUMERIC,
                pib NUMERIC,
                juros_usa NUMERIC,
                dolar NUMERIC,
                ibovespa NUMERIC
            );"""

            self.db.executa_query(query, commit=True)

            # -- Versões meta (Controle de tabelas populadas ou não)
            query = """
            CREATE TABLE IF NOT EXISTS meta.controle_populacao (
                schema_nome     TEXT NOT NULL,
                tabela_nome     TEXT NOT NULL,
                nome_serie      TEXT NOT NULL,
                carga_inicial DATE,
                ultima_execucao DATE,
                proxima_execucao DATE,
                observacao      TEXT,
                PRIMARY KEY (schema_nome, tabela_nome)
            );"""

            self.db.executa_query(query, commit=True)

            query = """
            CREATE TABLE IF NOT EXISTS silver.noticias (
                id SERIAL PRIMARY KEY,
                cod_bolsa TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                titulo TEXT,
                descricao TEXT,
                data_historico DATE NOT NULL,
                url TEXT,
                sentimento TEXT);
            """

            self.db.executa_query(query, commit=True)

            query = """
            CREATE TABLE IF NOT EXISTS silver.cotacao_pregao (
                datatime timestamptz,
                ativo TEXT NOT NULL,
                preco_abertura NUMERIC,
                preco_minimo NUMERIC,
                preco_maximo NUMERIC,
                preco_fechamento NUMERIC,
                volume_negociado NUMERIC,
                media_movel_50 NUMERIC,
                media_movel_200 NUMERIC,
                origem TEXT NOT NULL,
                PRIMARY KEY (ativo, datatime)
            );"""

            self.db.executa_query(query, commit=True)

            self.logger.info(
                f"Tabelas e SCHEMAS verificadas com sucesso!")

        except Exception as e:
            self.logger.error(
                f"Não foi possível verificar as tabelas: {e}")
            raise

    def last_date(self, camada, tabela, nome_serie, campo_data):

        # verifica a data da última informação inserida em determinada tabela.
        try:
            query = """
                SELECT MAX(%s) FROM %s.%s WHERE nome_serie = %s;
            """
            valores = (campo_data, camada, tabela, nome_serie)

            last_date = self.db.fetch_data(
                query=query, valores=valores, tipo_fetch="one")

            return last_date[0] if last_date and last_date[0] is not None else None

        except Exception as e:
            self.logger.error(
                f"Não foi possível verificar as tabelas: {e}")

    def register_populated(self, camada, tabela, nome_serie, inicial, data_exec, prox_data, obs):
        """
        Registra ou atualiza o status de população de uma tabela no schema 'meta'.

        Parameters
        ----------
        camada : str
            Nome do schema da tabela monitorada.
        tabela : str
            Nome da tabela monitorada.
        status : bool
            True se a tabela já foi populada, False caso contrário.
        data_populated : date
            Data da última população.
        observation : str
            Comentário ou observação adicional.
        """

        self.logger.info(
            f"Atualizando informações sobre população para {camada}.{tabela}")

        query = """
                INSERT INTO meta.controle_populacao (schema_nome, tabela_nome, nome_serie, carga_inicial, ultima_execucao, proxima_execucao, observacao) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (schema_nome, tabela_nome)
                DO UPDATE SET
                    proxima_execucao = EXCLUDED.proxima_execucao,
                    ultima_execucao = EXCLUDED.ultima_execucao,
                    observacao = EXCLUDED.observacao;
               """

        valores = (camada, tabela, nome_serie, inicial,
                   data_exec, prox_data, obs)

        try:
            self.db.executa_query(query, valores=valores, commit=True)
            self.logger.info(
                f"Dados atualizados com sucesso para: {nome_serie}")
            return True

        except Exception as e:
            self.logger.error(
                f"Erro ao atualizar/inserir dados sobre tabelas populadas em: {camada}.{tabela}. Detalhes: {e}")
        return False

    def last_pop(self, camada, tabela, nome_serie):
        """
        Verifica se a tabela já foi populada com base no controle em meta.controle_populacao.

        args:
            camada: str = o nome da camada = 'silver' ou 'gold'
            tabela: str = o nome da tabela a ser validada


        Returns
        -------
            return: bool or None
        """

        query = f"SELECT proxima_execucao FROM {camada}.{tabela} WHERE nome_serie = %s;"
        valores = (nome_serie,)

        try:
            quando = self.db.fetch_data(
                query=query, valores=valores, tipo_fetch="one")

            if quando is None:
                return None
            else:
                proxima_execucao = quando[0]
                hoje = datetime.today().date()
                return hoje >= proxima_execucao

        except Exception as e:
            self.logger.error(
                f"Falha ao validar população de {camada}.{tabela}. Detalhes: {e}")
            return None
