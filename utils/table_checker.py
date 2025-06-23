from typing import Union, Optional, Tuple


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
                f"Iniciando verificação das tabelas...")

            # -- Criação dos Schemas
            query = "CREATE SCHEMA IF NOT EXISTS silver;"

            self.db.executa_query(query, commit=True)

            query = "CREATE SCHEMA IF NOT EXISTS gold;"

            self.db.executa_query(query, commit=True)

            query = "CREATE SCHEMA IF NOT EXISTS meta;"

            self.db.executa_query(query, commit=True)

            # -- Tabela: Dólar diário (coleta diária)
            query = """
            CREATE TABLE IF NOT EXISTS silver.dolar_diario (
                data DATE PRIMARY KEY,
                bid NUMERIC,
                ask NUMERIC,
                high NUMERIC,
                low NUMERIC,
                var_bid NUMERIC,
                pct_change NUMERIC
            );"""

            self.db.executa_query(query, commit=True)

            # -- Tabela: SELIC (coleta diária ou mensal, dependendo da fonte)
            query = """
            CREATE TABLE IF NOT EXISTS silver.selic (
                data DATE PRIMARY KEY,
                valor NUMERIC
            );"""

            self.db.executa_query(query, commit=True)

            # -- Tabela: IPCA (índice de preços ao consumidor - mensal)
            query = """
            CREATE TABLE IF NOT EXISTS silver.ipca_mensal (
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

            # -- Tabela: Juros EUA (Fed Funds Rate - coleta diária ou após FOMC)
            query = """
            CREATE TABLE IF NOT EXISTS silver.juros_usa (
                data DATE PRIMARY KEY,
                valor NUMERIC
            );"""

            self.db.executa_query(query, commit=True)

            # -- Tabela: Ibovespa (índice diário de fechamento)
            query = """
            CREATE TABLE IF NOT EXISTS silver.ibovespa_diario (
                data DATE PRIMARY KEY,
                valor NUMERIC
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
                empresa         TEXT NOT NULL,
                populado        BOOLEAN DEFAULT FALSE,
                data_populacao  DATE,
                observacao      TEXT,
                PRIMARY KEY (schema_nome, tabela_nome)
            );"""

            self.db.executa_query(query, commit=True)

            query = """
            CREATE TABLE IF NOT EXISTS noticias (
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

            self.logger.info(
                f"Tabelas verificadas com sucesso!")

        except Exception as e:
            self.logger.error(
                f"Não foi possível verificar as tabelas: {e}")
            raise

    def last_date(self, camada, tabela, empresa, campo_data):

        # verifica a data da última informação inserida em determinada tabela.
        try:
            query = """
                SELECT MAX(%s) FROM %s.%s WHERE empresa = %s;
            """
            valores = (campo_data, camada, tabela, empresa)

            last_date = self.db.fetch_data(
                query=query, valores=valores, tipo_fetch="one")

            return last_date[0] if last_date and last_date[0] is not None else None

        except Exception as e:
            self.logger.error(
                f"Não foi possível verificar as tabelas: {e}")

    def register_populated(self, camada, tabela, empresa, status, data_populated, observation):
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
            f"Atualizando informação sobre carga da tabela: {camada}.{tabela} para empresa {empresa}")

        query = """
                INSERT INTO meta.controle_populacao (schema_nome, tabela_nome, empresa, populado, data_populacao, observacao) 
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (schema_nome, tabela_nome)
                DO UPDATE SET
                    populado = EXCLUDED.populado,
                    data_populacao = EXCLUDED.data_populacao,
                    observacao = EXCLUDED.observacao;
            """

        valores = (camada, tabela, empresa, status,
                   data_populated, observation)

        try:
            self.db.executa_query(query, valores=valores, commit=True)
            self.logger.info("Dados atualizados com sucesso.")
            return True

        except Exception as e:
            self.logger.error(
                f"Erro ao atualizar/inserir dados sobre tabelas populadas em: {camada}.{tabela}. Detalhes: {e}")
        return False

    def check_populated(self, camada, tabela, empresa, resposta: str = 'bool') -> Union[bool, Optional[Tuple]]:
        """
        Verifica se a tabela já foi populada com base no controle em meta.controle_populacao.

        args:
            camada: str = o nome da camada = 'silver' ou 'gold'
            tabela: str = o nome da tabela a ser validada
            resposta: str = se 'bool': apenas verifica se a tabela consta populata
                            se 'dados': devolve o registro completo sobre a tabela

        Returns
        -------
            se resposta = 'bool' = bool presente no registro referente a essa tabela.
            se resposta = 'dados' = tuple or None
                Retorna os dados do registro (se houver) ou None se não encontrado.
        """
        if resposta not in ("bool", "dados"):
            raise ValueError(
                "O parâmetro 'resposta' deve ser 'bool' ou 'dados'.")

        if resposta == 'bool':
            query = """
                SELECT populado FROM meta.controle_populacao
                WHERE schema_nome = %s AND tabela_nome = %s AND empresa = %s;
            """

            try:
                dados = self.db.fetch_data(query=query, valores=(
                    camada, tabela, empresa), tipo_fetch="one")
                if dados and len(dados) > 0:
                    return dados[0]
                else:
                    return False

            except Exception as e:
                self.logger.error(
                    f"Falha ao validar população de {camada}.{tabela}. Detalhes: {e}")
                return False

        else:
            query = """
                SELECT * FROM meta.controle_populacao
                WHERE schema_nome = %s AND tabela_nome = %s AND empresa = %s;
            """

            try:
                dados = self.db.fetch_data(query=query, valores=(
                    camada, tabela, empresa), tipo_fetch="one")
                return dados

            except Exception as e:
                self.logger.error(
                    f"Falha ao validar população de {camada}.{tabela}. Detalhes: {e}")
                return False
