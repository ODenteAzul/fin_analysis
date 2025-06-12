import psycopg2
import psycopg2.extras
import pandas as pd


class PostGreSQL():
    def __init__(self,
                 dbname="scrappingdb",
                 user="sirefelps",
                 password="Luisabc@123",
                 host="192.168.0.212",
                 port="5432",
                 logger=None):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.logger = logger
        self.conn = None
        self.cursor = None

    def conectar(self):
        try:
            self.logger.info(
                "Conectando com o PostgreSQL...")
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.cursor = self.conn.cursor(
                cursor_factory=psycopg2.extras.DictCursor)
            self.logger.info(
                "Conexão com PostgreSQL estabelecida com sucesso.")
            return self.conn, self.cursor
        except psycopg2.Error as e:
            self.logger.error(f"Erro ao conectar ao PostgreSQL: {e}")
            return None, None

    def fechar_conexao(self):
        self.logger.info(
            "Fechando conexões...")
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            self.logger.info("Conexão com PostgreSQL fechada.")

    def executa_query(self, query, valores=None, commit=False, many=False):
        try:
            if valores:
                if many:
                    self.cursor.executemany(query, valores)
                else:
                    self.cursor.execute(query, valores)
            elif not many:
                self.cursor.execute(query)

            if commit:
                self.conn.commit()

            return True

        except psycopg2.OperationalError as e:
            self.logger.error(f"Erro de conexão ao executar a query: {e}")
            return False
        except psycopg2.ProgrammingError as e:
            self.logger.error(f"Erro de sintaxe SQL: {e}")
            return False
        except psycopg2.Error as e:
            self.logger.error(f"Erro geral do banco de dados: {e}")
            return False

    def fetch_data(self, query, valores=None, tipo_fetch=None, n_linhas=0):

        dados = None

        try:
            if valores:
                self.cursor.execute(query, valores)
            else:
                self.cursor.execute(query)

            if tipo_fetch == "one":
                dados = self.cursor.fetchone()
                if not dados:
                    return None
            elif tipo_fetch == "many":
                dados = self.cursor.fetchmany(n_linhas)
                if not dados:
                    return []
            elif tipo_fetch == "all":
                dados = self.cursor.fetchall()
                if not dados:
                    return []

            return dados

        except psycopg2.Error as e:
            self.logger.error(f"Erro ao obter os dados: {e}")
