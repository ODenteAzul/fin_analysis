from utils.json_loader import carregar_lista_json
from datetime import datetime


class TableChecker():
    def __init__(self,
                 logger,
                 db,
                 ddl_creator):
        self.logger = logger
        self.db = db
        self.ddl_creator = ddl_creator

    def table_writer(self, camada="", tabela="", sql_file=""):
        """
            Renderiza e executa um template SQL com os parâmetros fornecidos.

            Args:
                 camada (str): Schema destino (obrigatório).
                sql_file (str): Caminho relativo ao template SQL (obrigatório).
                tabela (str): Nome da tabela, se aplicável (opcional).
        """

        if not camada:
            raise ValueError(
                "O atributo 'camada' não pode estar vazio.")
        if not sql_file:
            raise ValueError(
                "O atributo 'sql_file' não pode estar vazio.")

        template = self.ddl_creator.load_template(sql_file)

        params = {"camada": camada}
        if tabela:
            params["tabela"] = tabela

        query = self.ddl_creator.render_template(template, **params)
        self.db.executa_query(query, commit=True)

    def check_tables(self):

        try:

            self.logger.info(
                "Iniciando verificação das tabelas e SCHEMAS...")

            # -- Criação dos Schemas

            ls_schemas = carregar_lista_json("config/schemas.json")

            for schema in ls_schemas:

                self.logger.info(
                    f"Criando/verificando schema: {schema}")

                self.table_writer(
                    camada=schema,
                    sql_file="schema.sql")

            # Cotação diária Moedas

            ls_moedas = carregar_lista_json("config/moedas.json")

            for moeda in ls_moedas:

                self.logger.info(
                    f"Criando/verificando tabela: silver.{moeda['tabela']}")
                self.table_writer(
                    camada="silver",
                    tabela=moeda["tabela"],
                    sql_file="cotacao_diaria.sql")

            # INDICES/INDICADORES

            ls_indices = carregar_lista_json("config/indicadores.json")

            for indice in ls_indices:

                self.logger.info(
                    f"Criando/verificando tabela: silver.{indice['tabela']}")
                self.table_writer(
                    camada="silver",
                    tabela=indice["tabela"],
                    sql_file="indice.sql")

            # JUROS EUA

            ls_juros = carregar_lista_json("config/juros_eua.json")

            for indice in ls_juros:

                self.logger.info(
                    f"Criando/verificando tabela: silver.{indice['tabela']}")
                self.table_writer(
                    camada="silver",
                    tabela=indice["tabela"],
                    sql_file="indice.sql")

            # -- Tabela: Ibovespa (índice diário de fechamento)
            self.logger.info(
                "Criando/verificando tabela: silver.ibovespa_diario")

            self.table_writer(
                camada="silver",
                tabela="ibovespa_diario",
                sql_file="ibovespa_diario.sql")

            # NOTICIAS
            self.logger.info(
                "Criando/verificando tabela: silver.noticias")

            self.table_writer(
                camada="silver",
                tabela="noticias",
                sql_file="noticias.sql")

            # Cotações INTRA DIARIO
            self.logger.info(
                "Criando/verificando tabela: silver.cotacao_intra_diario")

            self.table_writer(
                camada="silver",
                tabela="cotacao_intra_diario",
                sql_file="cotacao_intra_diario.sql")

            # Versões gold:(tabelas preparadas, normalizadas
            # e prontas para consumo/ML)
            self.logger.info(
                "Criando/verificando tabela: gold.macro_indicadores")

            self.table_writer(
                camada="gold",
                tabela="macro_indicadores",
                sql_file="macro_indicadores.sql")

            self.logger.info(
                "Tabelas e SCHEMAS verificadas com sucesso!")

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

    def register_populated(
        self,
        camada,
        tabela,
        nome_serie,
        inicial,
        data_exec,
        prox_data,
        obs
    ):
        """
        Registra ou atualiza o status de população
        de uma tabela no schema 'meta'.

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
                INSERT INTO meta.controle_populacao (
                schema_nome,
                tabela_nome,
                nome_serie,
                carga_inicial,
                ultima_execucao,
                proxima_execucao,
                observacao)
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
                f"""Erro ao atualizar/inserir dados sobre
                tabelas populadas em: {camada}.{tabela}. Detalhes: {e}""")
        return False

    def last_pop(self, camada, tabela, nome_serie):
        """
        Verifica se a tabela já foi populada com
        base no controle em meta.controle_populacao.

        args:
            camada: str = o nome da camada = 'silver' ou 'gold'
            tabela: str = o nome da tabela a ser validada


        Returns
        -------
            return: bool or None
        """

        query = f"""SELECT proxima_execucao
        FROM {camada}.{tabela} WHERE nome_serie = %s;"""
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
                f"""Falha ao validar população
                de {camada}.{tabela}. Detalhes: {e}""")
            return None
