import yfinance as yf
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from dateutil.relativedelta import relativedelta
import pandas as pd
import jbridgedf as jdf
from config.ambience import EnvConfig
from config.json_loader import carregar_lista_json


class ScrappIndices():
    def __init__(self,
                 logger,
                 db,
                 table_checker,
                 controle):
        self.logger = logger
        self.db = db
        self.conn = conn
        self.cursor = cursor
        self.table_checker = table_checker
        self.controle = controle

    def _to_float(self, valor):
        try:
            return float(str(valor).replace(",", "."))
        except (ValueError, TypeError):
            return None

    def _calcula_proxima_execucao(self,
                                  data_base: date,
                                  frequencia: str) -> date:
        """
        Calcula a próxima data de execução com base na frequência informada.

        Parâmetros
        ----------
        data_base : datetime.date
            Data de referência para o cálculo
            (geralmente a data de hoje ou da última execução).
        frequencia : str
            Frequência da série: 'diaria', 'mensal', 'trimestral'.

        Retorno
        -------
        datetime.date
            Data da próxima execução programada.
        """
        try:
            frequencia = frequencia.lower()

            if frequencia == "diaria":
                return data_base + timedelta(days=1)

            elif frequencia == "mensal":
                return (data_base.replace(day=1) + relativedelta(months=1))

            elif frequencia == "trimestral":
                mes_atual = data_base.month
                mes_inicio_trimestre = 1 + 3 * ((mes_atual - 1) // 3)
                data_inicio_trimestre = data_base.replace(
                    month=mes_inicio_trimestre, day=1)

                return data_inicio_trimestre + relativedelta(months=3)

            else:
                return data_base + timedelta(days=1)

        except Exception as e:
            self.logger.error(
                f"Não foi possível calcular a próxima data de execução: {e}")

    def colheira_diaria(self):

        self.logger.info(
            "Buscando atualizações de índices econômicos..")

        try:

            if self.controle == 'indices':

                # busca os índices que normalmente são divulgados pela manhã

                indicadores = carregar_lista_json("config/indicadores.json")

                juros_eua = carregar_lista_json("config/juros_eua.json")

                for ind in indicadores:

                    self.logger.info(
                        f"Verificando {ind['tabela'].upper()}...")

                    atualizar = self.table_checker.last_pop(
                        camada='meta',
                        tabela='controle_populacao',
                        nome_serie=ind['tabela'])

                    if atualizar is None:

                        self.logger.info(
                            (f"Sem dados para {ind['tabela'].upper()},"
                             f" buscando primeira carga..."))

                        self._atualiza_sgs_bacen(
                            codigo_sgs=ind['codigo'],
                            hoje=False,
                            camada='silver',
                            tabela=ind['tabela'],
                            frequencia=ind['frequencia']
                        )
                    else:

                        if atualizar:

                            self._atualiza_sgs_bacen(
                                codigo_sgs=ind['codigo'],
                                hoje=True,
                                camada='silver',
                                tabela=ind['tabela'],
                                frequencia=ind['frequencia']
                            )

                        else:

                            self.logger.info(
                                (f"Dados do {ind['tabela'].upper()},"
                                 f" estão atualizados."))

                for juros in juros_eua:

                    atualizar = self.table_checker.last_pop(
                        camada='meta',
                        tabela='controle_populacao',
                        nome_serie=juros['tabela'])

                    if atualizar is None:

                        self.logger.info(
                            f"Verificando histórico de {juros['tabela']}...")

                        self._atualiza_juros_eua(
                            serie=juros['serie'],
                            hoje=False,
                            camada='silver',
                            tabela=juros['tabela'],
                            frequencia=juros['frequencia']
                        )

                    else:

                        if atualizar:

                            self._atualiza_juros_eua(
                                serie=juros['serie'],
                                hoje=True,
                                camada='silver',
                                tabela=juros['tabela'],
                                frequencia=juros['frequencia']
                            )

                        else:

                            self.logger.info(
                                (f"Dados para {juros['tabela']},"
                                 f" estão atualizados."))

            else:

                # fechamento diários, IBOVESPA, MOEDAS

                self.logger.info(
                    "Verificando fechamentos IBOVESPA...")

                atualizar = self.table_checker.last_pop(
                    camada='meta',
                    tabela='controle_populacao',
                    nome_serie='ibovespa_diario')

                if atualizar is None:

                    self.logger.info(
                        "Verificando histórico IBOVESPA...")

                    self._atualiza_serie_ibovespa(hoje=False)

                else:

                    if atualizar:

                        self._atualiza_serie_ibovespa(hoje=True)

                    else:

                        self.logger.info(
                            "Dados para IBOVESPA, estão atualizados.")

                pares_moeda = carregar_lista_json("config/moedas.json")

                for par in pares_moeda:

                    self.logger.info(
                        f"Verificando histórico de cotação: {par['par']}...")

                    atualizar = self.table_checker.last_pop(
                        camada='meta',
                        tabela='controle_populacao',
                        nome_serie=par['par'])

                    if atualizar is None:

                        self._atualiza_cambio(
                            par_moeda=par['par'],
                            camada='silver',
                            tabela=par['tabela'],
                            hoje=False)

                    else:

                        if atualizar:

                            self._atualiza_cambio(
                                par_moeda=par['par'],
                                camada='silver',
                                tabela=par['tabela'],
                                hoje=True)

                        else:

                            self.logger.info(
                                f"Dados para {par['par']}, estão atualizados.")

            self.logger.info(
                "Coleta de dados históricos efetuada com sucesso.")

        except Exception as e:
            self.logger.error(
                f"Não foi possível obter os índices: {e}")

    def _atualiza_serie_ibovespa(self, hoje=True):
        try:
            ibov = yf.Ticker("^BVSP")
            br_time = datetime.now(ZoneInfo("America/Sao_Paulo"))
            hoje_data = br_time.date()
            sucesso = False

            if hoje:
                hoje_str = hoje_data.strftime("%Y-%m-%d")
                data_inicial = None
                pop_string = "Atualização do valor de fechamento IBOVESPA"

                df_ibov = ibov.history(
                    start=hoje_str, end=hoje_str, interval="1d")

                if (not df_ibov.empty
                    and df_ibov.index[-1].date() == br_time.date()
                        and df_ibov is not None):
                    linha = df_ibov.iloc[-1]
                    query = """INSERT INTO silver.ibovespa_diario (
                                                data,
                                                preco_abertura,
                                                preco_minimo,
                                                preco_maximo,
                                                preco_fechamento,
                                                volume_negociado,
                                                media_movel_50,
                                                media_movel_200)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                    valores = [(
                        linha.name.date(),
                        self._to_float(float(linha["Open"])),
                        self._to_float(float(linha["Low"])),
                        self._to_float(float(linha["High"])),
                        self._to_float(float(linha["Close"])),
                        int(linha["Volume"]) if not pd.isna(
                            linha["Volume"]) else 0,
                        None,
                        None
                    )]

                    self.db.executa_query(query, valores, commit=True)

                    sucesso = True

                    try:
                        query = """WITH subquery AS (
                                        SELECT data,
                                            AVG(preco_fechamento) OVER
                                 (ORDER BY data ROWS BETWEEN 49 PRECEDING
                                 AND CURRENT ROW) AS media_50,
                                            AVG(preco_fechamento) OVER
                                 (ORDER BY data ROWS BETWEEN 199 PRECEDING
                                 AND CURRENT ROW) AS media_200
                                        FROM silver.ibovespa_diario
                                        WHERE media_movel_50 IS NULL OR
                                 media_movel_200 IS NULL
                                    )
                                    UPDATE silver.ibovespa_diario AS p
                                    SET media_movel_50 =
                                 COALESCE(subquery.media_50, p.media_movel_50),
                                        media_movel_200 =
                                 COALESCE(
                                            subquery.media_200,
                                 p.media_movel_200)
                                    FROM subquery
                                    WHERE p.data = subquery.data;"""

                        self.db.executa_query(query, commit=True)

                    except Exception as e:
                        self.logger.error(
                            (f"Não foi possível calcular médias"
                             f" móveis para IBOVESPA, erro: {e}"))

                else:
                    self.logger.info(
                        "IBOVESPA não retornou dados — agendada para retry.")

                    proxima = hoje_data + timedelta(days=1)

                    self.table_checker.register_populated(
                        camada='silver',
                        tabela='ibovespa_diario',
                        nome_serie='ibovespa_diario',
                        inicial=data_inicial,
                        data_exec=hoje_data,
                        prox_data=proxima,
                        obs="Dado não retornado — agendado para retry."
                    )

            else:
                data_inicial = hoje_data
                pop_string = "Carga inicial IBOVESPA"

                historico = ibov.history(period="10y")
                historico.index = pd.to_datetime(historico.index)
                historico.reset_index(inplace=True)

                if historico is not None and not historico.empty:
                    query = """INSERT INTO silver.ibovespa_diario (
                                                    data,
                                                    preco_abertura,
                                                    preco_minimo,
                                                    preco_maximo,
                                                    preco_fechamento,
                                                    volume_negociado,
                                                    media_movel_50,
                                                    media_movel_200)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                    valores = [
                        (row["Date"].date(),
                         self._to_float(float(row["Open"])),
                         self._to_float(float(row["Low"])),
                         self._to_float(float(row["High"])),
                         self._to_float(float(row["Close"])),
                         int(row["Volume"]) if not pd.isna(
                            row["Volume"]) else 0,
                            None,
                            None)
                        for _, row in historico.iterrows()]

                    self.db.executa_query(
                        query, valores, commit=True, many=True)

                    sucesso = True

                    self.logger.info("Valores IBOVESPA obtido com sucesso.")

                    try:
                        query = """UPDATE silver.ibovespa_diario AS p
                                        SET media_movel_50 = COALESCE
                                            (subquery.media_50,
                                            p.media_movel_50),
                                            media_movel_200 = COALESCE(
                                                subquery.media_200,
                                                p.media_movel_200)
                                        FROM (
                                            SELECT data,
                                                AVG(preco_fechamento) OVER
                                 (ORDER BY data ROWS BETWEEN 49 PRECEDING AND
                                 CURRENT ROW) AS media_50,
                                                AVG(preco_fechamento) OVER
                                 (ORDER BY data ROWS BETWEEN 199 PRECEDING AND
                                 CURRENT ROW) AS media_200
                                        FROM silver.ibovespa_diario
                                        ) AS subquery
                                        WHERE p.data = subquery.data;"""

                        self.db.executa_query(query, commit=True)

                        self.logger.info(
                            "Médias móveis do IBOVESPA:"
                            "calculados com sucesso.")

                    except Exception as e:
                        self.logger.error(
                            "Não foi possível calcular médias móveis para: "
                            f"IBOVESPA, erro: {e}")

            if sucesso:

                proxima = self._calcula_proxima_execucao(hoje_data, 'diaria')

                self.table_checker.register_populated(
                    camada='silver',
                    tabela='ibovespa_diario',
                    nome_serie='ibovespa_diario',
                    inicial=data_inicial,
                    data_exec=hoje_data,
                    prox_data=proxima,
                    obs=pop_string
                )

            else:

                if hoje:

                    self.logger.warning(
                        "IBOVESPA não retornou dados do fechamento atual:"
                        "agendada para retry.")

                    self.table_checker.register_populated(
                        camada='silver',
                        tabela='ibovespa_diario',
                        nome_serie='ibovespa_diario',
                        inicial=data_inicial,
                        data_exec=hoje_data,
                        prox_data=hoje_data,
                        obs="Dado não retornado — agendado para retry."
                    )

                else:

                    self.logger.warning(
                        "Não foi possível obter a carga inicial para IBOVESPA"
                        "Agendada para retry.")

        except Exception as e:
            self.logger.error(
                f"Sem dados para IBOVESPA para {hoje_data}, erro: {e}")

    def _recalcula_variacoes_cambio(self, camada, tabela, hoje, hoje_data):

        try:
            sql = f"""
                UPDATE {camada}.{tabela} c
                SET
                    preco_medio = (c.bid + c.ask) / 2.0,
                    spread = c.ask - c.bid,
                    amplitude_pct = CASE
                        WHEN c.low > 0 THEN ((c.high - c.low) / c.low) * 100
                        ELSE NULL
                    END,
                    fechamento_anterior = (
                        SELECT c2.bid
                        FROM {camada}.{tabela} c2
                        WHERE c2.data = c.data - INTERVAL '1 day'
                    ),
                    var_dia_real = CASE
                        WHEN (
                            SELECT c2.bid
                            FROM {camada}.{tabela} c2
                            WHERE c2.data = c.data - INTERVAL '1 day'
                        ) IS NOT NULL
                        THEN c.bid - (
                            SELECT c2.bid
                            FROM {camada}.{tabela} c2
                            WHERE c2.data = c.data - INTERVAL '1 day'
                        )
                        ELSE NULL
                    END,
                    var_dia_pct = CASE
                        WHEN (
                            SELECT c2.bid
                            FROM {camada}.{tabela} c2
                            WHERE c2.data = c.data - INTERVAL '1 day'
                        ) > 0
                        THEN ((c.bid - (
                            SELECT c2.bid
                            FROM {camada}.{tabela} c2
                            WHERE c2.data = c.data - INTERVAL '1 day'
                        )) / (
                            SELECT c2.bid
                            FROM {camada}.{tabela} c2
                            WHERE c2.data = c.data - INTERVAL '1 day'
                        )) * 100
                        ELSE NULL
                    END
            """

            if hoje:
                sql += f"\nWHERE c.data = '{hoje_data.strftime('%Y-%m-%d')}';"
            else:
                sql += """
                WHERE
                    preco_medio IS NULL OR
                    spread IS NULL OR
                    amplitude_pct IS NULL OR
                    fechamento_anterior IS NULL OR
                    var_dia_real IS NULL OR
                    var_dia_pct IS NULL;
                """

            self.db.executa_query(sql, commit=True)

        except Exception as e:
            self.logger.error(
                f"Erro ao calcular as variações de Cambio: {e}")

    def _atualiza_cambio(self, par_moeda, camada, tabela, hoje=True):

        if par_moeda is None:
            raise TypeError(
                "O parâmetro 'par_moeda' deve ser informado.")

        if camada is None:
            raise TypeError(
                "O parâmetro 'camada' deve ser informado.")

        if tabela is None:
            raise TypeError(
                "O parâmetro 'tabela' deve ser informado.")

        TOKEN = EnvConfig.AWESOME_API_KEY

        try:
            br_time = datetime.now(ZoneInfo("America/Sao_Paulo"))
            hoje_data = br_time.date()
            if hoje:
                is_list = False
                many = False
                pop_string = f"Atualizando os dados diários de {par_moeda}."
                data_inicial = None
                url = (f"https://economia.awesomeapi.com.br/json/"
                       f"last/{par_moeda}?token={TOKEN}")
                self.logger.info(
                    f"Iniciando coleta do fechamento do {par_moeda}...")
            else:
                is_list = True
                many = True
                pop_string = f"Carga Inicial de {par_moeda}."
                data_inicial = hoje_data
                dias_passados = (
                    hoje_data - (hoje_data - relativedelta(years=10))).days
                start_date = hoje_data - relativedelta(years=10)
                final_date = hoje_data .strftime("%Y%m%d")
                start_date = start_date.strftime("%Y%m%d")
                url = (f"https://economia.awesomeapi.com.br/json/"
                       f"daily/{par_moeda}/{dias_passados}?token="
                       f"{TOKEN}&start_date={start_date}"
                       f"&end_date={final_date}")
                self.logger.info(
                    f"Iniciando coleta:{par_moeda} dos últimos 10 anos...")

            self.logger.info(f"URL gerada para {par_moeda}: {url}")

            api_client = jdf.APIDataParser(self.logger)

            df_cambio = api_client.get_from_api(
                url, ['high', 'low', 'varBid', 'pctChange',
                      'bid', 'ask', 'timestamp'],
                is_list=is_list,
                convert_timestamp=True,
                sanitize=True,
                frequency='daily')

            if df_cambio is not None and not df_cambio.empty:

                df_cambio.sort_values("data", inplace=True)

                df_cambio = df_cambio.drop_duplicates(
                    subset=["data"], keep="last"
                )

                colunas_numericas = [
                    "high",
                    "low",
                    "bid",
                    "ask",
                    "varBid",
                    "pctChange",
                    "timestamp"
                ]

                for col in colunas_numericas:
                    if col in df_cambio.columns:
                        df_cambio[col] = df_cambio[col].apply(self._to_float)

                df_cambio.sort_values("data", inplace=True)

                query = f"""INSERT INTO {camada}.{tabela}
                (data, par_moeda, bid, ask,
                            high, low, var_bid, pct_change,
                            preco_medio, spread,
                            amplitude_pct, fechamento_anterior,
                            var_dia_real, var_dia_pct)
                            VALUES (%s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s)"""

                valores = []

                valores = [
                    (row["data"].date(),
                     par_moeda,
                     self._to_float(row["bid"]),
                     self._to_float(row["ask"]),
                     self._to_float(row["high"]),
                     self._to_float(row["low"]),
                     self._to_float(row.get("varBid")),
                     self._to_float(row.get("pctChange")),
                     None,  # preco_medio
                     None,  # spread
                     None,  # amplitude_pct
                     None,  # fechamento_anterior
                     None,  # var_dia_real
                     None)  # var_dia_pct
                    for _, row in df_cambio.iterrows()]

                if not many:
                    if isinstance(valores, list) and len(valores) == 1:
                        valores = valores[0]

                self.db.executa_query(query, valores, commit=True, many=many)

                self._recalcula_variacoes_cambio(
                    camada, tabela, hoje, hoje_data)

                self.logger.info(
                    f"Coleta do par {par_moeda} obtida e gravada com sucesso.")

                proxima = self._calcula_proxima_execucao(hoje_data, 'diaria')

                self.table_checker.register_populated(
                    camada=camada,
                    tabela=tabela,
                    nome_serie=par_moeda,
                    inicial=data_inicial,
                    data_exec=hoje_data,
                    prox_data=proxima,
                    obs=pop_string
                )

            else:

                if hoje:

                    self.logger.warning(
                        f"""{par_moeda} sem dados do fechamento atual.
                        Agendada para retry.""")

                    self.table_checker.register_populated(
                        camada=camada,
                        tabela=tabela,
                        nome_serie=tabela,
                        inicial=data_inicial,
                        data_exec=hoje_data,
                        prox_data=hoje_data,
                        obs="Dado não retornado — agendado para retry."
                    )

                else:

                    self.logger.warning(
                        f"""Sem carga inicial para {par_moeda}.
                        Agendada para retry.""")

        except Exception as e:
            self.logger.error(
                f"Erro ao obter câmbio diário para {par_moeda}: {e}")

    def _atualiza_sgs_bacen(self,
                            codigo_sgs=None,
                            hoje=True,
                            camada=None,
                            tabela=None,
                            frequencia=None):

        if codigo_sgs is None:
            raise TypeError(
                "O parâmetro 'codigo_sgs' deve ser informado.")

        if camada is None:
            raise TypeError(
                "O parâmetro 'camada' deve ser informado.")

        if tabela is None:
            raise TypeError(
                "O parâmetro 'tabela' deve ser informado.e")

        if frequencia is None:
            raise TypeError(
                "O parâmetro 'frequencia' deve ser informado.")

        SERIES = {
            11: "SELIC Diária",
            433: "IPCA Mensal",
            12: "CDI Diária",
            189: "IGP-M Mensal",
            10844: "IPCA Serviços",
            1: "Taxa de Câmbio USD (PTAX)",
            10880: "Reservas Internacionais"
        }

        nome_serie = SERIES.get(codigo_sgs, f"SGS {codigo_sgs}")

        try:

            br_time = datetime.now(ZoneInfo("America/Sao_Paulo"))
            hoje_data = br_time.date()

            if hoje:
                http_get_timeout = 10
                pop_string = f"Atualização diária {nome_serie}"
                if frequencia == 'diaria':
                    start_date = hoje_data - relativedelta(days=5)
                elif frequencia == 'semanal':
                    start_date = hoje_data - relativedelta(weeks=3)
                elif frequencia == 'mensal':
                    start_date = hoje_data - relativedelta(months=3)

                final_date = hoje_data.strftime("%d/%m/%Y")
                start_date = start_date.strftime("%d/%m/%Y")
                data_inicial = None

                url = (f"https://api.bcb.gov.br/dados/serie/"
                       f"bcdata.sgs.{codigo_sgs}/dados?"
                       f"formato=json&dataInicial={start_date}"
                       f"&dataFinal={final_date}")

                self.logger.info(
                    f"Iniciando coleta da {nome_serie} atual")
            else:
                http_get_timeout = 150
                pop_string = f"Carga inicial {nome_serie}"
                start_date = hoje_data - relativedelta(years=10)
                final_date = hoje_data.strftime("%d/%m/%Y")
                start_date = start_date.strftime("%d/%m/%Y")
                data_inicial = hoje_data

                url = (f"https://api.bcb.gov.br/dados/serie/"
                       f"bcdata.sgs.{codigo_sgs}/dados?"
                       f"formato=json&dataInicial={start_date}"
                       f"&dataFinal={final_date}")

                self.logger.info(
                    f"Iniciando coleta da {nome_serie} dos últimos 10 anos.")

            self.logger.info(f"URL gerada para {nome_serie}: {url}")

            api_client = jdf.APIDataParser(self.logger)

            df_indice = api_client.get_from_api(
                url, ['data', 'valor'],
                is_list=True,
                convert_timestamp=False,
                sanitize=True,
                frequency='auto',
                http_get_timeout=http_get_timeout,
                date_format="%d/%m/%Y")

            if df_indice is not None and not df_indice.empty:

                if hoje:
                    resultado = self.db.fetch_data(
                        query=f'SELECT MAX(data) FROM {camada}.{tabela};',
                        tipo_fetch='one')

                    data_last = resultado['max'] if resultado and resultado['max'] else None

                    if isinstance(data_last, datetime):
                        data_last = data_last.date()

                    maior_data = df_indice['data'].max()

                    maior_data = maior_data.date()

                    if maior_data > data_last:

                        nome_tabela = f"{camada}.{tabela}"
                        query = (f"INSERT INTO {nome_tabela}"
                                 f" (data, valor) VALUES (%s, %s);")
                        valores = [
                            (row["data"], self._to_float(row["valor"]))
                            for _, row in df_indice.iterrows()
                            if self._to_float(row["valor"]) is not None
                        ]

                        self.db.executa_query(
                            query, valores, commit=True, many=True)

                        self.logger.info(
                            f"{nome_serie} histórico obtido e gravado com sucesso.")

                        proxima = self._calcula_proxima_execucao(
                            hoje_data, frequencia)

                        self.table_checker.register_populated(
                            camada=camada,
                            tabela=tabela,
                            nome_serie=tabela,
                            inicial=data_inicial,
                            data_exec=hoje_data,
                            prox_data=proxima,
                            obs=pop_string
                        )
                    else:
                        self.logger.warning(
                            f"{nome_serie}: Nenhum novo dado identificado. \
                            Último no banco: {data_last}, \
                            último da API: {maior_data} — agendado para retry.")

                else:
                    nome_tabela = f"{camada}.{tabela}"
                    query = (f"INSERT INTO {nome_tabela}"
                             f" (data, valor) VALUES (%s, %s);")
                    valores = [
                        (row["data"], self._to_float(row["valor"]))
                        for _, row in df_indice.iterrows()
                        if self._to_float(row["valor"]) is not None
                    ]

                    self.db.executa_query(
                        query, valores, commit=True, many=True)

                    self.logger.info(
                        f"{nome_serie} histórico obtido e gravado com sucesso.")

                    proxima = self._calcula_proxima_execucao(
                        hoje_data, frequencia)

                    self.table_checker.register_populated(
                        camada=camada,
                        tabela=tabela,
                        nome_serie=tabela,
                        inicial=data_inicial,
                        data_exec=hoje_data,
                        prox_data=proxima,
                        obs=pop_string
                    )

            else:

                if hoje:

                    self.logger.warning(
                        f"""{nome_serie} Não retornou dados... """)

                    self.table_checker.register_populated(
                        camada=camada,
                        tabela=tabela,
                        nome_serie=tabela,
                        inicial=data_inicial,
                        data_exec=hoje_data,
                        prox_data=hoje_data,
                        obs="Dado não retornado — agendado para retry."
                    )

                else:

                    self.logger.warning(
                        (f"Sem carga inicial para:"
                         f" {nome_serie}agendada para retry."))

        except Exception as e:
            self.logger.error(
                f"Erro ao obter dados do {nome_serie}: {e}")

    def _atualiza_juros_eua(self,
                            serie=None,
                            hoje=True,
                            camada=None,
                            tabela=None,
                            frequencia=None):

        if serie is None:
            raise TypeError(
                "O parâmetro 'serie' deve ser informado.")

        if camada is None:
            raise TypeError(
                "O parâmetro 'camada' deve ser informado.")

        if tabela is None:
            raise TypeError(
                "O parâmetro 'tabela' deve ser informado.")

        if frequencia is None:
            raise TypeError(
                "O parâmetro 'frequencia' deve ser informado.")

        TOKEN = EnvConfig.FRED_API_KEY

        try:

            br_time = datetime.now(ZoneInfo("America/Sao_Paulo"))
            hoje_data = br_time.date()

            if hoje:
                http_get_timeout = 10
                pop_string = f"Atualização diária {serie}"
                only_date = hoje_data.strftime("%d/%m/%Y")
                data_inicial = None

                url = (f"https://api.stlouisfed.org/fred/"
                       f"series_observations?series_id={serie}"
                       f"&api_key={TOKEN}"
                       f"&file_type=json&observation_start={only_date}"
                       f"&observation_end={only_date}")
            else:
                http_get_timeout = 150
                pop_string = f"Carga inicial {serie}"
                data_inicial = hoje_data

                final_date = hoje_data.strftime("%Y-%m-%d")
                start_date = hoje_data - relativedelta(years=10)
                start_date = start_date.strftime("%Y-%m-%d")

                url = (f"https://api.stlouisfed.org/fred/"
                       f"series_observations?series_id={serie}"
                       f"&api_key={TOKEN}"
                       f"&file_type=json&observation_start={start_date}"
                       f"&observation_end={final_date}")

            self.logger.info(f"URL gerada para Juros EUA ({serie}): {url}")

            api_client = jdf.APIDataParser(self.logger)

            df_juros = api_client.get_from_api(
                url,
                ['date', 'value'],
                is_list=True,
                convert_timestamp=False,
                sanitize=True,
                frequency='monthly',
                http_get_timeout=http_get_timeout,
                data_key='observations',
                col_freq='date')

            if df_juros is not None and not df_juros.empty:

                df_juros.sort_values("date", inplace=True)

                df_juros = df_juros.drop_duplicates(
                    subset=["date"], keep="last"
                )

                nome_tabela = f"{camada}.{tabela}"
                query = f"""INSERT INTO {nome_tabela} (data, valor)
                VALUES (%s, %s);"""
                valores = [
                    (row["date"].date(), self._to_float(row["value"]))
                    for _, row in df_juros.iterrows()
                    if self._to_float(row["value"]) is not None
                ]

                self.db.executa_query(query, valores, commit=True, many=True)

                self.logger.info(
                    f"{serie} histórico obtido e gravado com sucesso.")

                proxima = self._calcula_proxima_execucao(hoje_data, frequencia)

                self.table_checker.register_populated(
                    camada=camada,
                    tabela=tabela,
                    nome_serie=tabela,
                    inicial=data_inicial,
                    data_exec=hoje_data,
                    prox_data=proxima,
                    obs=pop_string
                )

                self.logger.info(f"Juros ({serie}) EUA obtido com sucesso.")

            else:

                if hoje:

                    self.logger.warning(
                        f"""{serie} Sem dados atuais.
                        Agendada para retry.""")

                    self.table_checker.register_populated(
                        camada=camada,
                        tabela=tabela,
                        nome_serie=tabela,
                        inicial=data_inicial,
                        data_exec=hoje_data,
                        prox_data=hoje_data,
                        obs="Dado não retornado — agendado para retry."
                    )

                else:

                    self.warning.warning(
                        f"""Sem carga inicial para: {serie}.
                        Agendada para retry.""")

        except Exception as e:
            self.logger.error(
                f"Erro ao obter os Juros ({serie}) EUA: {e}")
            return None
