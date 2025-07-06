from indices.technical_indexes import TechIndexes
import pandas as pd
from datetime import datetime


class TechCalcs():
    def __init__(self,
                 logger,
                 db,
                 conn,
                 cursor):
        self.logger = logger
        self.db = db
        self.conn = conn
        self.cursor = cursor

    def executa_ti(self,
                   dia_atual,
                   novo_dia,
                   coleta_diaria_macro,
                   hora_abertura_bolsa,
                   hora_fechamento_bolsa):

        hora_atual = datetime.now().time()

        ti = TechIndexes(logger=self.logger,
                         db=self.db,
                         conn=self.conn,
                         cursor=self.cursor)

        try:

            self.logger.info(
                "Verificando a presença das tabelas de Índices Técnicos...")

            ti.verifica_tabelas()

            self.logger.info("Tabelas verificadas.")

            query = """SELECT COUNT(*) AS total_registros, 
                        SUM(CASE WHEN media_movel_50 IS NULL THEN 1 ELSE 0 END) AS faltando_calculo
                        FROM preco_acoes_diario 
                        WHERE EXTRACT(ISODOW FROM data_historico) BETWEEN 1 AND 5;"""

            last_day = self.db.fetch_data(query, tipo_fetch="one")

            total_registros = last_day["total_registros"]
            faltando_calculo = last_day["faltando_calculo"]

            self.logger.info("Verificando dados presentes...")

            if faltando_calculo > 0:
                self.logger.info(
                    "Processando os Índices Técnicos para o último dia de dados...")

                query = """SELECT * FROM preco_acoes_diario
                                ORDER BY data_historico DESC
                                LIMIT 200;"""

                dados = self.db.fetch_data(query, tipo_fetch='all')
                df_dados = pd.DataFrame(
                    dados, columns=[desc[0] for desc in self.cursor.description])

            elif total_registros > 0:
                self.logger.info(
                    "Todos os registros atualizados, nada a fazer...")
            else:
                self.logger.info(
                    "Processando os Índices Técnicos para todos os valores de fechamento diários...")

                query = """SELECT * FROM preco_acoes_diario
                            ORDER BY data_historico DESC;"""

                dados = self.db.fetch_data(query, tipo_fetch='all')
                df_dados = pd.DataFrame(
                    dados, columns=[desc[0] for desc in self.cursor.description])

                if not df_dados.empty:
                    df_dados.ffill(inplace=True)
                    df_dados = ti.calcular_rsi(df_dados)
                    df_dados = ti.calcular_macd(df_dados)
                    df_dados = ti.calcular_ema(df_dados)
                    df_dados = ti.calcular_atr(df_dados)
                    df_dados = ti.calcular_bollinger(df_dados)
                    df_dados = ti.calcular_adx(df_dados)
                    df_dados = ti.calcular_vwap(df_dados)
                    df_dados = ti.calcular_obv(df_dados)
                    df_dados = ti.calcular_preco_medio(df_dados)

                    df_dados.fillna(0, inplace=True)

                    query = """INSERT INTO indicadores_tecnicos_diarios (
                                    cod_bolsa,
                                    data,
                                    rsi,
                                    macd,
                                    linha_sinal_macd,
                                    histograma_macd,
                                    bandas_bollinger_superior,
                                    bandas_bollinger_inferior,
                                    desvio_padrao_bollinger,
                                    banda_superior_adaptativa,
                                    banda_inferior_adaptativa,
                                    banda_superior_keltner,
                                    banda_inferior_keltner,
                                    media_movel_exponencial_50,
                                    media_movel_exponencial_200,
                                    atr,
                                    adx,
                                    vwap,
                                    obv,
                                    preco_medio_diario) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                    valores = [
                        ('EMBR3.SA', row["data_historico"], row["RSI"], row["MACD"],
                         row["Sinal_MACD"], row["Histograma_MACD"],
                         row["Banda_Superior"], row["Banda_Inferior"],
                         row["Desvio_Padrao"], row["Banda_Superior_Adaptativa"],
                         row["Banda_Inferior_Adaptativa"], row["Banda_Superior_Keltner"],
                         row["Banda_Inferior_Keltner"], row["EMA_50"],
                         row["EMA_200"], row["ATR"], row["ADX"],
                         row["VWAP"], row["OBV"], row["preco_medio"])
                        for _, row in df_dados.iterrows()
                    ]

                    try:
                        if self.db.executa_query(query, valores, commit=True, many=True):
                            self.logger.info(
                                "Índices Técnicos diários atualizados e salvos.")

                    except Exception as e:
                        self.logger.error(
                            f"Erro ao inserir índices técnicos diários: {e}")

                    self.logger.info(
                        "Verificando dados de valores intradiários...")

                    if hora_abertura_bolsa <= hora_atual <= hora_fechamento_bolsa:
                        self.logger.info(
                            "Processando os cálculos para valores intradiários...")

                        df_dados_intra = ti.calcular_bollinger(
                            df_dados, periodo=50, intradiario=True)

                        print(df_dados_intra.head())

                        self.logger.info(
                            "Valores intradiários calculados e gravados com sucesso.")

                    self.logger.info(
                        "Todos os Índices Técnicos atualizados")

                self.logger.info("Índices técnicos já presentes e atualizados")

        except Exception as e:
            self.logger.error(
                f"Houve um problema ao processar os índices: {e}")
            with open("ti_errors.log", "a") as f:
                f.write(f"{hora_atual} - Erro: {e}\n")
