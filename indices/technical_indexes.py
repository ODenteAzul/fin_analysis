import numpy as np
import pandas as pd
from decimal import Decimal


class TechIndexes():
    def __init__(self,
                 logger,
                 db,
                 conn,
                 cursor,
                 ):
        self.logger = logger
        self.db = db
        self.conn = conn
        self.cursor = cursor

    def verifica_tabelas(self):
        query = """
            CREATE TABLE IF NOT EXISTS indicadores_tecnicos_diarios (
                id SERIAL PRIMARY KEY,
                cod_bolsa VARCHAR(10) NOT NULL,
                data DATE NOT NULL,
                rsi DECIMAL(5,2),
                macd DECIMAL(5,2),
                linha_sinal_macd DECIMAL(5,2),
                histograma_macd DECIMAL(5,2),
                bandas_bollinger_superior DECIMAL(10,2),
                bandas_bollinger_inferior DECIMAL(10,2),
                desvio_padrao_bollinger DECIMAL(10,2),
                banda_superior_adaptativa DECIMAL(10,2),
                banda_inferior_adaptativa DECIMAL(10,2),
                banda_superior_keltner DECIMAL(10,2),
                banda_inferior_keltner DECIMAL(10,2),
                media_movel_exponencial_50 DECIMAL(10,2),
                media_movel_exponencial_200 DECIMAL(10,2),
                atr DECIMAL(10,2),
                adx DECIMAL(5,2),
                vwap DECIMAL(10,2),
                obv BIGINT,
                preco_medio_diario DECIMAL(10,2)
            );
        """

        self.db.executa_query(query, commit=True)

    def calcular_rsi(self, df, periodo=14):
        df["variacao"] = df["preco_fechamento"].diff()
        df["ganhos"] = df["variacao"].clip(lower=0)
        df["perdas"] = df["variacao"].clip(upper=0).abs()

        df["media_ganhos"] = df["ganhos"].rolling(window=periodo).mean()
        df["media_perdas"] = df["perdas"].rolling(window=periodo).mean()

        df["RS"] = df["media_ganhos"] / df["media_perdas"]
        df["RSI"] = 100 - (100 / (1 + df["RS"]))

        return df

    def calcular_macd(self, df):
        df["EMA_12"] = df["preco_fechamento"].ewm(span=12, adjust=False).mean()
        df["EMA_26"] = df["preco_fechamento"].ewm(span=26, adjust=False).mean()
        df["MACD"] = df["EMA_12"] - df["EMA_26"]
        df["Sinal_MACD"] = df["MACD"].ewm(span=9, adjust=False).mean()
        df["Histograma_MACD"] = df["MACD"] - df["Sinal_MACD"]

        return df

    def calcular_atr(self, df, periodo=14):
        df["H-L"] = df["preco_maximo"] - df["preco_minimo"]
        df["H-C"] = np.abs(df["preco_maximo"] -
                           df["preco_fechamento"].shift(1))
        df["L-C"] = np.abs(df["preco_minimo"] -
                           df["preco_fechamento"].shift(1))

        df["TR"] = df[["H-L", "H-C", "L-C"]].max(axis=1)
        df["ATR"] = df["TR"].rolling(window=periodo).mean()

        return df

    def calcular_ema(self, df, periodo=20):
        df["EMA"] = df["preco_fechamento"].ewm(
            span=periodo, adjust=False).mean()
        df["EMA_50"] = df["preco_fechamento"].ewm(span=50, adjust=False).mean()
        df["EMA_200"] = df["preco_fechamento"].ewm(
            span=200, adjust=False).mean()

        return df

    def calcular_bollinger(self, df, periodo=20, intradiario=False):

        preco_ref = "preco" if intradiario else "preco_fechamento"

        df["SMA"] = df[preco_ref].rolling(window=periodo).mean()
        df["Desvio_Padrao"] = df[preco_ref].rolling(
            window=periodo).std()
        df["Banda_Superior"] = df["SMA"] + (df["Desvio_Padrao"] * 2)
        df["Banda_Inferior"] = df["SMA"] - (df["Desvio_Padrao"] * 2)
        df["Banda_Superior_Adaptativa"] = df["SMA"] + \
            (df["Desvio_Padrao"] * 2.5)
        df["Banda_Inferior_Adaptativa"] = df["SMA"] - \
            (df["Desvio_Padrao"] * 2.5)
        if intradiario:
            df["ATR"] = df[preco_ref].rolling(window=14).apply(
                lambda x: np.max(x) - np.min(x))
        df["Banda_Superior_ATR"] = df["SMA"] + (df["ATR"] * 1.5)
        df["Banda_Inferior_ATR"] = df["SMA"] - (df["ATR"] * 1.5)
        df["EMA_20"] = df[preco_ref].ewm(span=20, adjust=False).mean()
        df["Banda_Superior_Keltner"] = df["EMA_20"] + (df["ATR"] * 1.5)
        df["Banda_Inferior_Keltner"] = df["EMA_20"] - (df["ATR"] * 1.5)

        return df

    def calcular_adx(self, df, periodo=14):
        alpha = 1 / periodo

        df["H-L"] = df["preco_maximo"] - df["preco_minimo"]
        df["H-C"] = np.abs(df["preco_maximo"] -
                           df["preco_fechamento"].shift(1))
        df["L-C"] = np.abs(df["preco_minimo"] -
                           df["preco_fechamento"].shift(1))
        df["TR"] = df[["H-L", "H-C", "L-C"]].max(axis=1)

        df["ATR"] = df["TR"].ewm(alpha=alpha, adjust=False).mean()

        df["+DM"] = np.where((df["preco_maximo"] - df["preco_maximo"].shift(1)) >
                             (df["preco_minimo"].shift(1) - df["preco_minimo"]),
                             df["preco_maximo"] - df["preco_maximo"].shift(1), 0)
        df["-DM"] = np.where((df["preco_maximo"] - df["preco_maximo"].shift(1)) <
                             (df["preco_minimo"].shift(1) - df["preco_minimo"]),
                             df["preco_minimo"].shift(1) - df["preco_minimo"], 0)

        df["+DI"] = (df["+DM"].ewm(alpha=alpha,
                     adjust=False).mean() / df["ATR"]) * 100
        df["-DI"] = (df["-DM"].ewm(alpha=alpha,
                     adjust=False).mean() / df["ATR"]) * 100

        df["DX"] = (np.abs(df["+DI"] - df["-DI"]) /
                    (df["+DI"] + df["-DI"])) * 100
        df["ADX"] = df["DX"].ewm(alpha=alpha, adjust=False).mean()

        return df

    def calcular_vwap(self, df):
        df["VWAP"] = (df["preco_fechamento"] * df["volume_negociado"]
                      ).cumsum() / df["volume_negociado"].cumsum()

        return df

    def calcular_obv(self, df):
        df["preco_fechamento"] = pd.to_numeric(
            df["preco_fechamento"], errors="coerce")
        df["OBV"] = (np.sign(df["preco_fechamento"].diff())
                     * df["volume_negociado"]).cumsum()

        return df

    def calcular_preco_medio(self, df):
        df["preco_abertura"] = pd.to_numeric(df["preco_abertura"],
                                             errors="coerce").apply(lambda x: Decimal(x) if not pd.isna(x) else x)
        df["preco_minimo"] = pd.to_numeric(df["preco_minimo"],
                                           errors="coerce").apply(lambda x: Decimal(x) if not pd.isna(x) else x)
        df["preco_maximo"] = pd.to_numeric(df["preco_maximo"],
                                           errors="coerce").apply(lambda x: Decimal(x) if not pd.isna(x) else x)
        df["preco_fechamento"] = pd.to_numeric(df["preco_fechamento"],
                                               errors="coerce").apply(lambda x: Decimal(x) if not pd.isna(x) else x)

        df["preco_medio"] = (df["preco_abertura"] + df["preco_minimo"] +
                             df["preco_maximo"] + df["preco_fechamento"])/4

        return df
