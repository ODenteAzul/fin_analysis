CREATE TABLE IF NOT EXISTS {camada}.{tabela} (
                data DATE PRIMARY KEY,
                selic NUMERIC,
                ipca NUMERIC,
                pib NUMERIC,
                juros_usa NUMERIC,
                dolar NUMERIC,
                ibovespa NUMERIC
            );