CREATE TABLE IF NOT EXISTS {camada}.{tabela} (
                data DATE PRIMARY KEY,
                preco_abertura NUMERIC,
                preco_minimo NUMERIC,
                preco_maximo NUMERIC,
                preco_fechamento NUMERIC,
                volume_negociado NUMERIC,
                media_movel_50 NUMERIC,
                media_movel_200 NUMERIC
            );