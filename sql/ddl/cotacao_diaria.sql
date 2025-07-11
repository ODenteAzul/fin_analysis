CREATE TABLE IF NOT EXISTS {camada}.{tabela} (
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
            );