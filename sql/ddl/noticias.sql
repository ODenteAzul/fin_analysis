CREATE TABLE IF NOT EXISTS {camada}.{tabela} (
                id SERIAL PRIMARY KEY,
                cod_bolsa TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                titulo TEXT,
                descricao TEXT,
                data_historico DATE NOT NULL,
                url TEXT,
                sentimento TEXT);