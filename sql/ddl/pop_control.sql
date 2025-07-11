CREATE TABLE IF NOT EXISTS {camada}.{tabela} (
                schema_nome     TEXT NOT NULL,
                tabela_nome     TEXT NOT NULL,
                nome_serie      TEXT NOT NULL,
                carga_inicial DATE,
                ultima_execucao DATE,
                proxima_execucao DATE,
                observacao      TEXT,
                PRIMARY KEY (schema_nome, tabela_nome)
            );