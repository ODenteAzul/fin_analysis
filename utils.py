import logging


class LoggerCustomizado:
    def __init__(self, nome_arquivo="app.log", nivel=logging.INFO):
        """Inicializa o logger e configura o basicConfig."""
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(nivel)

        # Criar um formato de log
        formato = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

        # Criar um handler para salvar no arquivo
        file_handler = logging.FileHandler(nome_arquivo)
        file_handler.setFormatter(formato)
        self.logger.addHandler(file_handler)

        # Criar um handler para exibir no terminal
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formato)
        self.logger.addHandler(console_handler)

    def info(self, mensagem):
        """Registra uma mensagem de nível INFO."""
        self.logger.info(mensagem)

    def debug(self, mensagem):
        """Registra uma mensagem de nível DEBUG."""
        self.logger.debug(mensagem)

    def warning(self, mensagem):
        """Registra uma mensagem de nível WARNING."""
        self.logger.warning(mensagem)

    def error(self, mensagem):
        """Registra uma mensagem de nível ERROR."""
        self.logger.error(mensagem)

    def critical(self, mensagem):
        """Registra uma mensagem de nível CRITICAL."""
        self.logger.critical(mensagem)
