from scrapp_valores.run_macroeco import MacroEconomics
from datetime import datetime


class MacroEconomics():
    def __init__(self,
                 logger,
                 db,
                 conn,
                 cursor):
        self.logger = logger
        self.db = db
        self.conn = conn
        self.cursor = cursor

    def executa_macro(self, coleta_diaria):

        pass
