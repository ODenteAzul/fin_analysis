import re
from utils.json_loader import carregar_lista_json


class LexicalParser:

    def __init__(
        self,
        logger
    ):
        self.logger = logger
        self.termos_positivos = self._carregar_palavras(qual="positivo")
        self.termos_negativos = self._carregar_palavras(qual="negativo")
        self.termos_neutros = self._carregar_palavras(qual="neutro")
        self.negadores = self._carregar_palavras(qual="negadores")
        self.inversores = self._carregar_palavras(qual="inversores")

    def _carregar_palavras(
        self,
        qual: str
    ) -> set[str]:

        try:

            termos = carregar_lista_json("config/palavras.json")

            return set(termos.get(qual, []))

        except Exception as e:
            self.logger.error(f"""Não foi possível carregar os
                              termos de avaliação: {qual}, erro: {e}""")

    def _detectar_negacao(
        self,
        texto: str,
        termo: str
    ) -> bool:

        try:
            negadores_regex = "|".join(map(re.escape, self.negadores))
            padrao = rf"({negadores_regex})\s+{re.escape(termo)}"

            return re.search(padrao, texto)
        except Exception as e:
            self.logger.error(f"Erro ao avaliar a negação: {e}")

    def _avaliar_inversao_contexto(
        self,
        texto: str,
    ) -> int:

        texto = texto.lower()

        for inversor in self.inversores:
            if inversor in texto:
                partes = texto.split(inversor, 1)
                if len(partes) == 2:
                    score1, _ = self.analisar_texto(partes[0])
                    score2, _ = self.analisar_texto(partes[1])
                    if score1 * score2 < 0:
                        if abs(score2) > abs(score1):
                            return 1 if score2 > 0 else -1
        return 0

    def _classifica_label(
        self,
        score: float
    ) -> str:

        if score >= 1:
            return score, "positivo"
        elif score <= -1:
            return score, "negativo"
        else:
            return score, "neutro"

    def _analise_lexica(
        self,
        texto: str
    ) -> float:
        texto = texto.lower()
        score = 0

        try:
            for termo in self.termos_positivos:
                if re.search(rf"\b{re.escape(termo)}\b", texto):
                    score += -1 if self._detectar_negacao(texto, termo) else 1
                    self.logger.debug(
                        f"""Termo positivo detectado: {termo},
                        score atual: {score}""")

            for termo in self.termos_negativos:
                if re.search(rf"\b{re.escape(termo)}\b", texto):
                    score += 1 if self._detectar_negacao(texto, termo) else -1
                    self.logger.debug(
                        f"""Termo negativo detectado: {termo},
                        score atual: {score}""")

            return self._classifica_label(score)

        except Exception as e:
            self.logger.error(f"""Falha ao avaliar os sentimentos do texto:
                              {texto}, erro: {e}""")

    def analisar_texto(
        self,
        texto: str
    ) -> tuple:

        try:
            texto = texto.lower()
            score, label = self._analise_lexica(texto)
            adjust = self._avaliar_inversao_contexto(texto)
            score += adjust * 0.5

            return self._classifica_label(score)

        except Exception as e:
            self.logger.error(f"""Falha ao obter o resultado
                              erro: {e}""")
