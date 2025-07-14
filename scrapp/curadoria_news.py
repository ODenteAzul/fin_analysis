import spacy
import unicodedata
import re
from fuzzywuzzy import fuzz
from config.json_loader import carregar_lista_json


class Curadoria():
    def __init__(self,
                 logger,
                 db,
                 table_checker):
        self.logger = logger
        self.db = db
        self.table_checker = table_checker
        self.sin_dict = carregar_lista_json("config/sinonimos_empresas.json")
        self.base_news = carregar_lista_json("config/textos_base.json")
        self.news_data = spacy.load("pt_core_news_md")

    def _limpar_texto(
        texto: str
    ) -> str:
        if not texto:
            raise ValueError("Nenhum texto enviado para limpeza...")

        texto = unicodedata.normalize('NFKD', texto).encode(
            'ASCII', 'ignore').decode('ASCII')
        texto = re.sub(r'[^\w\s]', '', texto)
        texto = re.sub(r'\s+', ' ', texto)

        return texto.lower()

    def _verificar_relevancia_semantica(
        self,
        noticia_nova: str,
        noticia_base: str
    ) -> float:

        nlp = self.news_data

        try:
            doc1 = nlp(noticia_nova)
            doc2 = nlp(noticia_base)
            return doc1.similarity(doc2)

        except Exception as e:
            self.logger.info(
                f"Erro ao calcular similaridade semântica: {e}")
            raise

    def _verificar_relevancia_titulo(
        self,
        titulo: str,
        palavras_chave: list
    ):
        try:
            titulo = self._limpar_texto(titulo)
            return any(term.lower() in titulo for term in palavras_chave)

        except Exception as e:
            self.logger.info(
                f"Problema ao testar a relevância dos títulos: {e}")
            raise

    def _verificar_relevancia_termos(
        self,
        noticia_nova: str,
        palavras_chave: list,
        limite: float = 0.01
    ) -> bool:
        try:
            if isinstance(noticia_nova, str):
                texto_splitado = noticia_nova.lower().split()
                total_palavras = len(noticia_nova.lower().split())
                ocorrencias = sum(texto_splitado.count(term.lower())
                                  for term in palavras_chave)
                freq_relativa = ocorrencias / max(total_palavras, 1)

                return freq_relativa >= limite

        except Exception as e:
            self.logger.info(
                f"""Houve um problema ao verificar a
                relevância de termos da notícia. Erro: {e}""")
            raise

    def testar_curadoria(
            self,
    ):

        termos = carregar_lista_json("config/sinonimos_empresas.json")
        noticias = carregar_lista_json("config/noticias_teste.json")
        base_ref = carregar_lista_json("config/textos_base.json")

        for i, noticia in enumerate(noticias):
            self.logger.info(f"\n🔎 Notícia {i+1}: {noticia['titulo']}")

            titulo = noticia['titulo']
            corpo = noticia['corpo']
            esperado = noticia['esperado']

            resultado = self.noticia_e_relevante(
                titulo, corpo, termos, base_ref)

            self.logger.info(f"Resultado esperdo: {esperado}")
            self.logger.info(f"Resultado Real: {resultado}")

    def noticia_e_relevante(
        self,
        titulo: str,
        noticia_nova: str,
        termos_empresa: str,
        noticia_base: str
    ) -> bool:

        try:
            noticia_nova_limpa = self._limpar_texto(noticia_nova)

            noticia_base_limpa = self._limpar_texto(noticia_base)

            relevante_por_termos = self._verificar_relevancia_termos(
                noticia_nova=noticia_nova_limpa, palavras_chave=termos_empresa)

            relevancia_semantica = self._verificar_relevancia_semantica(
                noticia_nova_limpa, noticia_base_limpa)

            titulo_relevante = self._verificar_relevancia_titulo(
                titulo=titulo, palavras_chave=termos_empresa)

            condicao1 = ((titulo_relevante and relevante_por_termos)
                         and relevancia_semantica > 0.60)
            condicao2 = ((titulo_relevante or relevante_por_termos)
                         and relevancia_semantica > 0.85)
            condicao3 = (not (titulo_relevante or relevante_por_termos)
                         and relevancia_semantica > 0.90)

            if condicao1:
                return True

            elif condicao2:
                return True

            elif condicao3:
                return True

            else:
                return False

        except Exception as e:
            print(
                f"Houve um problema ao verificar a relevância da notícia {e}")

    def _titulos_sao_similares(
        self,
        titulo1,
        titulo2,
        limite=80
    ):
        try:
            if isinstance(titulo1, str) and isinstance(titulo2, str):
                return fuzz.token_set_ratio(titulo1.lower(),
                                            titulo2.lower()) > limite

            return False

        except Exception as e:
            self.logger.error(
                f"Problema ao testar a similaridades dos títulos: {e}")
            raise
