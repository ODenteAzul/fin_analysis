from utils.json_loader import carregar_lista_json
from analysis.lexical_parser import LexicalParser
from utils.utils import LoggerCustomizado
from analysis.sentiment_analysis import SentimentAnalyzer

noticias = carregar_lista_json("config/noticias_teste.json")

# Instancia a classe
log = LoggerCustomizado()

lexical_parser = LexicalParser(logger=log)

analisador = SentimentAnalyzer(
    logger=log, db=None, lexical_parser=lexical_parser, usar_bert=True)


def testar_noticias(noticias):
    for i, noticia in enumerate(noticias):
        print(f"\n🔎 Notícia {i + 1}: {noticia['titulo']}")

        titulo = noticia['titulo']
        corpo = noticia['corpo']
        esperado = noticia['sentimento']

        resultado = analisador.analisar_noticia(titulo, corpo)

        print(f"✅ Esperado: {esperado}")
        print(
            f"📌 Título - VADER: {resultado['sent_vader_label_titulo']} (score: {resultado['sent_vader_score_titulo']:.3f})")
        print(
            f"📌 Título - Lexical: {resultado['sent_lexical_label_titulo']} (score: {resultado['sent_lexical_score_titulo']:.3f})")
        print(
            f"📌 Conteúdo - Lexical: {resultado['sent_lexical_label_conteudo']} (score: {resultado['sent_lexical_score_conteudo']:.3f})")
        print(
            f"📌 Conteúdo - BERT: {resultado['sent_bert_label_conteudo']} (score: {resultado['sent_bert_score_conteudo']:.3f})")
        print(
            f"✅ Final combinado: {resultado['sent_label_final']} (score: {resultado['sent_score_final']:.3f})")


testar_noticias(noticias)
