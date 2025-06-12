from afinn import Afinn
from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from transformers import pipeline


sentiment_analysis = pipeline(
    "sentiment-analysis", model="yiyanghkust/finbert-tone")

batch_size = 5


class SentimentAnalysis():
    def __init__(self,
                 logger,
                 db,
                 conn,
                 cursor):
        self.logger = logger
        self.db = db
        self.conn = conn
        self.cursor = cursor

    @classmethod
    def verifica_sentimento(cls, titulo):
        vader = SentimentIntensityAnalyzer()
        score_titulo = vader.polarity_scores(titulo)["compound"]
        return score_titulo


titulo = "Embraer vê aumento nos lucros e novos pedidos na carteira."
texto = "Embraer conquista novo contrato milionário e carteira de pedidos decola."

score_vader = SentimentAnalysis.verifica_sentimento(titulo=texto)
print(score_vader)


# Varia de -1 (negativo) a 1 (positivo)
score_texto = TextBlob(texto).sentiment.polarity
print(score_texto)


afinn = Afinn()
score_afinn = afinn.score(texto)
print(score_afinn)


score_bert = sentiment_analysis(texto)[0]["score"]
print(score_bert)


def normalizar_score(score, min_val, max_val):
    return (score - min_val) / (max_val - min_val)


def calcular_score_final(score_vader, score_afinn, score_bert, peso_vader=0.3, peso_afinn=0.3, peso_bert=0.2):
    score_vader_norm = normalizar_score(
        score_vader, -1, 1)
    score_afinn_norm = normalizar_score(score_afinn, -5, 5)
    score_bert_norm = normalizar_score(
        score_bert, 0, 1)

    return (score_vader_norm * peso_vader) + (score_afinn_norm * peso_afinn) + (score_bert_norm * peso_bert)


print(calcular_score_final(score_vader=score_vader,
      score_afinn=score_afinn, score_bert=score_bert))
