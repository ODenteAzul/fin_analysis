from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from transformers import pipeline
import numpy as np
from deep_translator import GoogleTranslator


class SentimentAnalyzer():
    def __init__(
            self,
            logger,
            db,
            lexical_parser=None,
            usar_bert=True):
        self.logger = logger
        self.db = db
        self.lexical_parser = lexical_parser
        self.usar_berts = usar_bert

        self.vader = SentimentIntensityAnalyzer()

        if self.usar_berts:
            self.pipe_bert = pipeline(
                "sentiment-analysis",
                model="models/lipaoMai",
                tokenizer="models/lipaoMai",
                device=-1
            )

    def _traduzir_para_ingles(
        self,
        texto: str
    ) -> str:

        return GoogleTranslator(source='pt', target='en').translate(texto)

    def _analisar_vader(
        self,
        texto: str
    ) -> tuple:

        try:

            texto_traduzido = self._traduzir_para_ingles(texto)

            score = self.vader.polarity_scores(texto_traduzido)
            compound = score['compound']
            label = 'positivo' if compound >= 0.05 else 'negativo' if compound <= -0.05 else 'neutro'

            return compound, label

        except Exception as e:
            self.logger.error(f"Erro ao analisar com VADER: {e}")

            return None, 'neutro'

    def _analisar_lexical(
        self,
        texto: str
    ) -> tuple:

        try:

            if not self.lexical_parser:
                return None, 'neutro'
            return self.lexical_parser.analisar_texto(texto)

        except Exception as e:
            self.logger.error(f"Erro ao analisar com LexicalParser: {e}")

            return None, 'neutro'

    def _analisar_bert(
        self,
        texto: str,
        pipe
    ) -> tuple:

        try:

            if not texto.strip():
                return None, 'neutro'

            resultado = pipe(texto[:512])[0]
            label = resultado['label'].lower()
            score = resultado['score']

            if label == "label_0":  # negativo
                return -score, "negativo"
            elif label == "label_1":  # positivo
                return score, "positivo"

            return score, label

        except Exception as e:
            self.logger.error(f"Erro no BERT: {e}")

            return None, 'neutro'

    def _combinar_resultados(
        self,
        resultados: list,
        penalizar_bert: bool = True
    ) -> tuple:

        if len(resultados) == 3:
            pesos = [0.15, 0.15, 0.7]
        else:
            pesos = [0.1, 0.1, 0.6, 0.2]

        scores = [r[0] for r in resultados]
        if all(s is None for s in scores):
            return None, 'neutro'

        score_vader = scores[0]
        score_lex_titulo = scores[1]
        score_lex_conteudo = scores[2]
        score_bert = scores[3] if len(scores) > 3 else None

        outros_scores = [s for s in [score_vader,
                                     score_lex_titulo, score_lex_conteudo] if s is not None]

        media_outros = np.mean(outros_scores) if outros_scores else 0

        if penalizar_bert and score_bert is not None and media_outros != 0:
            if np.sign(score_bert) != np.sign(media_outros):
                score_bert *= (score_bert - media_outros)
                self.logger.debug(
                    f"""Score BERT penalizado por divergir do consenso.
                    Novo score: {score_bert:.3f}""")

        scores_final = [score_vader, score_lex_titulo, score_lex_conteudo]
        if score_bert is not None:
            scores_final.append(score_bert)

        scores_validos = [s for s in scores_final if s is not None]
        pesos_validos = pesos[:len(scores_validos)]

        media = np.average(scores_validos, weights=pesos_validos)

        if media < -0.85:
            label = 'negativo forte'
        elif media < -0.25:
            label = 'negativo'
        elif media > 0.9:
            label = 'positivo forte'
        elif media > 0.25:
            label = 'positivo'
        else:
            label = 'neutro'

        return media, label

    def analisar_noticia(
        self,
        titulo: str,
        conteudo: str
    ) -> dict:

        try:
            score_vader_titulo, label_vader_titulo = self._analisar_vader(
                titulo)
            score_lexical_titulo, label_lexical_titulo = self._analisar_lexical(
                titulo)
            score_lexical_conteudo, label_lexical_conteudo = self._analisar_lexical(
                conteudo)

            resultados = [(score_vader_titulo, label_vader_titulo),
                          (score_lexical_titulo, label_lexical_titulo),
                          (score_lexical_conteudo, label_lexical_conteudo)]

            score_bert_conteudo = None
            label_bert_conteudo = None

            if self.usar_berts:
                score_bert_conteudo, label_bert_conteudo = self._analisar_bert(
                    conteudo, self.pipe_bert)
                resultados.extend([(score_bert_conteudo, label_bert_conteudo)])

            score_final, label_final = self._combinar_resultados(resultados)

            return {
                "sent_vader_score_titulo": score_vader_titulo,
                "sent_vader_label_titulo": label_vader_titulo,
                "sent_lexical_score_titulo": score_lexical_titulo,
                "sent_lexical_label_titulo": label_lexical_titulo,
                "sent_lexical_score_conteudo": score_lexical_conteudo,
                "sent_lexical_label_conteudo": label_lexical_conteudo,
                "sent_bert_score_conteudo": score_bert_conteudo,
                "sent_bert_label_conteudo": label_bert_conteudo,
                "sent_score_final": score_final,
                "sent_label_final": label_final
            }

        except Exception as e:
            self.logger.error(f"Erro ao analisar notícia: {e}")
            return {
                "sent_vader_score_titulo": None,
                "sent_vader_label_titulo": label_vader_titulo,
                "sent_lexical_score_titulo": score_lexical_titulo,
                "sent_lexical_label_titulo": label_lexical_titulo,
                "sent_lexical_score_conteudo": None,
                "sent_lexical_label_conteudo": label_lexical_conteudo,
                "sent_bert_score_conteudo": None,
                "sent_bert_label_conteudo": label_bert_conteudo,
                "sent_score_final": None,
                "sent_label_final": label_final
            }
