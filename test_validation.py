import spacy
import unicodedata
import re
from fuzzywuzzy import fuzz
from config.json_loader import carregar_lista_json

sin_dict = carregar_lista_json("config/sinonimos_empresas.json")
base_news = carregar_lista_json("config/textos_base.json")


def _limpar_texto(texto):
    texto = unicodedata.normalize('NFKD', texto).encode(
        'ASCII', 'ignore').decode('ASCII')
    texto = re.sub(r'[^\w\s]', '', texto)
    return texto.lower()


def _titulos_sao_similares(self, titulo1, titulo2, limite=80):
    try:
        if isinstance(titulo1, str) and isinstance(titulo2, str):
            return fuzz.token_set_ratio(titulo1.lower(),
                                        titulo2.lower()) > limite

        return False

    except Exception as e:
        self.logger.error(
            f"Problema ao testar a similaridades dos títulos: {e}")
        raise


def _verificar_relevancia_semantica(
        noticia_nova,
        noticia_base
):

    nlp = spacy.load("pt_core_news_md")

    try:
        doc1 = nlp(noticia_nova)
        doc2 = nlp(noticia_base)
        return doc1.similarity(doc2)
    except Exception as e:
        print(
            f"Erro ao calcular similaridade semântica: {e}")
        raise


def _verificar_relevancia_titulo(
        titulo,
        palavras_chave,
        limite=80
):
    try:
        titulo = _limpar_texto(titulo)
        return any(term.lower() in titulo for term in palavras_chave)

    except Exception as e:
        print(
            f"Problema ao testar a relevância dos títulos: {e}")
        raise


def _verificar_relevancia_termos(
        noticia_nova,
        palavras_chave,
        limite=0.01
):
    try:
        if isinstance(noticia_nova, str):
            texto_splitado = noticia_nova.lower().split()
            total_palavras = len(noticia_nova.lower().split())
            ocorrencias = sum(texto_splitado.count(term.lower())
                              for term in palavras_chave)
            freq_relativa = ocorrencias / max(total_palavras, 1)

            return freq_relativa >= limite

    except Exception as e:
        print(
            f"Houve um problema aoverificar a relevância da notícia {e}")
        raise


termos_empresa = sin_dict.get("EMBR3.SA")
texto_base = base_news.get("EMBR3.SA")

print(termos_empresa)
print(texto_base)


def _noticia_e_relevante(
        titulo,
        noticia_nova,
        termos_empresa,
        noticia_base
):

    noticia_nova_limpa = _limpar_texto(noticia_nova)

    noticia_base_limpa = _limpar_texto(noticia_base)

    relevante_por_termos = _verificar_relevancia_termos(
        noticia_nova=noticia_nova_limpa, palavras_chave=termos_empresa)

    relevancia_semantica = _verificar_relevancia_semantica(
        noticia_nova_limpa, noticia_base_limpa)

    titulo_relevante = _verificar_relevancia_titulo(
        titulo=titulo, palavras_chave=termos_empresa)

    print(titulo_relevante)
    print(relevante_por_termos)
    print(relevancia_semantica)

    if (titulo_relevante and relevante_por_termos) and relevancia_semantica > 0.60:
        return True
    elif (titulo_relevante or relevante_por_termos) and relevancia_semantica > 0.85:
        return True
    elif not (titulo_relevante or relevante_por_termos) and relevancia_semantica > 0.90:
        return True
    else:
        return False


noticias_teste = [
    {
        "titulo": "Embraer fecha contrato bilionário com Força Aérea dos EUA",
        "corpo": "A Embraer assinou um contrato com o governo americano para fornecer aeronaves militares. O acordo fortalece a presença da fabricante brasileira no mercado internacional.",
        "esperado": True
    },
    {
        "titulo": "Gincana infantil promovida pela Embraer reúne moradores em São Bernardo",
        "corpo": "A gigante da aviação distribuiu presentes para as crianças, incentivando a interação entre vizinhos em evento comunitário.",
        "esperado": False
    },
    {
        "titulo": "Mercado analisa ações da Embraer após divulgação de novos resultados trimestrais",
        "corpo": "Com resultados acima do esperado, a ação da Embraer fechou em alta nesta terça-feira, refletindo a confiança dos investidores.",
        "esperado": True
    },
    {
        "titulo": "São José dos Campos recebe feira de inovação tecnológica",
        "corpo": "A feira contou com startups locais e algumas grandes empresas da região, como Embraer e Ericsson. O evento atraiu milhares de visitantes.",
        "esperado": False
    },
    {
        "titulo": "Embraer anuncia novo centro de pesquisa em aviação sustentável",
        "corpo": "A empresa brasileira lançou um novo centro de P&D voltado para tecnologias de aviação verde, com foco em biocombustíveis e eficiência energética.",
        "esperado": True
    },
    {
        "titulo": "Nem tudo são flores para a Embraer, gigante apresenta resultado abaixo do esperado",
        "corpo": "O primeiro trimestre foi abaixo das projeções para a Embraer, deixando os analistas com um pé atrás. A indústria aeronáutica como um todo apresentou retração em maio e junho e isso prejudicou os resultados. Porém a Embraer afirma ter planos para retomar o crescimento.",
        "esperado": True
    },
    {
        "titulo": "O setor aeroespacial está em declínio.",
        "corpo": "Esse é um ano difícil para as empresas de aviação. Gigantes do setor como a Airbus e a Boing estão amargando prejuízos e atrasos nas entregas. Também não ajuda a alta de preços em diversos países e a inflação. Algumas fabricantes como a nossa Embraer ainda se mantém firmes, pois atuam no nicho regional, que se mantém aquecido.",
        "esperado": False
    }
]


def testar_matriz(
        noticias,
        termos,
        base_ref,
):
    for i, noticia in enumerate(noticias):
        print(f"\n🔎 Notícia {i+1}: {noticia['titulo']}")

        titulo = noticia['titulo']
        corpo = noticia['corpo']
        esperado = noticia['esperado']

        resultado = _noticia_e_relevante(titulo, corpo, termos, base_ref)

        print(esperado)
        print(resultado)


testar_matriz(noticias_teste, termos_empresa, texto_base)
