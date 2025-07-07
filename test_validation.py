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


def _verificar_relevancia_semantica(noticia_nova, noticia_base):

    nlp = spacy.load("pt_core_news_md")

    try:
        doc1 = nlp(noticia_nova)
        doc2 = nlp(noticia_base)
        return doc1.similarity(doc2)
    except Exception as e:
        print(
            f"Erro ao calcular similaridade semântica: {e}")
        raise


def _verificar_relevancia(noticia_nova, palavras_chave, limite=0.005):
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


def _noticia_e_relevante(noticia_nova, titulo, termos_empresa, noticia_base):

    noticia_nova_limpa = _limpar_texto(noticia_nova)

    noticia_base_limpa = _limpar_texto(noticia_base)

    relevante_por_termos = _verificar_relevancia(
        noticia_nova=noticia_nova_limpa, palavras_chave=termos_empresa)

    relevancia_semantica = _verificar_relevancia_semantica(
        noticia_nova_limpa, noticia_base_limpa)

    titulo_relevante = _titulos_sao_similares()

    print(relevante_por_termos)
    print(relevancia_semantica)

    if not relevante_por_termos and relevancia_semantica > 0.85:
        return True
    elif relevante_por_termos and relevancia_semantica > 0.60:
        return True
    else:
        return False


noticia1 = """A Embraer está inciando estudos para o lançamento de um novo carro voador. 
                    Pois é, nem só de aviões vive a gigante da aviação, que busca se antecipar 
                    ao futuro já entrando em um mercado que tem atraido grandes empresas, mas 
                    em que nenhuma ainda obteve tanto sucesso. Para tornar sua tentativa mais 
                    certeira, a Embraer busca parcerias de peso, mundo afora, como outras gigantes 
                     da aviação, para conseguir decolar nesse universo ainda incerto. Mas de boba 
                     a Embraer não tem nada, e alocou um time de engenheiros de ponta para esse 
                     novo desafio.
                    """

noticia2 = """A fabricante brasileira Embraer reforça sua presença no mercado internacional com exportações de jatos comerciais e aeronaves para defesa, consolidando sua liderança em inovação no setor aeroespacial."""

notiica3 = """Uma grande gincana aberta, de rua mesmo, como aquelas de antigamente, está acontecendo em São Bernardo do Campo. Ela é promovida pela Embraer, com apoio de diversas outras empresas locais. A ideia é criar um ponto de encontro para todos na cidade. A gigante da aviação está até mesmo distribuindo presentes para as crianças que particiarem das brincadeiras, uma oportunidade para deixaram o celular de lado por um momento."""

resultado = _noticia_e_relevante(
    noticia_nova=notiica3,
    termos_empresa=termos_empresa,
    noticia_base=texto_base)

print(resultado)


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
    }
]


def testar_matriz(noticias, termos, base_ref, limite_semantico_com_termos=0.60, limite_semantico_sem_termos=0.90):
    for i, noticia in enumerate(noticias):
        print(f"\n🔎 Notícia {i+1}: {noticia['titulo']}")

        corpo = noticia['corpo']
        esperado = noticia['esperado']

        resultado = _noticia_e_relevante(corpo, termos, base_ref)

        print(esperado)
        print(resultado)


testar_matriz(noticias_teste, termos_empresa, texto_base)
