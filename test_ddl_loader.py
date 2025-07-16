from utils.ddl_loader import CriadorDDL
from utils.json_loader import carregar_lista_json

loader = CriadorDDL("sql/ddl")

ls_moedas = carregar_lista_json("config/moedas.json")

for moeda in ls_moedas:
    template = loader.load_template("cotacao_diaria.sql")
    rendered_sql = loader.render_template(
        template, camada="silver", tabela=moeda["tabela"])
    print(rendered_sql)
