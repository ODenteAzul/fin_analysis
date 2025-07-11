from utils.ddl_loader import CriadorDDL

loader = CriadorDDL("sql/ddl")

template = loader.load_template("cotacao_diaria.sql")
rendered_sql = loader.render_template(
    template, camada="silver", tabela="cambio_diario_dolar")

print(rendered_sql)
