from pathlib import Path


class CriadorDDL:
    def __init__(
            self,
            ddl_dir: str = "sql/ddl"):
        self.ddl_dir = Path(ddl_dir)
    """
    Classe para carregamento dos arquivos SQL.
    """

    def load_template(
        self,
        filename: str
    ) -> str:
        """
            Carrega o arquivo template SQL no caminho informado
        """
        file_path = self.ddl_dir / filename
        if not file_path.exists():
            raise FileNotFoundError(
                f"O arquivo não foi encontrado: {file_path}")
        return file_path.read_text(encoding="utf-8")

    def render_template(
        self,
        template: str,
        **kwargs
    ) -> str:
        """
        Substitui placeholders no template por valores informados.

        kwargs esperados: schema, table, etc.

        return: str: SQL pronta para uso.
        """
        for k, v in kwargs.items():
            if not isinstance(v, str) or not v.isidentifier():
                raise ValueError(f"Valor inválido para '{k}': '{v}'")

        return template.format(**kwargs)
