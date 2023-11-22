import pandas as pd # Biblioteca para bases de dados
import requests; import ssl # Bibliotecas para acessar API's

# Cria uma classe customizada para permitir o acesso a API do IBGE utilizando requests
# Pode ser utilizada como alternativa ao sidrapy (requer 'requests' e 'ssl')
# Não é necessário se for utilizar sidrapy
class TLSAdapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        ctx.options |= 0x4   # <-- the key part here, OP_LEGACY_SERVER_CONNECT
        kwargs["ssl_context"] = ctx
        return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)

def sidra_get(url: str = None,
              tabela: str | list = None,
              nivel_geografico: str | list = '1',
              variavel: str | list = 'all',
              classificacao: dict = None):
    """
    sidra_get() baixa tabelas do SIDRA.
    
    tabela: pode ser uma string ou uma lista de strings com os códigos das tabelas, contanto que elas possuam todos os demais parâmetros em comum.
    
    return DataFrame
    """

    if tabela:
        lista_tabelas = []
        lista_tabelas.append(tabela)
        lista_variaveis = []
        lista_variaveis.append(variavel)
        for t in lista_tabelas:
            # Monta a url
            endpoint = 'https://apisidra.ibge.gov.br/'
            consulta = f'values/t/{t}/'
            lista_niveis_geograficos = [f'n{n}/all' for n in nivel_geografico]
            consulta += f'{'/'.join(lista_niveis_geograficos)}/'
            consulta += f'v/{'/'.join(lista_variaveis)}/'
            consulta += '/'.join([f'c{key}/{','.join(value)}' for key, value in classificacao.items()])
            consulta += '/p/all'
            url = endpoint + consulta
    
    if url:
        with requests.session() as s:
            s.mount("https://", TLSAdapter())
            resposta = s.get(url).json()
    else:
        raise ValueError('sidra_get() requer pelo menos uma url ou uma tabela.')
    
    tabela = pd.DataFrame(resposta)
    # Promove a primeira linha como cabeçalho
    tabela.columns = tabela.iloc[0]
    tabela = tabela[1:]
    return tabela