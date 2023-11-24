import pandas as pd # Biblioteca para bases de dados
import requests; import ssl # Bibliotecas para acessar API's

# Cria uma classe customizada para permitir o acesso a API do IBGE utilizando requests
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
              classificacao: dict = None) -> pd.DataFrame:
    """
    sidra_get() baixa tabelas do SIDRA.
    
    Parâmetros
    ----------
    url: str
        A url da tabela. Pode ser obtida a partir da opção 'Links de Compartilhar' nas tabelas disponíveis em https://sidra.ibge.gov.br.
    
    tabela: str ou lista de str
        Código(s) da(s) tabela(s). Se múltiplas tabelas são fornecidas em uma lista, elas precisam ter os mesmos parâmetros em comum, senão resultará em erro.

    nivel_geografico: str ou lista de str, opcional
        Código(s) do(s) território(s). Por padrão, os dados para o Brasil (código = '1') serão consultados.

    variavel: str ou lista de str, opcional
        Código(s) da(s) variável(eis). Por padrão, todas as variáveis serão consultadas.

    classificacao: dict, opcional
        Um dicionário no formato {'Classificação': ['Categorias']}. Por padrão, a consulta utilizará o nível mais agregado de classificação.

    Os códigos das variáveis e os detalhes dos parâmetros podem ser consultados em https://servicodados.ibge.gov.br/api/docs/agregados?versao=3.

    Exemplos
    -------
    ```python

    # Consultando tabelas a partir dos parâmetros:

    tabelas_IPCA_dessazonalizado_subitens = ['1420', '2942', '661', '7061']
    territorios_selecionados = ['1', '6', '7']
    variacao_mensal_IPCA_dessazonalizado = '306'
    indices_selecionados = {'315': ['7169', '7170', '7171', '7432', '7479']}
    
    df_IPCA_dessazonalizado_subitem = sidra_get(
        tabela = tabelas_IPCA_dessazonalizado_subitens,
        nivel_geografico = territorios_selecionados,
        variavel = variacao_mensal_IPCA_dessazonalizado,
        classificacao = indices_selecionados)
    
    # Consultando uma tabela a partir de um link:

    print(df_IPCA_dessazonalizado_subitem)
    
    link_sidra = 'https://apisidra.ibge.gov.br/values/t/7060/n1/all/n7/all/n6/all/v/63/p/all'
    
    df_IPCA_subitem = sidra_get(url=link_sidra)
    
    print(df_IPCA_subitem)
    """
    
    # Consulta a url informada
    if url:
        
        with requests.session() as s:
            s.mount("https://", TLSAdapter())
            resposta = s.get(url)

        if resposta.status_code == 200:
            sidra_df = pd.DataFrame(resposta.json())
            sidra_df.columns = sidra_df.iloc[0]
            sidra_df = sidra_df[1:]
        else:
            raise ValueError(f'Erro na consulta {url}: {resposta.text}.')

    # Consulta as tabelas informadas
    elif tabela:
        lista_dfs = []
        lista_tabelas = tabela if isinstance(tabela, list) else [tabela]
        lista_variaveis = variavel if isinstance(variavel, list) else [variavel]
        lista_niveis_geograficos = [f'n{n}/all' for n in nivel_geografico]

        for t in lista_tabelas:
            endpoint = 'https://apisidra.ibge.gov.br/'
            
            consulta = f'values/t/{t}/'
            consulta += f'{'/'.join(lista_niveis_geograficos)}/'
            consulta += f'v/{'/'.join(lista_variaveis)}/'
            if classificacao:
                consulta += '/'.join([f'c{key}/{','.join(value)}' for key, value in classificacao.items()])
            consulta += '/p/all'
            
            url = endpoint + consulta

            with requests.session() as s:
                s.mount("https://", TLSAdapter())
                resposta = s.get(url)
            
            if resposta.status_code == 200:
                nova_tabela = pd.DataFrame(resposta.json())
                nova_tabela.columns = nova_tabela.iloc[0]
                nova_tabela = nova_tabela[1:]
                lista_dfs.append(nova_tabela)
            else:
                raise ValueError(f'Erro na tabela {t}: {resposta.text}.')
        
        sidra_df = pd.concat(lista_dfs, ignore_index=True)

    else:
        raise ValueError('sidra_get() requer pelo menos uma url ou uma tabela.')
    
    return sidra_df