import sqlite3
import pandas as pd
from uuid import uuid4
from datetime import datetime
from pandas import Series, DataFrame

##############
## CLIENTES ##
##############
def adiciona_novos_clientes(df: DataFrame, db_path: str):
    """
    Args:
        - df (DataFrame): Tabela com os dados de cliente|distribuidora|uc
        - db_path (str): Caminho para o arquivo .db

    Returns:
        - bool: Status do script (True => Funcionou || False => Erro)
    """
    clientes = adiciona_novo_id_clientes(df)
    clientes_existentes = get_clientes_existentes(db_path)
    novos_clientes = mantem_novos_clientes(clientes, clientes_existentes, col="nome")
    if not(novos_clientes.empty):
        salva_novos_clientes(ucs_nova=novos_clientes, db_path=db_path)


def adiciona_novo_id_clientes(ucs_nova: DataFrame) -> DataFrame:
    """
    Adiciona uuid as novas ucs

    Args:
        - ucs_nova (DataFrame): clientes obtidos da filtragem entre pdfs e banco de dados

    Returns:
        - DataFrame: tabela de clientes com a nova coluna de ID
    """
    ucs_nova["id"] = (
        ucs_nova.apply(lambda x: uuid4(), axis=1).astype(str)
        + "-"
        + ucs_nova.nome.str[4::-2] + "-" + ucs_nova.uc
    )
    
    return ucs_nova


def get_clientes_existentes(db_path: str) -> DataFrame:
    """
    Obtem os clientes ja adicionados ao banco de dados com seus respectivos ids

    Args:
        - db_path (str): caminho para o arquivo .db

    Returns:
        - DataFrame: Tabela com os dados obtidos do banco
    """
    conn = sqlite3.connect(db_path)
    df = pd.read_sql("SELECT * FROM clientes", conn)
    conn.close()
    return df


def mantem_novos_clientes(novos_clientes: DataFrame, clientes_existentes: DataFrame, col: str) -> DataFrame:
    """
    Filtra os clientes ja existentes para adicionar os novos ao banco de dados

    Args:
        - novos_clientes (DataFrame): clientes obtidos da leitura dos pdfs
        - clientes_existentes (DataFrame): clientes obtidos da leitura do banco de dados
        - col (str): nome da coluna dos clientes

    Returns:
        - DataFrame: tabela apenas com os clientes que precisam ser adicionados ao banco de dados
    """
    nomes_db = clientes_existentes[col]
    return novos_clientes[~novos_clientes[col].isin(nomes_db)].reset_index(drop=True)


def salva_novos_clientes(ucs_nova: DataFrame, db_path: str) -> bool:
    """
    Adiciona os novos clientes com seus respectivos ids ao banco

    Args:
        - ucs_nova (DataFrame): clientes com ids obtidos apos a funcao adiciona_novo_id_clientes
        - db_path (str): caminho para o banco de dados

    Returns:
        - bool: status de sucesso ou falha na insercao dos novos valores
    """
    try:
        conn = sqlite3.connect(db_path)
        ucs_nova.to_sql("clientes", conn, index=False, if_exists="append")
        conn.close()
        return True
    except Exception:
        return False

##############
## CONSUMOS ##
##############
def adiciona_novos_consumos(df: DataFrame, dist: str, db_path: str):
    """
    Processa os dados para salva-los no banco 

    Args:
        - df (DataFrame): consumos obtidos da leitura do pdf
        - db_path (str): caminho para o banco de dados
        - dist (str): nome da distribuidora

    Returns:
    """

    clientes = get_clientes_existentes(db_path)
    novos_consumos = criar_id_consumos(df_id=clientes, df_consumo=df.drop_duplicates(subset=["nome", "uc", "datas"]))
    
    consumos_existentes = get_consumos_existentes(db_path, dist)
    novos_consumos_filtrados = mantem_novos_consumos(novos_consumos=novos_consumos, consumos_existentes=consumos_existentes)
    
    if not(novos_consumos_filtrados.empty):
        salva_novos_consumos(novos_consumos_filtrados, db_path, dist)


def criar_id_consumos(df_id: DataFrame, df_consumo: DataFrame) -> DataFrame:
    """
    Cria coluna de id para nao gravar dados repetidos de consumo por cliente
    Args:
        - df_id (DataFrame): tabela com os ids originais
        - df_consumo (DataFrame): tabela com os consumos obtidos dos pdfs

    Returns:
        - DataFrame: tabela com os novos ids gerados a partir das datas e ids originais
    """
    df = df_id.merge(df_consumo, on=["nome", "uc", "distribuidora"])
    df["id_datas"] = df["id"] + "-" + df["datas"].dt.strftime("%m%Y")
    return df.drop(columns=["nome", "id", "datas", "uc", "distribuidora", "ths"])


def get_consumos_existentes(db_path: str, distribuidora: str) -> DataFrame:
    """
    Le o banco de dados e obtem as informacoes de consumo

    Args:
        - db_path (str): Caminho para o arquivo .db
        - distribuidora (str): nome da distribuidora/tabela do banco
        
    Returns:
        - DataFrame: DataFrame obtido do banco de dados
    """
    conn = sqlite3.connect(db_path)
    consumos = pd.read_sql(f"SELECT * FROM {distribuidora}", conn)
    conn.close()
    return consumos


def mantem_novos_consumos(
    novos_consumos: DataFrame, consumos_existentes: DataFrame
) -> DataFrame:
    """
    Filtra os consumos ja adicionados, e retorna apenas os consumos necessarios para insercao no banco de dados

    Args:
        - novos_consumos (DataFrame): Tabela com os dados de consumo obtidos dos pdfs
        - consumos_existentes (DataFrame): Tabela com os dados de consumo obtidos do banco de dados

    Return:
        - DataFrame: Tabela apenas com os dados de consumo que nao estao presentes no banco de dados
    """
    id_col = "id_datas"
    ids = consumos_existentes[id_col]
    return novos_consumos[~novos_consumos[id_col].isin(ids)]


def salva_novos_consumos(novos_consumos: DataFrame, db_path: str, dist: str) -> bool:
    """
    salva os novos consumos com seus respectivos ids ao banco

    Args:
        - novos_consumos (DataFrame): consumos ainda nao adicionados ao banco
        - db_path (str): caminho para o banco de dados
        - dist (str): nome da distribuidora

    Returns:
        - bool: status de sucesso ou falha na insercao dos novos valores
    """

    try:
        conn = sqlite3.connect(db_path)
        novos_consumos.to_sql(dist, conn, index=False, if_exists="append")
        conn.close()
        return True
    except Exception:
        return False

############
## OUTROS ##
############
def arruma_decimal(coluna: Series) -> Series:
    """
    Substitui vírgulas por pontos em uma coluna e converte para float.

    Args:
        - coluna (Series): Uma coluna de dados contendo números com vírgulas como separador decimal.

    Returns:
        - Series: Uma nova coluna com vírgulas substituídas por pontos e convertida para float.
    """
    coluna = coluna.str.replace(".", "", regex=False)
    return coluna.str.replace(",", ".", regex=False).astype(float)


def remove_datas_duplicadas(df: DataFrame, col: str) -> DataFrame:
    """
    Args:
        - df (DataFrame): Tabela original
        - col (str): Nome da coluna de datas
    """

    return df.iloc[df.reset_index()[col].drop_duplicates().index]


def remove_outliers(df: DataFrame, col: str, quantile: float) -> DataFrame:
    """
    Alguns valores são obtidos incorretamente e quando salvos no dataframe final, 
    são considerados outliers, eles precisam ser removidos para que quando uma analise for realizada, 
    ela não seja incorreta
    Args:
        - df: o DataFrame com os outliers
        - col: a coluna a analiser os outliers
        - quantile: o quantil a ser considerado durante a remoção dos outliers

    Returns:
        - DataFrame com outliers removidos
    """
    q = df[col].quantile(quantile)
    return df.query(f"{col} <= {q}").reset_index(drop=True)


def texto_para_data(texto: str):
    """
    Converte uma string de texto no formato 'Mês/Ano' em um objeto de data.

    Args:
        - texto (str): Uma string no formato 'Mês/Ano', por exemplo, 'Jan/2023'.

    Returns:
        - datetime: Um objeto datetime representando a data correspondente ao texto fornecido.
    """
    meses = {
        "jan": 1,
        "fev": 2,
        "mar": 3,
        "abr": 4,
        "mai": 5,
        "jun": 6,
        "jul": 7,
        "ago": 8,
        "set": 9,
        "out": 10,
        "nov": 11,
        "dez": 12,
    }
    partes = texto.split("/")
    numero_mes = meses.get(partes[0].lower())
    ano = int(partes[1])
    return datetime(ano, numero_mes, 1)
