import sqlite3
import pandas as pd
from uuid import uuid4
from pandas import Series
from datetime import datetime


def arruma_decimal(coluna: Series) -> Series:
    """
    Substitui vírgulas por pontos em uma coluna e converte para float.

    Args:
        coluna (Series): Uma coluna de dados contendo números com vírgulas como separador decimal.

    Returns:
        Series: Uma nova coluna com vírgulas substituídas por pontos e convertida para float.
    """
    coluna = coluna.str.replace(".", "", regex=False)
    return coluna.str.replace(",", ".", regex=False).astype(float)


def texto_para_data(texto):
    """
    Converte uma string de texto no formato 'Mês/Ano' em um objeto de data.

    Args:
        texto (str): Uma string no formato 'Mês/Ano', por exemplo, 'Jan/2023'.

    Returns:
        datetime: Um objeto datetime representando a data correspondente ao texto fornecido.
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


def adiciona_novo_id_clientes(ucs_nova: pd.DataFrame) -> pd.DataFrame:
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
        + ucs_nova.nome.str[4::-2]
    )
    return ucs_nova


def get_clientes_existentes(db_path: str) -> pd.DataFrame:
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


def mantem_novos_clientes(ucs_df: pd.DataFrame, ucs_db: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra os clientes ja existentes para adicionar os novos ao banco de dados

    Args:
        - ucs_df (DataFrame): clientes obtidos da leitura dos pdfs
        - ucs_db (DataFrame): clientes obtidos da leitura do banco de dados
    Returns:
        - DataFrame: tabela apenas com os clientes que precisam ser adicionados ao banco de dados
    """
    col = "nome"
    nomes_db = ucs_db[col]
    return ucs_df[~ucs_df[col].isin(nomes_db)].reset_index(drop=True)


def salva_novos_clientes(ucs_nova: pd.DataFrame, db_path: str) -> bool:
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


def criar_id_consumos(df_id: pd.DataFrame, df_consumo: pd.DataFrame) -> pd.DataFrame:
    """
    Cria coluna de id para nao gravar dados repetidos de consumo por cliente
    Args:
        - df_id (DataFrame): tabela com os ids originais
        - df_consumo (DataFrame): tabela com os consumos obtidos dos pdfs

    Returns:
        - DataFrame: tabela com os novos ids gerados a partir das datas e ids originais
    """
    df = df_id.merge(df_consumo, on="nome")
    df["id_datas"] = df["id"] + "-" + df["datas"].dt.strftime("%m%Y")
    return df.drop(columns=["nome", "id", "datas"])


def get_consumos_existentes(db_path: str) -> pd.DataFrame:
    """
    Le o banco de dados e obtem as informacoes de consumo

    Args:
        - db_path (str): Caminho para o arquivo .db
    Returns:
        - pd.DataFrame: DataFrame obtido do banco de dados
    """
    conn = sqlite3.connect(db_path)
    consumos = pd.read_sql("SELECT * FROM cpfl", conn)
    conn.close()
    return consumos


def mantem_novos_consumos(
    novos_consumos: pd.DataFrame, consumos_existentes: pd.DataFrame
) -> pd.DataFrame:
    """
    Filtra os consumos ja adicionados, e retorna apenas os consumos necessarios para insercao no banco de dados

    Args:
        - novos_consumos (pd.DataFrame): Tabela com os dados de consumo obtidos dos pdfs
        - consumos_existentes (pd.DataFrame): Tabela com os dados de consumo obtidos do banco de dados
    Return:
        - pd.DataFrame: Tabela apenas com os dados de consumo que nao estao presentes no banco de dados
    """
    id_col = "id_datas"
    ids = consumos_existentes[id_col]
    return novos_consumos[~novos_consumos[id_col].isin(ids)]


def remove_datas_duplicadas(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Args:
        - df (DataFrame): Tabela original
        - col (str): Nome da coluna de datas
    """

    return df.iloc[df.reset_index()[col].drop_duplicates().index]


def salva_novos_consumos(novos_consumos: pd.DataFrame, db_path: str) -> bool:
    """
    Adiciona os novos consumos com seus respectivos ids ao banco

    Args:
        - novos_consumos (DataFrame): consumos ainda nao adicionados ao banco
        - db_path (str): caminho para o banco de dados
    Returns:
        - bool: status de sucesso ou falha na insercao dos novos valores
    """

    try:
        conn = sqlite3.connect(db_path)
        novos_consumos.to_sql("cpfl", conn, index=False, if_exists="append")
        conn.close()
        return True
    except Exception:
        return False
