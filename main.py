import os
import sys
import contextlib
import pandas as pd
from typing import Dict, List, Tuple
from PyPDF2.errors import PdfReadError
from sqlite3 import Connection, Cursor
from scripts.funcoes.f_database import *
from scripts.classes.Fatura import Fatura
from scripts.classes.Identificador import Identificador

join_path = lambda y, x: os.path.join(y, x)
filename = lambda path, filetype: path.split("\\")[-1].split(filetype)[0]

def adiciona_data_para_forecast(df: DataFrame) -> DataFrame:
    """
    Adiciona nova linha ao dataframe com a data a ser realizada a previsão
    """
    nova_data = pd.DateOffset(months=1) + df.datas.iloc[-1]
    df = pd.concat([df, pd.DataFrame([[pd.NA for i in range(len(df.columns))]], columns=df.columns)], axis=0, ignore_index=True).reset_index(drop=True)
    df.datas.iloc[-1] = nova_data
    return df


def set_distribuidoras(paths: List[Dict[str, str]]) -> Dict[str, Fatura]:
    """
    Identifica as Distribuidoras pelas Faturas e retorna uma relação Distribuidora <-> Faturas

    Args:
        - paths (List[Dict[str, str]]): Relação UC <-> Caminho para a Fatura
    
    Returns:
        - Dict[str, Fatura]: relações Distribuidora <-> Faturas
    """
    DISTRIBUIDORAS: Dict[str, Fatura] = {}
    for info in paths:
        with contextlib.suppress(IndexError,TypeError, PdfReadError):
            path: str = info.get(*info.keys())
            DISTRIBUIDORA: Fatura = Identificador(infos=info).main()(path)
            name: str = DISTRIBUIDORA.distribuidora
            if name not in DISTRIBUIDORAS.keys():
                DISTRIBUIDORAS[name]: Fatura = []
            if name != None:
                if name in ["CPFL", "EDP", "COPEL", "CEMIG", "ENEL"]:
                    DISTRIBUIDORA.main()
                DISTRIBUIDORAS[name].append(DISTRIBUIDORA)
    return DISTRIBUIDORAS


def set_consumos_e_demandas(DISTRIBUIDORAS: Dict[str, Fatura]) -> Tuple[DataFrame, DataFrame]:
    """
    Obtem os dados de consumo e demanda de cada UC e DISTRIBUIDORA e transforma em um DataFrame/Tabela com essas informções

    Args:
        - DISTRIBUIDORAS (Dict[str, Fatura]): relações Distribuidora <-> Faturas 

    Returns:
        - Tuple[DataFrame, DataFrame]: DataFrames de Consumos e Demandas
    """
    CONSUMOS = []
    DEMANDAS = []
    for key in DISTRIBUIDORAS:
        if key in ["CPFL", "EDP", "COPEL", "CEMIG", "ENEL"]:
            for dis in DISTRIBUIDORAS[key]:
                consumo = set_consumo_df(dis)
                DEMANDAS.append(dis.demanda)
                CONSUMOS.append(consumo)
    return (
        pd.concat(CONSUMOS, ignore_index=True).set_index('datas'),
        pd.concat(DEMANDAS, ignore_index=True).set_index('datas')
    )


def set_consumo_df(dis: Fatura) -> DataFrame:
    """
    Monta o dataframe de consumo com os valores da classe original (Fatura)
    """
    consumo = dis.consumo
    consumo['nome'] = dis.nome.replace(" ", "_")
    consumo['uc'] = dis._caminho.split("\\")[-1][:-4]
    consumo['distribuidora'] = dis.distribuidora
    consumo['medida_consumo'] = dis.medida_consumo
    consumo['medida_demanda'] = dis.medida_demanda
    consumo['ths'] = dis.ths
    return consumo


def set_infos(consumos: DataFrame, demandas: DataFrame) -> DataFrame:
    """
    Une os dataframes de consumo e demanda, transforma os tipos de dados numericos de texto para float,
    e Remove possiveis outliers (acima do quantil 0.9)

    Args:
        - consumos (DataFrame): Tabela de consumo 
        - demandas (DataFrame): Tabela de demanda 
    
    Returns:
        - DataFrame: Um único dataframe agregando as informações das duas tabelas e sem os outliers
    """
    infos = pd.concat([demandas, consumos], axis=1).drop_duplicates().fillna(-1).reset_index()
    infos.consumo_ponta = infos.consumo_ponta.astype(float)
    infos.demanda_ponta = infos.demanda_ponta.astype(float)
    infos.demanda_fora_de_ponta = infos.demanda_fora_de_ponta.astype(float)
    return remove_outliers(infos, "consumo_ponta", 0.9)


def clientes_db(db_path: str, PATH: str) -> Tuple[DataFrame, DataFrame, bool]:
    """
    Obtem as informações dos pdfs e transforma em um dataframe com as informações uteis (infos), adiciona um uuid aos clientes
    compara os clientes obtidos com os existentes e salva os novos clientes na tabela clientes do banco de dados
    Retorna as informações uteis obtidas, os clientes ja existentes, e o status da tentativa de gravar os novos clientes ao banco

    Args:
        - db_path (str): caminho para o banco de dados
        - PATH (str): caminho para os pdfs

    Returns:
        - Tuple[DataFrame, DataFrame, bool]: informações uteis obtidas, 
                                             clientes ja existentes, 
                                             status
    """
    FOLDERS: List[str] = os.listdir(PATH)
    PATHS: List[Dict[str, str]] = [{filename(x, ".pdf"): join_path(PATH, x)} for x in FOLDERS]
    DISTRIBUIDORAS: Dict[str, Fatura] = set_distribuidoras(PATHS)
    infos: DataFrame = pd.concat(
        set_consumos_e_demandas(DISTRIBUIDORAS), axis=1
    ).drop_duplicates().fillna(-1).reset_index()
    clientes: DataFrame = adiciona_novo_id_clientes(infos.copy())
    clientes_existentes: DataFrame = get_clientes_existentes(db_path)
    return infos, clientes_existentes, salva_novos_clientes(mantem_novos_clientes(clientes, clientes_existentes, col="nome"), db_path)


def dados_por_tabela(db_path: str) -> Dict[str, List[str]]:
    """
    Para cada distribuidora:
        - Lê o banco de dados e obtem informações da tabela clientes como nome, unidade consumidora (uc);
        consumo total (consumo ponta + consumo fora ponta) e data da tabela da distribuidora, e une os dados com base no id do cliente
    Agrega os dados obtidos em um dicionario (Dict[str, List[str]])
        Args:
            - 
    """
    resultados: Dict[str, List[str]] = {}
    conn: Connection = sqlite3.connect(db_path)
    cursor: Cursor = conn.cursor()
    tabelas = tabelas_db(db_path)
    for distribuidora in tabelas:
        cursor.execute(
            f"""SELECT nome, uc, (consumo_ponta+consumo_fora_de_ponta) AS consumo_total, SUBSTR(id_datas, -6) AS data FROM {distribuidora} INNER JOIN
            clientes ON id = SUBSTR(id_datas, 1, LENGTH(id_datas) - 7)
            """
        )
        result: List[str] = cursor.fetchall()
        resultados |= {distribuidora: result}
    conn.close()
    return resultados


def distribuidora_db(
        db_path: str, 
        consumos_df: DataFrame, 
        clientes_existentes: DataFrame, 
        distribuidora: str
        ) -> bool:
        """
        Salva os consumos ainda não adicionados ao banco, para a distribuidora especificada

        Args:
            - db_path (str): caminho para o banco de dados,
            - consumos_df (DataFrame): tabela de consumos obtidos da leitura dos pdfs,
            - clientes_existentes (DataFrame): tabela com os clientes do banco de dados,
            - distribuidora (str): Nome da tabela que os valores serão adicionados

        Returns:
            - bool: status da realização da gravação dos dados (true = dados salvos, false = dados não foram salvos)
        """
        temp: DataFrame = criar_id_consumos(df_id=clientes_existentes, df_consumo=consumos_df.drop_duplicates(subset=["nome", "datas"]))
        consumos_db: DataFrame = get_consumos_existentes(db_path, distribuidora=distribuidora)
        novos_consumos: DataFrame = mantem_novos_consumos(novos_consumos=temp, 
                                                          consumos_existentes=consumos_db)
        return salva_novos_consumos(novos_consumos, 
                                    db_path, 
                                    distribuidora)


def tabelas_db(db_path: str) -> List[str]:
    """
    Obtem todas as tabelas do banco de dados exceto a tabela clientes e retorna uma lista com o nome delas

    Args:
        - db_path (str): Caminho para o banco de dados
    
    Returns:
        - List[str]: lista das tabelas(distribuidoras) obtidas
    """
    conn: Connection = sqlite3.connect(db_path)
    cursor: Cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' and name != 'clientes';")
    tabelas: List[str] = list(map(lambda x: x[0], cursor.fetchall()))
    conn.close()
    return tabelas


def salva_consumos_csv(resultados: Dict[str, List[str]]) -> None:
    """
    Lê os resultados e transforma em DataFrame, transforma os dados do tipo string para datetime quando necessario,
    arruma os valores da coluna nome como 'nome - uc'
    para cada 'nome - uc' => salva um arquivo csv no caminho especificado

    Args:
        - resultados (Dict[str, List[str]]): valores obtidos da função dados_por_tabela()
    """
    path_base = r"q:\0. PASTAS PESSOAIS\MATEUS COLAÇO\MODELOS\ARIMA\previsao_consumo_arima\distribuidoras"
    for key in resultados:
        path_key: str = os.path.join(path_base, key)
        os.makedirs(path_key, exist_ok=True)
        df: DataFrame = pd.DataFrame(resultados[key], columns=["nome", "uc", "consumo_total", "datas"])
        df.nome = df.nome.str.rstrip().str.lstrip() + "-" + df.uc.str.rstrip().str.lstrip()
        df.datas = pd.to_datetime(df.datas, format="%m%Y")
        for nome in df.nome.unique():
            path_csv: str = os.path.join(path_key, str(nome))
            os.makedirs(path_csv, exist_ok=True)
            adiciona_data_para_forecast(df.loc[df.nome == nome][["datas", "consumo_total"]].sort_values("datas", ascending=True)).to_csv(f"{path_csv}\\consumo_{nome}.csv", index=False, sep=';')


def main(path: str):
    PATH = path
    db_path = r'.\leitor_pdf\banco_de_dados\distribuidoras.db'
    consumos_df, clientes_existentes, _ = clientes_db(db_path=db_path, PATH=PATH)
    for distribuidora in consumos_df.distribuidora.unique():
        consumos_df_filtrado = consumos_df.loc[consumos_df.distribuidora == distribuidora]
        distribuidora_db(db_path=db_path, consumos_df=consumos_df_filtrado, clientes_existentes=clientes_existentes, distribuidora=distribuidora)
    resultados = dados_por_tabela(db_path)
    salva_consumos_csv(resultados)

if __name__ == "__main__":
    path = sys.argv[1]
    main(path)
