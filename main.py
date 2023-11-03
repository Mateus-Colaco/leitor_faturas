import os
import sys
import contextlib
import regex as re
import pandas as pd
from scripts.funcoes.f_database import *
from scripts.classes.Fatura import Fatura
from typing import Dict, List, Tuple
from PyPDF2.errors import PdfReadError
from scripts.classes.Identificador import Identificador

join_path = lambda y, x: os.path.join(y, x)
filename = lambda path, filetype: path.split("\\")[-1].split(filetype)[0]


def set_distribuidoras(paths: List[Dict[str, str]]):
    DISTRIBUIDORAS = {}
    for info in paths:
        with contextlib.suppress(IndexError,TypeError, PdfReadError):
            path = info.get(*info.keys())
            DISTRIBUIDORA = Identificador(infos=info).main()(path)
            name = DISTRIBUIDORA.distribuidora
            if name not in DISTRIBUIDORAS.keys():
                DISTRIBUIDORAS[name] = []
            if name != None:
                if name in ["CPFL", "EDP", "COPEL", "CEMIG", "ENEL"]:
                    DISTRIBUIDORA.main()
                DISTRIBUIDORAS[name].append(DISTRIBUIDORA)
    return DISTRIBUIDORAS


def set_consumos_e_demandas(DISTRIBUIDORAS: Dict[str, Fatura]) -> Tuple[DataFrame, DataFrame]:
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
    consumo = dis.consumo
    consumo['nome'] = dis.nome.replace(" ", "_")
    consumo['uc'] = dis._caminho.split("\\")[-1][:-4]
    consumo['distribuidora'] = dis.distribuidora
    consumo['medida_consumo'] = dis.medida_consumo
    consumo['medida_demanda'] = dis.medida_demanda
    consumo['ths'] = dis.ths
    return consumo


def set_infos(consumos: DataFrame, demandas: DataFrame):
    infos = pd.concat([demandas, consumos], axis=1).drop_duplicates().fillna(-1).reset_index()
    infos.consumo_ponta = infos.consumo_ponta.astype(float)
    infos.demanda_ponta = infos.demanda_ponta.astype(float)
    infos.demanda_fora_de_ponta = infos.demanda_fora_de_ponta.astype(float)
    return remove_outliers(infos, "consumo_ponta", 0.9)


def clientes_db(db_path: str, PATH: str) -> Tuple[DataFrame, DataFrame, bool]:
    FOLDERS = os.listdir(PATH)
    PATHS = [{filename(x, ".pdf"): join_path(PATH, x)} for x in FOLDERS]
    DISTRIBUIDORAS = set_distribuidoras(PATHS)
    infos = pd.concat(
        set_consumos_e_demandas(DISTRIBUIDORAS), axis=1
    ).drop_duplicates().fillna(-1).reset_index()
    clientes = adiciona_novo_id_clientes(infos.copy())
    clientes_existentes = get_clientes_existentes(db_path)
    return infos, clientes_existentes, salva_novos_clientes(mantem_novos_clientes(clientes, clientes_existentes, col="nome"), db_path)


def dados_por_tabela(db_path: str) -> Dict[str, List[str]]:
    resultados = {}
    conn = sqlite3.connect(db_path)
    cursor= conn.cursor()
    tabelas = tabelas_db(db_path)
    for distribuidora in tabelas:
        cursor.execute(
            f"""SELECT nome, uc, (consumo_ponta+consumo_fora_de_ponta) AS consumo_total, SUBSTR(id_datas, -6) AS data FROM {distribuidora} INNER JOIN
            clientes ON id = SUBSTR(id_datas, 1, LENGTH(id_datas) - 7)
            """
        )
        result = cursor.fetchall()
        resultados |= {distribuidora: result}
    conn.close()
    return resultados


def distribuidora_db(db_path: str, consumos_df: DataFrame, clientes_existentes: DataFrame, distribuidora: str) -> bool:
        temp = criar_id_consumos(df_id=clientes_existentes, df_consumo=consumos_df.drop_duplicates(subset=["nome", "datas"]))
        consumos_db = get_consumos_existentes(db_path, distribuidora=distribuidora)
        novos_consumos = mantem_novos_consumos(
        novos_consumos=temp, consumos_existentes=consumos_db
        )
        return salva_novos_consumos(novos_consumos, db_path, distribuidora)


def tabelas_db(db_path):
    conn = sqlite3.connect(db_path)
    cursor= conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' and name != 'clientes';")
    tabelas = list(map(lambda x: x[0], cursor.fetchall()))
    conn.close()
    return tabelas

def salva_consumos_csv(resultados: Dict[str, List[str]]):
    path_base = r"q:\0. PASTAS PESSOAIS\MATEUS COLAÇO\MODELOS\ARIMA\previsao_consumo_arima\distribuidoras"
    for key in resultados:
        path_key = os.path.join(path_base, key)
        os.makedirs(path_key, exist_ok=True)
        df = pd.DataFrame(resultados[key], columns=["nome", "uc", "consumo_total", "datas"])
        df.nome = df.nome.str.rstrip().str.lstrip() + "-" + df.uc.str.rstrip().str.lstrip()
        df.datas = pd.to_datetime(df.datas, format="%m%Y")
        for nome in df.nome.unique():
            path_csv = os.path.join(path_key, nome)
            os.makedirs(path_csv, exist_ok=True)
            df.loc[df.nome == nome][["datas", "consumo_total"]].to_csv(f"{path_csv}\\consumo_{nome}.csv", index=False)
    
def main(path: str)    :
    # PATH = r"Q:\APOIO ATENDIMENTO\Faturas EcoExp\Setembro"
    PATH = path
    db_path = r'C:\Users\mateus.souza\Desktop\Projetos_n\leitor_pdf\banco_de_dados\distribuidoras.db'
    consumos_df, clientes_existentes, _ = clientes_db(db_path=db_path, PATH=PATH)
    for distribuidora in consumos_df.distribuidora.unique():
        consumos_df_filtrado = consumos_df.loc[consumos_df.distribuidora == distribuidora]
        distribuidora_db(db_path=db_path, consumos_df=consumos_df_filtrado, clientes_existentes=clientes_existentes, distribuidora=distribuidora)
    resultados = dados_por_tabela(db_path)
    salva_consumos_csv(resultados)

if __name__ == "__main__":
    path = sys.argv[1]
    main(path)
