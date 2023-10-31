import regex as re
import pandas as pd
from typing import Any, List, Tuple
from Fatura import Fatura


class COPEL(Fatura):
    def __init__(self, path: str):
        super().__init__(path)
    
    def _consumo_posicao(self, energia_descricao: str) -> int:
        """
        Encontra o índice de início da descrição de energia na página da fatura.

        Args:
            energia_descricao (str): A descrição da energia a ser encontrada (consumo ponta ou fora ponta).

        Returns:
            int: O índice de início da descrição de energia.
        """

        indice = self._ultima_pagina.find(energia_descricao)
        assert (
            indice != -1
        ), f"Tipo de energia não encontrado, esperado: 'consumo ponta' ou 'consumo fora de ponta' || Obtido {energia_descricao} - Cliente {self._nome}"
        self.indice = indice
        return indice
    
    @Fatura.distribuidora.getter
    def distribuidora(self) -> str:
        return self.__class__.__name__

    @Fatura.consumo.setter
    def consumo(self, flag: Any):
        cols = {0: "datas", 1:"valor", 2:"data_venc", 3: "data_pgto", 4: "consumo_ponta", 5:"consumo_fora_de_ponta", 6: "demanda_ponta", 7: "demanda_fora_de_ponta"}       
        index = self._ultima_pagina.find('mês/ano faturavencimento pagamento pontafora pta. pontafora pta. pontafora pta. pontafora pta.')
        ultima_pagina_lista = self.ultima_pagina[index:].split("\n")[1:]
        df = self.dataframe(ultima_pagina_lista)
        df = df[df.columns[:8]].rename(columns=cols)
        df = self.arruma_consumos_nao_separados(df)
        df = self.remove_outliers(self.muda_tipo(df))
        self._consumo = df[["datas", "consumo_ponta", "consumo_fora_de_ponta"]]
        self.demanda = df[["datas", "demanda_ponta", "demanda_fora_de_ponta"]]
        self.data = df.datas
    
    @Fatura.data.setter
    def data(self, datas: pd.Series):
        if not self._data:
            self._data = datas

    @Fatura.demanda.setter
    def demanda(self, demandas: pd.DataFrame):
            self._demanda = demandas

    @Fatura.medida_consumo.setter
    def medida_consumo(self, flag: Any) -> None:
        self._medida_consumo = re.search("consumo \((\wwh)", self.ultima_pagina).group(1)

    @Fatura.medida_demanda.setter
    def medida_demanda(self, flag: Any) -> None:
        self._medida_demanda = re.search("demanda \((\ww)", self.ultima_pagina).group(1)

    @Fatura.nome.setter
    def nome(self, flag: Any):
        if self._ultima_pagina.split("\n")[1] != "segunda via":
            self._nome = self._ultima_pagina.split("\n")[1]
        else:
            self._nome = self._ultima_pagina.split("\n")[3]
    
    def dataframe(self, pagina_lista: List[str]):
        return pd.DataFrame(
            list(
                map(
                    lambda i:list(filter(lambda x: x!="", i)),
                    list(map(lambda x: x.split(" "), pagina_lista))
                    )
                )
        )

    def main(self):
        self.nome = None
        self.medida_consumo = None
        self.medida_demanda = None
        self.consumo = None

    def arruma_data_sem_pgto(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Algumas datas de pgto mais recentes ainda nao processaram ou nao receberam pgto e estao em branco, por isso, 
        os dados de consumo podem ser deslocados para a posicao dessa data, desarrumando o dataframe, o script checa se o valor é uma data
        caso nao seja, faz as alteracoes nas demais colunas e no valor da coluna de data_pgto
        """
        for val in df["data_pgto"]:

            if val.find("/") == -1:
                index = df["data_pgto"].values.tolist().index(val)
                colunas_incorretas = ["data_pgto", "consumo_ponta", "consumo_fora_de_ponta", "demanda_ponta"]
                colunas_corretas = ["consumo_ponta", "consumo_fora_de_ponta", "demanda_ponta", "demanda_fora_de_ponta"]
                df = self.atribui_valores_corretos(df, colunas_corretas, colunas_incorretas, index)
        return df

    def arruma_consumos_nao_separados(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Alguns pdf's são lidos de forma incorreta, não separando os consumos P e FP (100kWh P e 1000kWh FP podem ser lidos como 1001000)
        o script separa esses consumos e os ajusta suas respectivas colunas
        """
        df = self.arruma_data_sem_pgto(df)
        if self.checa_diferenca(df, 100):
            num_chars = len(df["consumo_ponta"].iloc[1])
            df, p, fp = self.atribui_demanda_P_e_FP(df, num_chars)
            df.loc[0].consumo_ponta = p
            df.loc[0].consumo_fora_de_ponta = fp
        return df

    def atribui_demanda_P_e_FP(self, df: pd.DataFrame, num_chars: int) -> Tuple[pd.DataFrame, str, str]:
            p, fp = df.consumo_ponta.iloc[0][:num_chars], df.consumo_ponta.iloc[0][num_chars:]
            df.loc[0, ["demanda_ponta", "demanda_fora_de_ponta"]] = df[["consumo_fora_de_ponta", "demanda_ponta"]].iloc[0].values
            return df, p, fp

    def atribui_valores_corretos(self, df: pd.DataFrame, colunas_corretas: List[str], colunas_incorretas: List[str], index: int) -> pd.DataFrame:
        df.loc[index, colunas_corretas] = df[colunas_incorretas].iloc[index].values
        df.loc[index, ["data_pgto"]] = "01/01/1900"
        return df

    def checa_diferenca(self, df: pd.DataFrame, lim: float) -> float:
        return df["consumo_ponta"].astype(float).iloc[0] / df["consumo_ponta"].astype(float).iloc[1] > lim

    def muda_tipo(self, df: pd.DataFrame) -> pd.DataFrame:
        df.demanda_ponta = pd.to_numeric(df.demanda_ponta, errors='coerce')
        df.demanda_fora_de_ponta = pd.to_numeric(df.demanda_fora_de_ponta, errors='coerce')
        df.consumo_ponta = pd.to_numeric(df.consumo_ponta, errors='coerce')
        df.consumo_fora_de_ponta = pd.to_numeric(df.consumo_fora_de_ponta, errors='coerce')
        df.datas = pd.to_datetime(df.datas, format="%m/%Y")
        return df
            
    def remove_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        cols = ["consumo_ponta", "consumo_fora_de_ponta", "demanda_ponta", "demanda_fora_de_ponta"]
        for col in cols:
            lim_max = df[col].quantile(0.9)
            lim_min = df.consumo_ponta.mean() - df.consumo_ponta.std()
            if df[col].max() > 10 * lim_max:
                df = df.loc[df[col] < lim_max].reset_index(drop=True)
                
            index = df[(df.consumo_ponta / lim_min) < 0.0001].index
            df = df.drop(index)
        return df
