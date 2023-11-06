import warnings
import regex as re
import pandas as pd
from scripts.classes.Fatura import Fatura
from typing import Any, List, Tuple
from pandas import DataFrame, Series
warnings.filterwarnings('ignore')

arruma_string_df: str = lambda x: x.rstrip().replace(".", "").replace(",", ".")
arruma_string_nome: str = lambda x: re.split("\s{2,}|\s\-\s", x)[0].lstrip().replace(" ", "_")

class ENEL(Fatura):
    def __init__(self, path: str):
        super().__init__(path)
           
    def main(self):
        temp = self.historico_dataframe(self.historico_lista())
        self.data = temp.datas
        self.consumo = temp[["datas", "consumo_ponta", "consumo_fora_de_ponta"]]
        self.demanda = temp.drop(["consumo_ponta", "consumo_fora_de_ponta"], axis=1)
        self.nome = None
        self.ths = None
        self.medida_demanda, self.medida_consumo = self.encontra_medidas()

    def arruma_tipos(self, df: DataFrame) -> DataFrame:
        df.datas = df.datas.apply(self.transforma_data)
        df.loc[:, df.columns[1:]] = df.loc[:, df.columns[1:]].apply(pd.to_numeric)
        return df
    
    def encontra_medidas(self) -> Tuple[str, str]:
        padrao1 = "quant\.\n?.*\((\ww)\/\s?\n?(\wwh)"
        resultado = re.search(padrao1, self.primeira_pagina)
        if not resultado:
            padrao2 = "\((\ww)\/(\wwh)\/"
            resultado = re.search(padrao2,self.primeira_pagina)
        return resultado.groups()

    def historico_lista(self) -> List[str]:
        pattern = "\d\s(28|30|31)( \w+|\w+)"
        indice0 = self.primeira_pagina.find("mês/ano")
        indice1 = re.search(pattern, self.primeira_pagina[indice0:]).span(2)[0] + indice0
        return self.primeira_pagina[indice0:indice1].split("\n")[2:]
    
    def historico_dataframe(self, historico_lista: List[str]) -> DataFrame:
        df = DataFrame(
            [re.split("\s+|\t", arruma_string_df(x)) for x in historico_lista]
        )
        if len(df.columns) == 6:
            cols = ["datas", "demanda_ponta", "demanda_fora_de_ponta", "consumo_ponta", "consumo_fora_de_ponta", "dias"]
        else:
            cols = ["datas", "demanda_fora_de_ponta", "consumo_ponta", "consumo_fora_de_ponta", "dias"]
        df.columns = cols
        return self.arruma_tipos(df.drop(["dias"], axis=1))

    @Fatura.distribuidora.getter
    def distribuidora(self) -> str:
        return self.__class__.__name__

    @Fatura.consumo.setter
    def consumo(self, consumos: DataFrame):
        self._consumo = consumos
    
    @Fatura.data.setter
    def data(self, datas: Series):
        if not self._data:
            self._data = datas
    
    @Fatura.demanda.setter
    def demanda(self, demandas: DataFrame):
        if not self._demanda:
            self._demanda = demandas

    @Fatura.medida_consumo.setter
    def medida_consumo(self, medida: str):
        self._medida_consumo = medida
    
    @Fatura.medida_demanda.setter
    def medida_demanda(self, medida: str):
        self._medida_demanda = medida

    @Fatura.nome.setter
    def nome(self, flag: Any):
        indicador_nome = "nome do pagador/cpf/cnpj/endereço"
        indice = self.primeira_pagina.find(indicador_nome) + len(indicador_nome)
        pagina_lista = self.primeira_pagina[indice:].split("\n")
        if " " in pagina_lista:
            pagina_lista.remove(" ")
        
        if "" in pagina_lista:
            pagina_lista.remove("")

        self._nome = arruma_string_nome(pagina_lista[0])

    @Fatura.ths.setter
    def ths(self, flag: Any):
        indice0 = self.primeira_pagina.find(" - a4 - ") + len(" - a4 - ")
        indice1 = self.primeira_pagina[indice0: ].find(" -") + indice0 
        self._ths = self.primeira_pagina[indice0: indice1]
