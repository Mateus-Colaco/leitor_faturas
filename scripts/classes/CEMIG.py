import regex as re
from Fatura import Fatura
from typing import Any, List, Tuple
from pandas import DataFrame, Series


class CEMIG(Fatura):
    def __init__(self, path: str):
        super().__init__(path)
        self.split_str = lambda x: re.split("\s+", x)[:-1]
    
    @Fatura.distribuidora.getter
    def distribuidora(self) -> str:
        return self.__class__.__name__

    @Fatura.consumo.setter
    def consumo(self, consumo: DataFrame):
        self._consumo = consumo
    
    @Fatura.data.setter
    def data(self, datas: Series):
        self._data = datas

    @Fatura.demanda.setter
    def demanda(self, demanda: DataFrame):
        self._demanda = demanda

    @Fatura.medida_consumo.setter
    def medida_consumo(self, indice: int):
        self._medida_consumo = re.search("energia\((\w*)\)", self._primeira_pagina).group(1)
    
    @Fatura.medida_demanda.setter
    def medida_demanda(self, indice: int):
        self._medida_demanda = re.search("demanda\((\w*)\)", self._primeira_pagina).group(1)

    @Fatura.nome.setter
    def nome(self, flag: Any):
        self._nome = re.search("cliente: (\w.*) unidade:", self._ultima_pagina).group(1)

    @Fatura.ths.setter
    def ths(self, texto_a_encontrar: str):
        self._ths = re.search(texto_a_encontrar, self._primeira_pagina).group(1)
           
    def main(self):
        idx_0, idx_1 = self.indices()
        dados_lista = list(map(self.split_str, self.dados_lista(idx_0, idx_1)))
        cols = ["datas", "demanda_ponta", "demanda_fora_de_ponta", "consumo_ponta", "consumo_fora_de_ponta"]
        df = DataFrame(dados_lista, columns=cols)
        df.datas = df.datas.apply(self.transforma_data)
        
        self.consumo = df[["datas", "consumo_ponta", "consumo_fora_de_ponta"]]
        self.demanda = df[["datas", "demanda_ponta", "demanda_fora_de_ponta"]]
        self.data = df["datas"]
        self.ths = "ths (\w+)\s"
        
        self.nome = None
        self.medida_demanda = None
        self.medida_consumo = None

    def dados_lista(self, idx_0: int, idx_1: int) -> List[str]:
        return self._primeira_pagina[idx_0: idx_1].split("\n")

    def indices(self) -> Tuple[int, int]:
        encontrar = "hp hfp hp hfp hr"
        idx_0 = self._primeira_pagina.find(encontrar) + len(encontrar) + 1
        idx_1 = self._primeira_pagina.find("reservado ao fisco")
        return idx_0, idx_1
    