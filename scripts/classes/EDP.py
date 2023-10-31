import pandas as pd
from Fatura import Fatura
from typing import Any, List, Tuple



class EDP(Fatura):
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

    def _encontra_ano_anterior(self, datas: str):
        """
        Retorna o ano A-1 com base nas datas fornecidas.

        Args:
            datas (str): Uma string contendo datas no formato 'Mês/Ano'.

        Returns:
            int: O ano A-1.
        """
        return int(datas[:4]) - 1

    def _find_consumo(self, energia_descricao: int) -> List[str]:
        """
        Extrai informações de consumo da página da fatura.

        Args:
            indice (int): O índice de início da busca na página.

        Returns:
            List[str]: Uma lista de strings contendo informações de consumo.
        """
        indice = self._consumo_posicao(energia_descricao)
        self.medida_consumo = indice
        lista_consumo = self._lista_consumo(indice)
        energia_ind, dias_ind = self._get_indices(lista_consumo)
        return lista_consumo[energia_ind+1: dias_ind]

    def _get_indices(self, lista: List[str]) -> Tuple[int, int]:
        return lista.index("energia_wh"), lista.index("dias")

    def _lista_consumo(self, indice) -> List[str]:
        return (
            self._ultima_pagina[indice:]
            .replace("kwh", "\nenergia_wh")
            .replace("mwh", "\nenergia_wh")
            .replace(" ", "")
            .replace("dias", "\ndias")
            .split("\n")
        )
    
    @Fatura.consumo.setter
    def consumo(self, consumo: pd.DataFrame):
        self._consumo = consumo

    @Fatura.data.setter
    def data(self, data: pd.Series):
        if not self._data:
            self._data = data

    @Fatura.demanda.setter
    def demanda(self, demanda: pd.DataFrame):
        self._demanda = demanda
    
    @Fatura.distribuidora.getter
    def distribuidora(self) -> str:
        return self.__class__.__name__

    @Fatura.medida_consumo.setter
    def medida_consumo(self, flag: Any):
        self._medida_consumo = self.pdf["pagina_1"].split("\n")[0][-3:]

    @Fatura.medida_demanda.setter
    def medida_demanda(self, flag: Any):
        self._medida_demanda = self.pdf["pagina_1"].split("\n")[3][-2:]

    @Fatura.nome.setter
    def nome(self, flag: Any):
        _nome = self.primeira_pagina.split('cliente / endereço de entrega')[1]
        self._nome = _nome.split("\n")[1].rstrip().replace(" ", "_")
    
    @Fatura.ths.setter
    def ths(self, flag: Any):
        index = self._primeira_pagina.find("modalidade tarifária")
        self._ths = "verde" if "verde" in self._primeira_pagina[index:].split("\n")[1] else "azul"

    def main(self):
        self.ths = None
        self.nome = None
        df = self.dados_historico()
        self.data = df["datas"]
        self.demanda = df[["datas", "demanda_fora_de_ponta"]]
        self.consumo = df[["datas", "consumo_ponta", "consumo_fora_de_ponta"]]
        self.medida_consumo = None
        self.medida_demanda = None

    def delimitador_historico(self):
        """
        Define o delimitador da ultima pagina onde esta o historico de consumo e demanda
        """
        index = self.ultima_pagina.find("histórico de consumo")
        texto = self.ultima_pagina[:index].split("\n", )[:13]
        return list(map(lambda x: self.organiza_array(x), texto))

    def dados_historico(self) -> pd.DataFrame:
        array = self.delimitador_historico()
        return self.dados_ths_verde(array)

    def dados_ths_verde(self, array):
        df = pd.DataFrame(array, columns=['datas', 'consumo_ponta', 'consumo_fora_de_ponta_ind', 'consumo_fora_de_ponta_cap', 'demanda_fora_de_ponta'])
        df['consumo_fora_de_ponta'] = df['consumo_fora_de_ponta_ind'].astype(float) + df['consumo_fora_de_ponta_cap'].astype(float)
        
        df.drop(['consumo_fora_de_ponta_cap', 'consumo_fora_de_ponta_ind'], axis=1, inplace=True)
        df.datas = pd.to_datetime(df.datas, format="%m/%y")
        return df
    
    def organiza_array(self, array):
        return array.split(" ")[:5] if self.ths == 'verde' else array.split(" ")[:6]