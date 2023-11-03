import pandas as pd
import regex as re
from scripts.classes.Fatura import Fatura
from typing import Any, List, Tuple


class ELEKTRO(Fatura):
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
    
    @Fatura.distribuidora.getter
    def distribuidora(self) -> str:
        return self.__class__.__name__

    @Fatura.consumo.setter
    def consumo(self, flag: Any):
        """
        Define os Consumos ponta e fora ponta.

        self._consumo: Dict[str, List[str]]
        """        
        self._consumo = "fazendo"
    
    @Fatura.data.setter
    def data(self, flag: Any):
        if not self._data:
            dt_str = self.data_string()
            dt_datetime = pd.to_datetime(dt_str, format="%m/%Y")
            self._data = pd.Series(dt_datetime, name="datas")

    @Fatura.demanda.setter
    def demanda(self, flag: Any):
        if not self._demanda:
            self._demanda = "fazendo"

    @Fatura.medida_consumo.setter
    def medida_consumo(self, flag: Any):
        self._medida_consumo = re.search('consumo ponta te (\wwh)', self.primeira_pagina).group(1)

    @Fatura.medida_demanda.setter
    def medida_demanda(self, flag: Any):
        self._medida_demanda = re.search('demanda tusd (\ww)', self.primeira_pagina).group(1)

    @Fatura.nome.setter
    def nome(self, flag: Any):
        if "segunda via" in self._primeira_pagina:
            self._nome = self._primeira_pagina.split("\n")[3]
        else:
            self._nome = self._primeira_pagina.split("\n")[1]

    @Fatura.ths.setter
    def ths(self, flag: Any):
        self._ths = re.search("horária (\w*)\s/", self._primeira_pagina).group(1)

    def main(self):
        self.ths, self.nome = None, None
        self.medida_consumo, self.medida_demanda = None, None
        self.data = None

    def data_string(self) -> str:
        return re.search("leitura atual: \d.*/(\d*/\d*)", self.primeira_pagina).group(1)
