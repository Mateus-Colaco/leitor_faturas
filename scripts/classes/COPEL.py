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
    def data(self, primeiro_indice: int):
        if not self._data:
            self._data = "fazendo"

    @Fatura.medida_consumo.setter
    def medida_consumo(self, indice: int) -> None:
        self._medida_consumo = "fazendo"

    @Fatura.nome.setter
    def nome(self, flag: Any):
        self._nome = "fazendo"
           
    def main(self):
        print("nao desenvolvido")