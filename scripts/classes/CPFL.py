import regex as re
from scripts.classes.Fatura import Fatura
import pandas as pd
from datetime import datetime
from typing import Any, List, Tuple
from scripts.funcoes.f_database import arruma_decimal

# FALTA DESENVOLVER MEDIDA DEMANDA

class CPFL(Fatura):
    def __init__(self, path: str) -> None:
        super().__init__(path)
    
    def _texto_posicao(self, energia_descricao: str) -> int:
        """
        Encontra o índice de início da descrição de energia na página da fatura.

        Args:
            energia_descricao (str): A descrição da energia a ser encontrada (consumo ponta ou fora ponta, ou demanda).

        Returns:
            int: O índice de início da descrição de energia.
        """

        indice = self._ultima_pagina.find(energia_descricao)
        assert (
            indice != -1
        ), f"Tipo de energia não encontrado, esperado: 'consumo ponta' ou 'consumo fora de ponta' || Obtido {energia_descricao} - Cliente {self._nome}"
        return indice

    def _encontra_ano_anterior(self, datas: str) -> int:
        """
        Retorna o ano A-1 com base nas datas fornecidas.

        Args:
            datas (str): Uma string contendo datas no formato 'Mês/Ano'.

        Returns:
            int: O ano A-1.
        """
        return int(datas[:4]) - 1

    def _encontra_info(self, energia_descricao: int) -> List[str]:
        """
        Extrai informações de consumo da página da fatura.

        Args:
            indice (int): O índice de início da busca na página.

        Returns:
            List[str]: Uma lista de strings contendo informações de consumo.
        """
        indice = self._texto_posicao(energia_descricao)
        self.medida_consumo = indice
        lista_consumo = self._lista_consumo(indice)
        energia_ind, dias_ind = self._get_indices(lista_consumo)
        return arruma_decimal(pd.Series(lista_consumo[energia_ind+1: dias_ind]))

    def _get_indices(self, lista: List[str]) -> Tuple[int, int]:
        return lista.index("energia_wh"), lista.index("dias")

    def _lista_consumo(self, indice: int) -> List[str]:
        return (
            self._ultima_pagina[indice:]
            .replace("kwh", "\nenergia_wh")
            .replace("mwh", "\nenergia_wh")
            .replace(" ", "")
            .replace("dias", "\ndias")
            .split("\n")
        )
    
    def _lista_demanda(self, indice: int) -> List[str]:
        ultimo_index = indice + self.ultima_pagina[indice:].find("dias")
        texto = self.ultima_pagina[indice:ultimo_index]
        info = re.sub("ll.*", "", texto.replace("dias", "\ndias")).split("\n")
        return list(filter(lambda x: x != '', info))

    @Fatura.distribuidora.getter
    def distribuidora(self) -> str:
        return self.__class__.__name__ 

    @Fatura.consumo.setter
    def consumo(self, flag: Any):
        """
        Define os Consumos ponta e fora ponta.

        self._consumo: DataFrame
        """        
        lista_consumoP = self._encontra_info("consumo ponta")
        lista_consumoFP = self._encontra_info("consumo fora de ponta")
        self._consumo = pd.DataFrame({
            "datas": self.data.to_list(),
            "consumo_ponta": lista_consumoP, 
            "consumo_fora_de_ponta": lista_consumoFP
        })
    
    @Fatura.data.setter
    def data(self, primeiro_indice: int):
        if not self._data:
            datas_lista = self.datas_lista_filtro(primeiro_indice)
            datas_series = pd.Series(datas_lista, name="datas")
            self._data = datas_series.apply(self.transforma_data)

    @Fatura.demanda.setter
    def demanda(self, flag: Any):
        if self.ths == "verde":
            lista_demanda = self.filtra_lista_demanda("demanda - [")
            self._demanda = pd.concat([self.data, lista_demanda], axis=1, ignore_index=True).rename(columns={0: "datas", 1:"demanda_fora_de_ponta"})
        elif self.ths == "azul":
            lista_demandaP = self.filtra_lista_demanda("demanda ponta - [")
            lista_demandaFP = self.filtra_lista_demanda("demanda fora de ponta - [")
            self._demanda = pd.concat([self.data, lista_demandaP, lista_demandaFP], axis=1, ignore_index=True).rename(columns={0: "datas", 1:"demanda_ponta", 2:"demanda_fora_de_ponta"})

    @Fatura.medida_consumo.setter
    def medida_consumo(self, indice: int):
        pag = self.ultima_pagina[indice:] 
        self._medida_consumo = (
            "kwh"
            if pag.find("[kwh]") != -1
            else "mwh"
            if pag.find("[mwh]") != -1
            else "nao encontrado"
        )

    @Fatura.nome.setter
    def nome(self, flag: Any):
        self._nome = self.primeira_pagina.split("\n")[0].replace(" ", "_").replace(".", "")

    @Fatura.ths.setter
    def ths(self, flag: Any):
        THSs = {"Cliente Livre-A4".lower(): "azul", "verde": "verde", "Tarifa Azul-A4".lower(): "azul"}
        ths = [x in self._primeira_pagina for x in THSs]
        if sum(ths) != 0:
            assert sum(ths) == 1, f"Mais de uma THS encontrada {self.nome}"
            key = list(THSs.keys())[ths.index(True)]
            self._ths = THSs[key]
        else:
            self._ths = "ths_nao_encontrada"

    def main(self) -> None:
        self.data = self._texto_posicao("consumo fora de ponta")
        self.nome = None
        self.ths = None
        self.consumo = None
        self.demanda = None

    def datas_lista_sem_filtro(self, indice: int) -> List[str]:
        return re.sub("(jul)?ll.*", r"\1", self.ultima_pagina[indice:]).split("\n")

    def datas_lista_filtro(self, indice: int) -> List[str]:
        datas = self.datas_lista_sem_filtro(indice)
        try:
            ultimo_index = datas.index("")
        except Exception:
            datas = "\n".join(datas)
            ultimo_index = re.search("\n\w.*dias", datas).span()[0] 
            datas = datas[:ultimo_index].split("\n")
            # medida = self._consumo["medida_ponta"][0].lower()
            # ultimo_index = list(filter(lambda x: medida in x, datas[1:]))[0]
            # ultimo_index = datas.index(ultimo_index) + 1
        return self.datas_texto(datas[1:ultimo_index])

    def datas_texto(self, datas:List[str]):
        datas = "\n".join(datas)
        ano_anterior = self.encontra_ano_anterior(datas)
        inputs = (datas, ano_anterior)
        if ano_anterior := self.set_ano_anterior(*inputs):
            return self.set_ano_atual(*inputs) + ano_anterior
        return self.set_ano_atual(*inputs)
        

    def encontra_ano_anterior(self, datas:str) -> int:
        """
        Retorna o ano A-1 com base nas datas fornecidas.

        Args:
            datas (str): Uma string contendo datas no formato 'Mês/Ano'.

        Returns:
            int: O ano A-1.
        """
        return int(datas[:4]) - 1

    def filtra_lista_demanda(self, energia_desc: str):
        indice = self._texto_posicao(energia_desc)
        indice_fim = self.ultima_pagina[indice + len(energia_desc):].find("]") + (indice + len(energia_desc))
        self._medida_demanda = self.ultima_pagina[indice + len(energia_desc):indice_fim]
        lista_demanda = list(filter(lambda x: x != '', self._lista_demanda(indice)))
        return pd.Series([float(valor.replace(',', '.')) for valor in lista_demanda if valor.replace(',', '.').replace('.', '', 1).isdigit()], name="demanda")

    def set_ano_anterior(self, datas: str, ano_anterior: int) -> List[str]:
        sep = datas.find(str(ano_anterior))
        ano_anterior_lista = re.sub("\s", "\n", datas[sep:]).split("\n")
        ano_anterior_lista = list(filter(lambda x: x != "", ano_anterior_lista))[1:]
        return [f"{x[:3]}/{ano_anterior}" for x in ano_anterior_lista]

    def set_ano_atual(self, datas: str, ano_anterior: int) -> List[str]:
        sep = datas.find(str(ano_anterior))

        if sep != -1:
            ano_atual_lista = re.sub("\s", "\n", datas[:sep]).split("\n")
        else:
            ano_atual_lista = re.sub("\s", "\n", datas[:]).split("\n")

        ano_atual_lista = list(filter(lambda x: x != "", ano_atual_lista))[1:]
        return [f"{x[:3]}/{str(ano_anterior+1)}" for x in ano_atual_lista]
    