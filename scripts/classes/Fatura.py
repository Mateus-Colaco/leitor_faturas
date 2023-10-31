from datetime import datetime
import PyPDF2
from typing import Dict, List

import pandas as pd


class Fatura:
    """
    Classe que representa uma fatura em PDF.

    Args:
        caminho (str): O caminho para o arquivo PDF da fatura.

    Attributes:
        caminho (str): O caminho para o arquivo PDF da fatura.
        pdf (PyPDF2.PdfReader): Um objeto PyPDF2.PdfReader para ler o PDF.
        primeira_pagina e ultima_pagina (str): Texto extraído da (primeira | ultima) página do PDF em letras minúsculas.
        nome (str): Nome do cliente
        consumo e demanda (List[float]): Lista de (consumos | demandas) registrados no histórico
        data (List[str]): Lista de datas registradas no histórico
        medida_demanda e medida_consumo (str): Descricao da unidade de medida da (consumo | demanda)
        ths (str): Descricao da tarifa horosazonal do cliente 
    """
    def __init__(self, path: str) -> None:
        self._caminho = path
        self._pdf, self._primeira_pagina, self._ultima_pagina =  None, None, None
        self._le_fatura()
        self._nome, self._consumo, self._demanda = None, None, None
        self._ths, self._medida_consumo, self._medida_demanda = None, None, None
        self._data = None

    def _le_fatura(self) -> None:
        """
        Lê o arquivo PDF da fatura e extrai informações relevantes e define as variaveis pdf (contem o conteúdo de cada pagina),
        a primeira pagina e a ultima pagina

        Returns:
            Tuple[PyPDF2.PdfReader, str, str]: Uma tupla contendo o objeto PdfReader,
            o texto da primeira página em letras minúsculas e o texto da última página em letras minúsculas.
        """
        with open(self._caminho, "rb") as pdf:
            conteudo = PyPDF2.PdfReader(pdf)
            conteudo = {
                f"pagina_{n}": conteudo.pages[n].extract_text().lower()
                for n in range(len(conteudo.pages))
            }
        self.pdf = conteudo
        self.primeira_pagina = list(conteudo.values())[0]
        self.ultima_pagina = list(conteudo.values())[-1]
    
    @property
    def distribuidora(self) -> None:
        return None
    
    @property
    def consumo(self) -> List[float]:
        return self._consumo

    @consumo.setter
    def consumo(self, consumo: List[float]) -> None:
        self._consumo = consumo
    
    @property
    def data(self) -> List[str]:
        return self._data
    
    @data.setter
    def data(self, datas) -> None:
        self._data = datas

    @property
    def demanda(self) -> List[float]:
        return self._demanda

    @demanda.setter
    def demanda(self, demanda: List[float]) -> None:
        self._demanda = demanda
    
    @property
    def medida_demanda(self) -> str:
        return self._medida_demanda

    @medida_demanda.setter
    def medida_demanda(self, medida_demanda: str) -> None:
        self._medida_demanda = medida_demanda

    @property
    def medida_consumo(self) -> str:
        return self._medida_consumo

    @medida_consumo.setter
    def medida_consumo(self, medida_consumo: str) -> None:
        self._medida_consumo = medida_consumo

    @property
    def nome(self) -> str:
        return self._nome
    
    @nome.setter
    def nome(self, nome: str) -> None:
        self._nome = nome

    @property
    def pdf(self):
        return self._pdf
    
    @pdf.setter
    def pdf(self, pdf: Dict[str, str]):
        self._pdf = pdf

    @property
    def primeira_pagina(self) -> str:
        return self._primeira_pagina
    
    @primeira_pagina.setter
    def primeira_pagina(self, pagina: str) -> None:
        self._primeira_pagina = pagina

    @property
    def ths(self) -> str:
        return self._ths
    
    @ths.setter
    def ths(self, ths: str) -> None:
        self._ths = ths

    @property
    def ultima_pagina(self) -> str:
        return self._ultima_pagina
    
    @ultima_pagina.setter
    def ultima_pagina(self, pagina: str) -> None:
        self._ultima_pagina = pagina

    def transforma_data(self, x: str) -> datetime:
        relacao_mes_val = {
            "jan": "01-01",
            "fev": "01-02",
            "mar": "01-03",
            "abr": "01-04",
            "mai": "01-05",
            "jun": "01-06",
            "jul": "01-07",
            "ago": "01-08",
            "set": "01-09",
            "out": "01-10",
            "nov": "01-11",
            "dez": "01-12",
        }
        try:
            return pd.to_datetime(relacao_mes_val[x[:3]] + "-" + x[-4:], format="%d-%m-%Y")
        except ValueError:
            return pd.to_datetime(relacao_mes_val[x[:3]] + "-" + x[-2:], format="%d-%m-%y")