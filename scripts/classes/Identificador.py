import os
from typing import Any, Dict, Union
from classes.Fatura import Fatura
from classes import CEMIG, COPEL, CPFL, EDP, ELEKTRO, ENEL

class Identificador:
    """
    Lê o PDF e identifica o nome da Distribuidora
    
    Args:
        - infos (Dict): Dicionario com informacao de nome do arquivo e caminho para o pdf

    Returns:
        - str | None: Nome da Distribuidora, ou None se não identificado
    """
    def __init__(self, infos: Dict[str, str]) -> None:
        caminho = list(infos.values())[0]
        if os.path.getsize(caminho) < 3000000:
            self.fatura = Fatura(caminho).primeira_pagina.lower()
        else:
            self.fatura = ""

    def get_distribuidora(self) -> Union[str,None]:
        _dict = self.get_textos_a_encontrar()
        try:
            texto = list(filter(lambda x: x in self.fatura, _dict.keys()))
            assert len(texto) == 1, f"textos encontrados: {texto}"
            return _dict[texto[0]]
        
        except AssertionError:
            return None

    def get_textos_a_encontrar(self) -> Dict[str, str]:
        """
        Configura e relaciona os textos e distribuidoras para pesquisar
        """
        textos = ["eletropaulo metropolitana eletricidade de são paulo s.a", "elektro redes s.a.", "cpflempresas", "copel distribuição s.a", "fale com cemig", "edp são paulo distribuição de energia s.a."]
        distribuidoras = ["enel sp", "elektro", "cpfl", "copel", "cemig", "edp"]
        return dict(zip(textos, distribuidoras))
    
    def main(self) -> Union[CEMIG.CEMIG, COPEL.COPEL, CPFL.CPFL, EDP.EDP, ENEL.ENEL, ELEKTRO.ELEKTRO, None]:
        distr = self.get_distribuidora()
        if distr == "cemig":
            return CEMIG.CEMIG
        elif distr == "copel":
            return COPEL.COPEL
        elif distr == "cpfl":
            return CPFL.CPFL
        elif distr == "edp":
            return EDP.EDP
        elif distr == "elektro":
            return ELEKTRO.ELEKTRO
        elif distr == "enel sp":
            return ENEL.ENEL