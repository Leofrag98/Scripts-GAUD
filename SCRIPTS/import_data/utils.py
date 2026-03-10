# /import_data/utils.py

import re

def remove_non_numbers(text: str) -> str:
    """
    Remove todos os caracteres não numéricos de uma string.
    Retorna uma string vazia se a entrada for None.
    """
    if text is None:
        return ""
    return re.sub(r'\D', '', str(text))

def strip(value):
    """
    Aplica o método .strip() de forma segura, apenas se o valor for uma string.
    Retorna o valor original se não for uma string.
    """
    if isinstance(value, str):
        return value.strip()
    return value