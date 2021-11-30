import pandas as pd

def format_cnpj(cnpj: pd.Series) -> pd.Series:
    """Formata uma série de CNPJs como string com 14 dígitos.
    
    :param cnpj:pd.Series: A série do pandas a ser formatada.
    :returns new_cnpj:pd.Series: A série de CNPJs formatada.
    """
    new_cnpj = cnpj.astype('str')
    new_cnpj = new_cnpj.str.zfill(14)

    return new_cnpj
