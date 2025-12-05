import pandas as pd

from dicts import cod_uf, cfop_cod_descr, cst_icms, cst_pis_cofins


CFOP_SERIES = pd.Series(cfop_cod_descr, dtype="string")
CST_ICMS_SERIES = pd.Series(cst_icms, dtype="string")
CST_PIS_COFINS_SERIES = pd.Series(cst_pis_cofins, dtype="string")
UF_SERIES = pd.Series(cod_uf, dtype="string")


def map_cfop(series: pd.Series) -> pd.Series:
    """Map CFOP codes to their descriptions."""
    return series.map(CFOP_SERIES)


def map_cst_icms(series: pd.Series) -> pd.Series:
    """Map CST ICMS codes to descriptions."""
    return series.map(CST_ICMS_SERIES)


def map_cst_pis_cofins(series: pd.Series) -> pd.Series:
    """Map CST PIS/COFINS codes to descriptions."""
    return series.map(CST_PIS_COFINS_SERIES)


def map_uf(series: pd.Series) -> pd.Series:
    """Map UF numeric codes to state abbreviations."""
    return series.map(UF_SERIES)
