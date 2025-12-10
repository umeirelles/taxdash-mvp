"""
SPED file loaders using the sped-parser library.

This module provides functions to load and process Brazilian SPED files:
- EFD Contribuições (PIS/COFINS)
- EFD Fiscal (ICMS/IPI)
- ECD (Digital Accounting)

The loaders use the sped-parser library for robust file parsing and add custom logic for:
- Multi-file support with cross-file unique IDs
- Parent-child relationship tracking
- Metadata extraction and forward-filling
"""

import tempfile
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd
import streamlit as st
from sped_parser import EFDContribuicoesParser, EFDFiscalParser, ECDParser

from . import config
from .logger import get_logger

logger = get_logger(__name__)


def _save_uploaded_file_temporarily(uploaded_file) -> Path:
    """Save an uploaded Streamlit file to a temporary location for parsing."""
    uploaded_file.seek(0)
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.txt') as tmp:
        tmp.write(uploaded_file.read())
        return Path(tmp.name)


def _build_composite_ids(
    df: pd.DataFrame,
    file_index: int,
    periodo: Optional[str],
    cnpj: Optional[str]
) -> pd.Series:
    """Build cross-file unique IDs: <file_idx>|<period>|<cnpj>|<row_no>"""
    prefix_file = str(file_index)
    prefix_periodo = str(periodo) if periodo is not None else "NA"
    prefix_cnpj = str(cnpj) if cnpj is not None else "NA"
    row_no = pd.Series(df.index, index=df.index).astype(str).str.zfill(7)
    composite_id = (prefix_file + "|" + prefix_periodo + "|" + prefix_cnpj + "|" + row_no).astype("string")
    return composite_id


def _establish_parent_child_relationships(
    df: pd.DataFrame,
    parent_reg_codes: List[str]
) -> pd.DataFrame:
    """Establish id_pai (parent ID) relationships based on register codes (column '1')."""
    if '1' in df.columns and 'id' in df.columns and 'id_pai' in df.columns:
        df.loc[df['1'].isin(parent_reg_codes), 'id_pai'] = df['id']
        df['id_pai'] = df['id_pai'].ffill()
    return df


@st.cache_data
def load_and_process_data(uploaded_file) -> pd.DataFrame:
    """
    Load one or more SPED Contribuições files and build stable, cross-file unique IDs.

    Args:
        uploaded_file: Single file or list of uploaded Streamlit files

    Returns:
        DataFrame with columns: id, id_pai, periodo, cnpj, '0', '1', '2', ...
    """
    if uploaded_file is None or (isinstance(uploaded_file, list) and len(uploaded_file) == 0):
        st.error("Por favor, carregue pelo menos um arquivo .txt")
        st.stop()

    files = uploaded_file if isinstance(uploaded_file, list) else [uploaded_file]
    dfs = []

    logger.info(f"Starting SPED Contribuições load: {len(files)} file(s)")

    for i, f in enumerate(files):
        tmp_path = _save_uploaded_file_temporarily(f)
        logger.debug(f"Processing file {i+1}/{len(files)}: {getattr(f, 'name', 'unknown')}")

        try:
            # Parse using sped-parser library
            parser = EFDContribuicoesParser()
            data = parser.parse_file(str(tmp_path))
            logger.debug(f"File {i+1} parsed successfully with sped-parser")

            # Get raw DataFrame - already has 'id', 'id_pai', '0', '1', '2', ... columns
            df_temp = data.raw_dataframe.copy()

            # Clean up temporary file
            tmp_path.unlink()

            if df_temp.empty or len(df_temp) == 0:
                logger.warning(f"File {i+1} is empty or has no valid data")
                continue

            # Remove sped-parser's internal 'id' and 'id_pai' columns - we'll create our own
            if 'id' in df_temp.columns:
                df_temp.drop(columns=['id'], inplace=True)
            if 'id_pai' in df_temp.columns:
                df_temp.drop(columns=['id_pai'], inplace=True)

            # Extract metadata from header
            # sped-parser provides: period_start, period_end, cnpj
            periodo = f"{data.header.period_start:%Y%m}" if hasattr(data.header, 'period_start') and data.header.period_start else None
            cnpj = data.header.cnpj if hasattr(data.header, 'cnpj') else None

            # Also can get from first row (register 0000) as fallback
            # Note: Before rename, register types are in column '0' (not '1')
            if periodo is None and '0' in df_temp.columns and '7' in df_temp.columns:
                header_row = df_temp[df_temp['0'] == '0000']
                if not header_row.empty:
                    periodo_raw = header_row.iloc[0]['7']
                    if pd.notna(periodo_raw) and len(str(periodo_raw)) >= 8:
                        periodo = str(periodo_raw)[2:]  # Remove initial characters

            if cnpj is None and '0' in df_temp.columns and '9' in df_temp.columns:
                header_row = df_temp[df_temp['0'] == '0000']
                if not header_row.empty:
                    cnpj = header_row.iloc[0]['9']

            # Shift numeric column names by +1 to match processor expectations
            # sped-parser puts register types in '0', but processors expect them in '1'
            numeric_cols = [c for c in df_temp.columns if c.isdigit()]
            rename_map = {c: str(int(c) + 1) for c in numeric_cols}
            df_temp.rename(columns=rename_map, inplace=True)

            # Build our custom composite IDs
            composite_id = _build_composite_ids(df_temp, i, periodo, cnpj)

            # Add metadata columns at the beginning
            df_temp.insert(0, 'cnpj', cnpj)
            df_temp.insert(0, 'periodo', periodo)
            df_temp.insert(0, 'id_pai', None)
            df_temp.insert(0, 'id', composite_id)

            # Forward-fill CNPJ for specific registers
            if '1' in df_temp.columns:
                df_temp.loc[df_temp['1'].isin(["0000", "C001", "D001", "M001", "1001"]), 'cnpj'] = cnpj
                # For register 0140, use column '4'
                if '4' in df_temp.columns:
                    df_temp.loc[df_temp['1'] == "0140", 'cnpj'] = df_temp['4']
                # For registers A010, C010, D010, F010, I010, P010, use column '2'
                if '2' in df_temp.columns:
                    df_temp.loc[df_temp['1'].isin(["A010", "C010", "D010", "F010", "I010", "P010"]), 'cnpj'] = df_temp['2']

            # Establish parent-child relationships
            df_temp = _establish_parent_child_relationships(df_temp, config.PARENT_REG_CONTRIB)

            # Forward-fill cnpj
            df_temp['cnpj'] = df_temp['cnpj'].ffill()

            logger.info(f"File {i+1} processed successfully: {len(df_temp)} rows, CNPJ={cnpj}, Periodo={periodo}")
            dfs.append(df_temp)

        except Exception as e:
            logger.error(f"Error processing file {i+1}: {str(e)}", exc_info=True)
            st.warning(f"Erro ao processar arquivo {i}: {str(e)}")
            # Clean up temporary file on error
            if tmp_path.exists():
                tmp_path.unlink()
            continue

    if not dfs:
        logger.error("No valid SPED Contribuições files could be loaded")
        st.error("Falha ao ler o SPED Contribuições: nenhum arquivo válido ou todas as linhas foram descartadas como inválidas.")
        st.stop()

    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"SPED Contribuições load complete: {len(df)} total rows from {len(dfs)} file(s)")
    return df


@st.cache_data
def load_and_process_sped_fiscal(uploaded_files) -> pd.DataFrame:
    """
    Load one or more SPED Fiscal files and build stable, cross-file unique IDs.

    Args:
        uploaded_files: List of uploaded Streamlit files

    Returns:
        DataFrame with columns: id, id_pai, periodo, cnpj_estab, ie_estab, uf_estab, '1', '2', ...
    """
    if not uploaded_files:
        st.error("Por favor, carregue no mínimo um arquivo .txt")
        st.stop()

    dfs = []

    for i, single_file in enumerate(uploaded_files):
        tmp_path = _save_uploaded_file_temporarily(single_file)

        try:
            # Parse using sped-parser library
            parser = EFDFiscalParser()
            data = parser.parse_file(str(tmp_path))

            # Get raw DataFrame
            df_temp = data.raw_dataframe.copy()

            # Clean up temporary file
            tmp_path.unlink()

            if df_temp.empty or len(df_temp) == 0:
                continue

            # Remove sped-parser's internal 'id' and 'id_pai' columns
            if 'id' in df_temp.columns:
                df_temp.drop(columns=['id'], inplace=True)
            if 'id_pai' in df_temp.columns:
                df_temp.drop(columns=['id_pai'], inplace=True)

            # Extract metadata from header
            periodo = f"{data.header.period_start:%Y%m}" if hasattr(data.header, 'period_start') and data.header.period_start else None
            cnpj_estab = data.header.cnpj if hasattr(data.header, 'cnpj') else None
            uf_estab = data.header.uf if hasattr(data.header, 'uf') else None

            # For state registration, check if available in header
            ie_estab = None
            # Fallback to first row (register 0000)
            # Note: Before rename, register types are in column '0' (not '1')
            if '0' in df_temp.columns:
                header_row = df_temp[df_temp['0'] == '0000']
                if not header_row.empty:
                    if periodo is None and '4' in df_temp.columns:
                        periodo_raw = header_row.iloc[0]['4']
                        if pd.notna(periodo_raw) and len(str(periodo_raw)) >= 8:
                            periodo = str(periodo_raw)[2:]
                    if cnpj_estab is None and '7' in df_temp.columns:
                        cnpj_estab = header_row.iloc[0]['7']
                    if ie_estab is None and '10' in df_temp.columns:
                        ie_estab = header_row.iloc[0]['10']
                    if uf_estab is None and '9' in df_temp.columns:
                        uf_estab = header_row.iloc[0]['9']

            # Shift numeric column names by +1 to match processor expectations
            # sped-parser puts register types in '0', but processors expect them in '1'
            numeric_cols = [c for c in df_temp.columns if c.isdigit()]
            rename_map = {c: str(int(c) + 1) for c in numeric_cols}
            df_temp.rename(columns=rename_map, inplace=True)

            # Build composite IDs
            composite_id = _build_composite_ids(df_temp, i, periodo, cnpj_estab)

            # Add metadata columns
            df_temp.insert(0, 'uf_estab', uf_estab)
            df_temp.insert(0, 'ie_estab', ie_estab)
            df_temp.insert(0, 'cnpj_estab', cnpj_estab)
            df_temp.insert(0, 'periodo', periodo)
            df_temp.insert(0, 'id_pai', None)
            df_temp.insert(0, 'id', composite_id)

            # Establish parent-child relationships
            df_temp = _establish_parent_child_relationships(df_temp, config.PARENT_REG_FISCAL)

            # Forward-fill cnpj_estab
            df_temp['cnpj_estab'] = df_temp['cnpj_estab'].ffill()

            dfs.append(df_temp)

        except Exception as e:
            st.warning(f"Erro ao processar arquivo SPED Fiscal {i}: {str(e)}")
            if tmp_path.exists():
                tmp_path.unlink()
            continue

    if not dfs:
        st.error("Falha ao ler o SPED Fiscal: nenhum arquivo válido.")
        st.stop()

    df_sped_fiscal = pd.concat(dfs, ignore_index=True)

    # Remove duplicates by ID
    if "id" in df_sped_fiscal.columns:
        df_sped_fiscal = df_sped_fiscal.drop_duplicates(subset=["id"], keep="last")

    return df_sped_fiscal


def _process_single_ecd_file(uploaded_file, file_index: int) -> pd.DataFrame:
    """Process a single ECD file using sped-parser."""
    tmp_path = _save_uploaded_file_temporarily(uploaded_file)

    try:
        # Parse using sped-parser library
        parser = ECDParser()
        data = parser.parse_file(str(tmp_path))

        # Get raw DataFrame
        df = data.raw_dataframe.copy()

        # Clean up temporary file
        tmp_path.unlink()

        if df.empty or len(df) == 0:
            return pd.DataFrame(columns=['id', 'id_pai', 'ano', 'cnpj', '1'])

        # Remove sped-parser's internal 'id' and 'id_pai' columns
        if 'id' in df.columns:
            df.drop(columns=['id'], inplace=True)
        if 'id_pai' in df.columns:
            df.drop(columns=['id_pai'], inplace=True)

        # Extract metadata - year from period_start
        ano_ecd = str(data.header.period_start.year) if hasattr(data.header, 'period_start') and data.header.period_start else None
        cnpj = data.header.cnpj if hasattr(data.header, 'cnpj') else None

        # Fallback to first row
        # Note: Before rename, register types are in column '0' (not '1')
        if '0' in df.columns:
            header_row = df[df['0'] == '0000']
            if not header_row.empty:
                if ano_ecd is None and '3' in df.columns:
                    ano_raw = header_row.iloc[0]['3']
                    if pd.notna(ano_raw) and len(str(ano_raw)) >= 8:
                        ano_ecd = str(ano_raw)[4:]
                if cnpj is None and '6' in df.columns:
                    cnpj = header_row.iloc[0]['6']

        # Shift numeric column names by +1 to match processor expectations
        # sped-parser puts register types in '0', but processors expect them in '1'
        numeric_cols = [c for c in df.columns if c.isdigit()]
        rename_map = {c: str(int(c) + 1) for c in numeric_cols}
        df.rename(columns=rename_map, inplace=True)

        # Build composite IDs
        composite_id = _build_composite_ids(df, file_index, ano_ecd, cnpj)

        # Add metadata columns
        df.insert(0, 'cnpj', cnpj)
        df.insert(0, 'ano', ano_ecd)
        df.insert(0, 'id_pai', None)
        df.insert(0, 'id', composite_id)

        # Establish parent-child relationships
        df = _establish_parent_child_relationships(df, config.PARENT_REG_ECD)

        return df

    except Exception as e:
        st.warning(f"Erro ao processar arquivo ECD: {str(e)}")
        if tmp_path.exists():
            tmp_path.unlink()
        return pd.DataFrame(columns=['id', 'id_pai', 'ano', 'cnpj', '1'])


@st.cache_data
def load_and_process_ecd(uploaded_files) -> pd.DataFrame:
    """
    Load one or more ECD files and build stable, cross-file unique IDs.

    Args:
        uploaded_files: Single file or list of uploaded Streamlit files

    Returns:
        DataFrame with columns: id, id_pai, ano, cnpj, '1', '2', ...
    """
    if uploaded_files is None or (isinstance(uploaded_files, list) and len(uploaded_files) == 0):
        st.error("Por favor, carregue pelo menos um arquivo .txt")
        st.stop()

    files = uploaded_files if isinstance(uploaded_files, list) else [uploaded_files]
    files = [f for f in files if f is not None]

    if not files:
        st.error("Por favor, carregue pelo menos um arquivo .txt válido")
        st.stop()

    dfs = []
    for idx, single_file in enumerate(files):
        df_single = _process_single_ecd_file(single_file, idx)
        if not df_single.empty:
            dfs.append(df_single)

    if not dfs:
        st.error("Falha ao ler a ECD: nenhum arquivo válido ou todas as linhas foram descartadas como inválidas.")
        st.stop()

    df_ecd = pd.concat(dfs, ignore_index=True)
    return df_ecd
