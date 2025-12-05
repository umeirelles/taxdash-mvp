import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from dicts import *
import csv
import io

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('future.no_silent_downcasting', True)

# Configure Streamlit page
st.set_page_config(page_title="TaxDash", page_icon=":material/code:", layout="wide")


# --------------------------------------------------------------------------------------------------------------------
# Core Data Loading & Processing
# --------------------------------------------------------------------------------------------------------------------


@st.cache_data
def load_and_process_data(uploaded_file):
    """Load one or more SPED Contribuições files and build stable, cross-file unique IDs.
    Backwards-compatible: accepts a single file or a list of files.
    """
    # Validate input
    if uploaded_file is None or (isinstance(uploaded_file, list) and len(uploaded_file) == 0):
        st.error("Por favor, carregue pelo menos um arquivo .txt")
        st.stop()

    DELIMITER = '|'
    ENCODING = 'latin-1'
    COLUMN_NAMES = [str(i) for i in range(40)]
    PARENT_REG_CODES = [
        "0000", "0140", "A100", "C100", "C180", "C190", "C380", "C400", "C500", "C600", "C800", "D100", "D500",
        "F100", "F120", "F130", "F150", "F200", "F500", "F600", "F700", "F800", "I100", "M100", "M200",
        "M300", "M350", "M400", "M500", "M600", "M700", "M800", "P100", "P200", "1010", "1020", "1050",
        "1100", "1200", "1300", "1500", "1600", "1700", "1800", "1900"
    ]

    # Normalize to a list for multi-file support
    files = uploaded_file if isinstance(uploaded_file, list) else [uploaded_file]
    dfs = []

    for i, f in enumerate(files):
        # Robust, memory-safe read for SPED Contribuições
        f.seek(0)
        reader = pd.read_csv(
            f,
            header=None,
            delimiter=DELIMITER,
            names=COLUMN_NAMES,
            encoding=ENCODING,
            dtype=str,
            engine="python",
            on_bad_lines="skip",
            chunksize=200_000,
            quoting=csv.QUOTE_NONE
        )
        parts = []
        for chunk in reader:
            if '1' in chunk.columns:
                mask_end = chunk['1'].astype(str).eq('9999')
                if mask_end.any():
                    first_idx = int(np.argmax(mask_end.to_numpy()))
                    parts.append(chunk.iloc[:first_idx+1])
                    break
            parts.append(chunk)
        df_temp = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=COLUMN_NAMES)
        # Post-trim safety: ensure we only keep up to and including first 9999 row
        if not df_temp.empty and '1' in df_temp.columns:
            mask_all = df_temp['1'].astype(str).eq('9999')
            if mask_all.any():
                cut = int(np.argmax(mask_all.to_numpy()))
                df_temp = df_temp.iloc[:cut+1].copy()
        if df_temp.empty:
            # Skip empty/invalid files quietly
            continue

        # Extract period and CNPJ for this file (guard on the same columns we read)
        data_efd = df_temp.loc[0, '7'][2:] if pd.notna(df_temp.loc[0, '7']) else None
        cnpj_header = df_temp.loc[0, '9'] if pd.notna(df_temp.loc[0, '9']) else None

        # Drop unnecessary column if present
        if '0' in df_temp.columns:
            df_temp.drop(columns=['0'], inplace=True)

        # Build deterministic, cross-file row id
        # id := "<fileidx>|<periodo>|<cnpj>|<row_no_zfilled>"
        prefix_file = str(i)
        prefix_periodo = str(data_efd) if data_efd is not None else "NA"
        prefix_cnpj    = str(cnpj_header) if cnpj_header is not None else "NA"
        row_no = pd.Series(df_temp.index, index=df_temp.index).astype(str).str.zfill(7)
        composite_id = (prefix_file + "|" + prefix_periodo + "|" + prefix_cnpj + "|" + row_no).astype("string")

        # Insert new columns at the front, preserving existing downstream expectations
        df_temp.insert(0, 'cnpj', None)
        df_temp.insert(0, 'periodo', data_efd)
        df_temp.insert(0, 'id_pai', None)
        df_temp.insert(0, 'id', composite_id)

        # Assign CNPJ values using known record rules, then forward fill within this file only
        df_temp.loc[df_temp['1'].isin(["0000", "C001", "D001", "M001", "1001"]), 'cnpj'] = cnpj_header
        df_temp.loc[df_temp['1'] == "0140", 'cnpj'] = df_temp['4']
        df_temp.loc[df_temp['1'].isin(["A010", "C010", "D010", "F010", "I010", "P010"]), 'cnpj'] = df_temp['2']

        # Parent IDs → id_pai = id on parent records, then forward-fill
        df_temp.loc[df_temp['1'].isin(PARENT_REG_CODES), 'id_pai'] = df_temp['id']
        df_temp['id_pai'] = df_temp['id_pai'].ffill()
        df_temp['cnpj'] = df_temp['cnpj'].ffill()

        dfs.append(df_temp)

    # Concatenate all files
    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=COLUMN_NAMES + ['id', 'id_pai', 'periodo', 'cnpj'])
    if df.empty:
        st.error("Falha ao ler o SPED Contribuições: nenhum arquivo válido ou todas as linhas foram descartadas como inválidas.")
        st.stop()

    return df


# SPED-FISCAL
@st.cache_data
def load_and_process_sped_fiscal(uploaded_files):
    if not uploaded_files:
        st.error("Por favor, carregue no mínimo um arquivo .txt")
        st.stop()

    DELIMITER = '|'
    ENCODING = 'latin-1'
    COLUMN_NAMES = [str(i) for i in range(42)]
    PARENT_REG_CODES = [
        "0000",
        "C100", "C300", "C350", "C400", "C495", "C500", "C600", "C700", "C800", "C860",
        "D100", "D300", "D350", "D400", "D500", "D600", "D695", "D700", "D750",
        "E100", "E200", "E300", "E500",
        "G110",
        "H005",
        "K100", "K200", "K210", "K220", "K230", "K250", "K260", "K270", "K280", "K290", "K300",
        "1100", "1200", "1300", "1350", "1390", "1400", "1500", "1600", "1601", "1700", "1800", "1900", "1960", "1970", "1980"
    ]

    # Collect each SPED Fiscal file into a list, then concatenate
    dfs = []
    for i, single_file in enumerate(uploaded_files):
        # Robust read with fallback: try fast C engine; on parser error, fall back to Python engine with chunking
        try:
            single_file.seek(0)
            df_temp = pd.read_csv(
                single_file,
                header=None,
                delimiter=DELIMITER,
                names=COLUMN_NAMES,
                low_memory=False,
                encoding=ENCODING,
                dtype=str,
                engine="c",
                on_bad_lines="skip"
            )
            # Trim at SPED Fiscal end marker 9999 (include the 9999 row)
            if not df_temp.empty and '1' in df_temp.columns:
                mask_all = df_temp['1'].astype(str).eq('9999')
                if mask_all.any():
                    cut = int(np.argmax(mask_all.to_numpy()))
                    df_temp = df_temp.iloc[:cut+1].copy()
        except pd.errors.ParserError:
            # Fallback for malformed lines / very long fields
            single_file.seek(0)
            reader = pd.read_csv(
                single_file,
                header=None,
                delimiter=DELIMITER,
                names=COLUMN_NAMES,
                encoding=ENCODING,
                dtype=str,
                engine="python",
                on_bad_lines="skip",
                chunksize=200_000,
                quoting=csv.QUOTE_NONE
            )
            parts = []
            for chunk in reader:
                if '1' in chunk.columns:
                    mask_end = chunk['1'].astype(str).eq('9999')
                    if mask_end.any():
                        first_idx = int(np.argmax(mask_end.to_numpy()))
                        parts.append(chunk.iloc[:first_idx+1])
                        break
                parts.append(chunk)
            df_temp = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=COLUMN_NAMES)
        if df_temp.empty:
            # Skip empty/invalid files quietly
            continue

        # Extract period and registration identifiers for this file (guard on the same columns we read)
        data_efd = df_temp.loc[0, '4'][2:] if pd.notna(df_temp.loc[0, '4']) else None
        cnpj_estab = df_temp.loc[0, '7'] if pd.notna(df_temp.loc[0, '7']) else None
        ie_estab = df_temp.loc[0, '10'] if pd.notna(df_temp.loc[0, '10']) else None
        uf_estab = df_temp.loc[0, '9'] if pd.notna(df_temp.loc[0, '9']) else None

        # Drop the '0' column if it exists
        if '0' in df_temp.columns:
            df_temp.drop(columns=['0'], inplace=True)

        # ----- Build a deterministic, cross-file row id
        # id := "<fileidx>|<periodo>|<cnpj_estab>|<row_no_zfilled>"
        prefix_file = str(i)
        prefix_periodo = str(data_efd) if data_efd is not None else "NA"
        prefix_cnpj    = str(cnpj_estab) if cnpj_estab is not None else "NA"
        row_no = pd.Series(df_temp.index, index=df_temp.index).astype(str).str.zfill(7)
        composite_id = (prefix_file + "|" + prefix_periodo + "|" + prefix_cnpj + "|" + row_no).astype("string")

        # Insert the new columns (keep prior order expectations)
        df_temp.insert(0, 'uf_estab', uf_estab)
        df_temp.insert(0, 'ie_estab', ie_estab)
        df_temp.insert(0, 'cnpj_estab', cnpj_estab)
        df_temp.insert(0, 'periodo', data_efd)
        df_temp.insert(0, 'id_pai', None)
        df_temp.insert(0, 'id', composite_id)

        # Assign parent IDs using the composite id and forward-fill within the file
        df_temp.loc[df_temp['1'].isin(PARENT_REG_CODES), 'id_pai'] = df_temp['id']
        df_temp['id_pai'] = df_temp['id_pai'].ffill()
        df_temp['cnpj_estab'] = df_temp['cnpj_estab'].ffill()

        # Append this file’s DataFrame to the list
        dfs.append(df_temp)

    # Concatenate all files into a single DataFrame
    df_sped_fiscal = pd.concat(dfs, ignore_index=True)

    # In case the same file is imported again, drop duplicate composite IDs
    if "id" in df_sped_fiscal.columns:
        df_sped_fiscal = df_sped_fiscal.drop_duplicates(subset=["id"], keep="last")

    return df_sped_fiscal


# ECD
@st.cache_data
def load_and_process_ecd(uploaded_file):
    # If no file is provided, inform the user and stop execution
    if uploaded_file is None:
        st.error("Por favor, carregue um arquivo .txt")
        st.stop()

    DELIMITER = '|'
    ENCODING = 'latin-1'
    COLUMN_NAMES = [str(i) for i in range(40)]
    PARENT_REG_CODES = [
        "0000", "0001", "C001", "C040", "C050", "C150", "C600", "I001", "I010", "I050", "I150"
    ]

    # Robust reader: try fast C engine; on parser error, fall back to Python engine (tolerant to odd quotes/cert bytes)
    try:
        uploaded_file.seek(0)
        reader = pd.read_csv(
            uploaded_file,
            header=None,
            delimiter=DELIMITER,
            names=COLUMN_NAMES,
            encoding=ENCODING,
            dtype=str,
            engine="c",
            on_bad_lines="skip",
            chunksize=200_000
        )
    except Exception:
        uploaded_file.seek(0)
        reader = pd.read_csv(
            uploaded_file,
            header=None,
            delimiter=DELIMITER,
            names=COLUMN_NAMES,
            encoding=ENCODING,
            dtype=str,
            engine="python",
            on_bad_lines="skip",
            chunksize=200_000,
            quoting=csv.QUOTE_NONE
        )

    dfs = []
    in_skip = False       # True after first I200, until first I350
    after_resume = False  # True after first I350 (from that row onwards)

    try:
        for chunk in reader:
            codes = chunk['1'].astype(str)
            mask_i200 = codes.eq('I200')
            mask_i350 = codes.eq('I350')

            if not in_skip and not after_resume:
                has200 = mask_i200.any()
                has350 = mask_i350.any()

                if not has200 and not has350:
                    # Still before any I200; keep everything
                    dfs.append(chunk)
                    continue

                if has200 and not has350:
                    # First I200 happens in this chunk: keep rows before it, start skipping
                    first200 = int(np.argmax(mask_i200.to_numpy()))
                    if first200 > 0:
                        dfs.append(chunk.iloc[:first200])
                    in_skip = True
                    continue

                if not has200 and has350:
                    # No I200 yet but found I350 (rare) → include all; from now on include all next chunks
                    dfs.append(chunk)
                    after_resume = True
                    continue

                # Both I200 and I350 in same chunk
                first200 = int(np.argmax(mask_i200.to_numpy()))
                first350 = int(np.argmax(mask_i350.to_numpy()))
                if first200 < first350:
                    # keep rows before I200 AND rows from I350 onward; skip in-between just in this chunk
                    if first200 > 0:
                        dfs.append(chunk.iloc[:first200])
                    dfs.append(chunk.iloc[first350:])
                    after_resume = True   # resume permanently after first I350
                    in_skip = False
                else:
                    # I350 appears before I200 (unexpected ordering) → include full chunk and mark resumed
                    dfs.append(chunk)
                    after_resume = True
                    in_skip = False
                continue

            if in_skip and not after_resume:
                # We are skipping until we hit I350
                if mask_i350.any():
                    first350 = int(np.argmax(mask_i350.to_numpy()))
                    dfs.append(chunk.iloc[first350:])  # include from I350 onward
                    in_skip = False
                    after_resume = True
                else:
                    # Skip entire chunk
                    continue
                continue

            if after_resume:
                # After first I350: include everything
                dfs.append(chunk)
                continue
    except Exception:
        # Fallback: pre-trim raw text up to the first |I990| and parse in memory with the Python engine
        uploaded_file.seek(0)
        raw = uploaded_file.read()
        text = raw.decode(ENCODING, errors='ignore') if isinstance(raw, (bytes, bytearray)) else str(raw)

        trimmed_lines = []
        for ln in text.splitlines():
            trimmed_lines.append(ln)
            if ln.startswith('|I990|'):
                break
        buf = io.StringIO('\n'.join(trimmed_lines))

        df_fallback = pd.read_csv(
            buf,
            header=None,
            delimiter=DELIMITER,
            names=COLUMN_NAMES,
            encoding=ENCODING,
            dtype=str,
            engine='python',
            on_bad_lines='skip',
            quoting=csv.QUOTE_NONE
        )

        # Apply the I200..I350 windowing to the single DataFrame
        if not df_fallback.empty and '1' in df_fallback.columns:
            codes = df_fallback['1'].astype(str)
            has200 = codes.eq('I200').any()
            has350 = codes.eq('I350').any()
            if has200 and not has350:
                first200 = int(np.argmax(codes.eq('I200').to_numpy()))
                if first200 > 0:
                    df_fallback = df_fallback.iloc[:first200].copy()
            elif has200 and has350:
                first200 = int(np.argmax(codes.eq('I200').to_numpy()))
                first350 = int(np.argmax(codes.eq('I350').to_numpy()))
                if first200 < first350:
                    df_fallback = pd.concat([
                        df_fallback.iloc[:first200],
                        df_fallback.iloc[first350:]
                    ], ignore_index=True)
        dfs = [df_fallback]

    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=COLUMN_NAMES)

    # Trim at ECD end marker I990 (include the I990 row)
    if not df.empty and '1' in df.columns:
        mask_all = df['1'].astype(str).eq('I990')
        if mask_all.any():
            cut = int(np.argmax(mask_all.to_numpy()))
            df = df.iloc[:cut+1].copy()

    # Extract period and CNPJ
    ano_ecd = df.loc[0, '3'][4:] if pd.notna(df.loc[0, '3']) else None
    cnpj = df.loc[0, '6'] if pd.notna(df.loc[0, '6']) else None

    # Drop unnecessary column
    if '0' in df.columns:
        df.drop(columns=['0'], inplace=True)

    # Insert new columns
    df.insert(0, 'cnpj', cnpj)
    df.insert(0, 'ano', ano_ecd)
    df.insert(0, 'id_pai', None)
    df.insert(0, 'id', df.index.astype(str))

    # Assign parent IDs
    df.loc[df['1'].isin(PARENT_REG_CODES), 'id_pai'] = df['id']

    # Forward fill missing CNPJ and id_pai
    df['id_pai'] = df['id_pai'].ffill()
    #df['cnpj'] = df['cnpj'].ffill()

    return df


# layout dos datadrames
def style_df(df):
    return df.style.set_properties(**{'background-color': "#d619b0", 'color': "#d0d619"})


# --------------------------------------------------------------------------------------------------------------------
# FUNÇOES PARA FILTRAR REGISTROS ORIGINAIS SPED FISCAL E CONTRIBUIÇOES
# --------------------------------------------------------------------------------------------------------------------

# filtro blocos SPED CONTRIBUICOES

def Bloco_0(df):
    ### registro 0140
    reg_0140 = (df[df['1'] == '0140']
            .iloc[:,0:13]
            .sort_values(by='2', ascending=True))
    reg_0140 = reg_0140.drop_duplicates(subset='2')

    ### registro 0150
    reg_0150 = (df[df['1'] == '0150']
            .iloc[:,0:17]
            .sort_values(by='5', ascending=False))
    reg_0150 = reg_0150.drop_duplicates(subset='2')

    ### registro 0200
    reg_0200 = (df[df['1'] == '0200']
            .iloc[:,0:16]
            .sort_values(by='8', ascending=False))
    reg_0200 = reg_0200.drop_duplicates(subset='2') 

    return reg_0140, reg_0150, reg_0200


def Bloco_M(df):

    M100 = (df[df['1'] == 'M100']
            .iloc[:,0:19]
            .replace(',', '.', regex=True))
    M100[['4', '5', '6', '8', '9', '10', '11', '12', '14', '15']] = M100[['4', '5', '6', '8', '9', '10', '11', '12', '14', '15']].apply(pd.to_numeric, errors='coerce')

    M105 = (df[df['1'] == 'M105']
        .iloc[:,0:14]
        .replace(',', '.', regex=True))
    M105[['4', '5', '6', '7', '8', '9']] = M105[['4', '5', '6', '7', '8', '9']].apply(pd.to_numeric, errors='coerce')
    M105.insert(6, 'NAT_BC_CRED', M105['2'].map(tab_4_3_7))      # incluido a descrição do NAT_BC_CRED (tabela 4.3.7)

    # Registro M110: Ajustes do Crédito de PIS/Pasep Apurado
    M110 = (df[df['1'] == 'M110']
        .iloc[:,0:11]
        .replace(',', '.', regex=True))
    M110['3'] = M110['3'].apply(pd.to_numeric, errors='coerce')
    M110.insert(8, 'tipo_ajuste', M110['4'].map(tab_4_3_8))      # incluido a descrição do COD_AJUSTE (tabela 4.3.8)

    M210 = (df[df['1'] == 'M210']
            .iloc[:,:20]
            .replace(',', '.', regex=True))
    M210[['3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16']] = M210[['3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16']].apply(pd.to_numeric, errors='coerce')
    M210.insert(6, 'cod_contribuicao', M210['2'].map(tab_4_3_5))      # incluido a descrição do cod_contribuicao (tabela 4.3.5)

    # Registro M510: Ajustes do Crédito de COFINS Apurado
    M510 = (df[df['1'] == 'M510']
        .iloc[:,0:11]
        .replace(',', '.', regex=True))
    M510['3'] = M510['3'].apply(pd.to_numeric, errors='coerce')
    M510.insert(8, 'tipo_ajuste', M510['4'].map(tab_4_3_8))      # incluido a descrição do COD_AJUSTE (tabela 4.3.8)

    M610 = (df[df['1'] == 'M610']
            .iloc[:,:20]
            .replace(',', '.', regex=True))
    M610[['3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16']] = M610[['3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16']].apply(pd.to_numeric, errors='coerce')
    M610.insert(6, 'cod_contribuicao', M610['2'].map(tab_4_3_5))      # incluido a descrição do cod_contribuicao (tabela 4.3.5)

    M400 = (df[df['1'] == 'M400']
            .iloc[:,:9]
            .replace(',', '.', regex=True))
    M400['3'] = M400['3'].apply(pd.to_numeric, errors='coerce')
    M400.insert(6, 'cst_descr', M400['2'].map(cst_pis_cofins))      # incluido a descrição do CST

    return M100, M105, M110, M210, M400, M510, M610


def Bloco_A(df, reg_0140, reg_0150, reg_0200):

    ###  A100 
    A100 = (df[df['1'] == 'A100']
            .iloc[:,0:25]
            .replace(',', '.', regex=True))
    A100[['12', '14', '15', '16', '17', '18', '19', '20', '21']] = A100[['12', '14', '15', '16', '17', '18', '19', '20', '21']].apply(pd.to_numeric, errors='coerce')


    ###  A170
    A170 = (df[df['1'] == 'A170']
            .iloc[:,0:22]
            .replace(',', '.', regex=True))
    A170[['5', '6', '10', '11', '12', '14', '15', '16']] = A170[['5', '6', '10', '11', '12', '14', '15', '16']].apply(pd.to_numeric, errors='coerce')
    A170.insert(4, 'ind_emit', A170['id_pai'].map(A100.set_index('id')['3']))       # incluido IND_EMIT (0-propria; 1-terceiros)
    A170.insert(4, 'ind_oper', A170['id_pai'].map(A100.set_index('id')['2']))       # incluido IND_OPER (0-entrada; 1-saida)
    A170.insert(4, 'part_cod', A170['id_pai'].map(A100.set_index('id')['4']))       # incluido COD_PART 
    A170.insert(5, 'part_nome', A170['part_cod'].map(reg_0150.set_index('2')['3']))       # incluido a Descrição do Participante
    A170.insert(5, 'part_cnpj', A170['part_cod'].map(reg_0150.set_index('2')['5']))       # incluido o CNPJ do Participante
    A170.insert(4, 'part_uf_cod', A170['part_cod'].map(reg_0150.set_index('2')['8'].astype(str).str[:2]))       # incluido a UF do Participante
    A170.insert(5, 'part_uf', A170['part_uf_cod'].map(cod_uf))      # incluido a UF do Participante
    A170.insert(14, 'descr_serv_0200', A170['3'].map(reg_0200.drop_duplicates(subset='2').set_index('2')['3']))    # inserindo a descricao do serviço do 0200
    A170.drop('part_uf_cod', axis=1, inplace=True)
    A170.insert(4, 'uf_empresa', A170['cnpj'].map(reg_0140.set_index('4')['5']))       # incluido a UF da Empresa em análise    

    A170.insert(28, 'iss', A170['id_pai'].map(A100.set_index('id')['21']))       # incluido ISS


    return A100, A170


def Bloco_C(df, reg_0140, reg_0150, reg_0200):

    ###  C100
    C100 = (df[df['1'] == 'C100']
            .iloc[:,0:33]
            .replace(',', '.', regex=True))
    C100[['12', '14', '15', '16', '18', '19', '20', '21','22', '23', '24', '26', '27', '28', '29']] = C100[['12', '14', '15', '16', '18', '19', '20', '21','22', '23', '24', '26', '27', '28', '29']].apply(pd.to_numeric, errors='coerce')


    ###  C170
    C170 = (df[df['1'] == 'C170']
            .iloc[:,0:41]
            .replace(',', '.', regex=True))
    C170[['5', '7', '8', '13', '14', '15', '16', '17', '18', '22', '23', '24', '26', '27', '28', '29', '30', '32', '33', '34', '35', '36']] = C170[['5', '7', '8', '13', '14', '15', '16', '17', '18', '22', '23', '24', '26', '27', '28', '29', '30', '32', '33', '34', '35', '36']].apply(pd.to_numeric, errors='coerce')
    C170.insert(15, 'cfop_descr', C170['11'].map(cfop_cod_descr))     # incluido a descrição do CFOP
    C170.insert(7, 'ncm', C170['3'].map(reg_0200.set_index('2')['8']))       # incluido a NCM
    C170.insert(7, 'item_descr', C170['3'].map(reg_0200.set_index('2')['3']))       # incluido a DESCR_ITEM
    C170.insert(4, 'mod_nf', C170['id_pai'].map(C100.set_index('id')['5']))       # incluido o Modelo da NF
    C170.insert(4, 'ind_emit', C170['id_pai'].map(C100.set_index('id')['3']))       # incluido o IND_EMIT (0-propria; 1-terceiros)
    C170.insert(4, 'ind_oper', C170['id_pai'].map(C100.set_index('id')['2']))       # incluido o IND_OPER (0-entrada; 1-saida)
    C170.insert(4, 'part_cod', C170['id_pai'].map(C100.set_index('id')['4']))       # incluido COD_PART 
    C170.insert(5, 'part_nome', C170['part_cod'].map(reg_0150.set_index('2')['3']))       # incluido a Descrição do Participante
    C170.insert(5, 'part_cnpj', C170['part_cod'].map(reg_0150.set_index('2')['5']))       # incluido o CNPJ do Participante
    C170.insert(5, 'part_uf_cod', C170['part_cod'].map(reg_0150.set_index('2')['8'].astype(str).str[:2]))       # incluido a UF do Participante
    C170.insert(5, 'part_ie', C170['part_cod'].map(reg_0150.set_index('2')['7']))       # incluido a IE do Participante
    C170.insert(6, 'part_uf', C170['part_uf_cod'].map(cod_uf)) 
    C170.drop('part_uf_cod', axis=1, inplace=True)
    C170.insert(4, 'uf_empresa', C170['cnpj'].map(reg_0140.set_index('4')['5']))       # incluido a UF da Empresa em análise
    C170.insert(13, 'chave_nf', C170['id_pai'].map(C100.set_index('id')['9']))       # incluido a CHAVE_NF

    
    ###  C175
    C175 = (df[df['1'] == 'C175']
            .iloc[:,0:22]
            .replace(',', '.', regex=True))
    C175[['3', '4', '6', '7', '8', '9', '10', '12', '13', '14', '15', '16']] = C175[['3', '4', '6', '7', '8', '9', '10', '12', '13', '14', '15', '16']].apply(pd.to_numeric, errors='coerce')
    C175.insert(6, 'cfop_descr', C175['2'].map(cfop_cod_descr))     # incluido a descrição do CFOP
    C175.insert(4, 'mod_nf', C175['id_pai'].map(C100.set_index('id')['5']))       # incluido o Modelo da NF
    C175.insert(4, 'ind_emit', C175['id_pai'].map(C100.set_index('id')['3']))       # incluido o IND_EMIT (0-propria; 1-terceiros)
    C175.insert(4, 'ind_oper', C175['id_pai'].map(C100.set_index('id')['2']))       # incluido o IND_OPER (0-entrada; 1-saida)
    C175.insert(4, 'uf_empresa', C175['cnpj'].map(reg_0140.set_index('4')['5']))       # incluido a UF da Empresa em análise
    C175.insert(4, 'cidade_estab', C175['cnpj'].map(reg_0140.set_index('4')['7']))       # incluido a cidade do estabelecimento
    

    ### C181 
    C181 = (df[df['1'] == 'C181']
            .iloc[:,0:15]
            .replace(',', '.', regex=True))
    C181[[ '4', '5', '6','10']] = C181[[ '4', '5', '6','10']].apply(pd.to_numeric, errors='coerce')
    C181.insert(6, 'cst_descr', C181['2'].map(cst_pis_cofins))      # incluido a descrição do CST
    C181.insert(8, 'cfop_descr', C181['3'].map(cfop_cod_descr))     # incluido a descrição do CFOP


    ### C185
    C185 = (df[df['1'] == 'C185']
            .iloc[:,0:15]
            .replace(',', '.', regex=True))
    C185[[ '4', '5', '6','10']] = C185[[ '4', '5', '6','10']].apply(pd.to_numeric, errors='coerce')
    C185.insert(6, 'cst_descr', C185['2'].map(cst_pis_cofins))      # incluido a descrição do CST
    C185.insert(8, 'cfop_descr', C185['3'].map(cfop_cod_descr))     # incluido a descrição do CFOP

    return C100, C170, C175, C181, C185


# filtro blocos SPED FISCAL --------------------------------------------------------------------------------------------------------------

def Bloco_0_Sped_Fiscal(df):

    ### registro 0150
    REG_0150_SF = (df[df['1'] == '0150']
            .iloc[:,0:19]
            .sort_values(by='5', ascending=False))
    REG_0150_SF = REG_0150_SF.drop_duplicates(subset='2')

    ### registro 0200
    REG_0200_SF = (df[df['1'] == '0200']
            .iloc[:,0:19]
            .sort_values(by='8', ascending=False))
    REG_0200_SF = REG_0200_SF.drop_duplicates(subset='2') 

    return REG_0150_SF, REG_0200_SF


def Bloco_C_Sped_Fiscal(df, REG_0150_SF, REG_0200_SF):

    # C100
    C100_SF = (df[df['1'] == 'C100']
            .iloc[:,0:35]
            .replace(',', '.', regex=True))
    C100_SF[['12', '14', '15', '16', '18', '19', '20', '21','22', '23', '24', '26', '27', '28', '29']] = C100_SF[['12', '14', '15', '16', '18', '19', '20', '21','22', '23', '24', '26', '27', '28', '29']].apply(pd.to_numeric, errors='coerce')


    # C170
    C170_SF = (df[df['1'] == 'C170']
            .iloc[:,0:44]
            .replace(',', '.', regex=True))
    C170_SF[['5', '7', '8', '13', '14', '15', '16', '17', '18', '22', '23', '24', '26', '27', '28', '29', '30', '32', '33', '34', '35', '36', '38']] = C170_SF[['5', '7', '8', '13', '14', '15', '16', '17', '18', '22', '23', '24', '26', '27', '28', '29', '30', '32', '33', '34', '35', '36', '38']].apply(pd.to_numeric, errors='coerce')
    C170_SF.insert(17, 'cfop_descr', C170_SF['11'].map(cfop_cod_descr))     # incluido a descrição do CFOP
    C170_SF.insert(8, 'ncm', C170_SF['3'].map(REG_0200_SF.set_index('2')['8']))       # incluido a NCM
    C170_SF.insert(10, 'item_descr', C170_SF['3'].map(REG_0200_SF.set_index('2')['3']))       # incluido a DESCR_ITEM
    C170_SF.insert(6, 'mod_nf', C170_SF['id_pai'].map(C100_SF.set_index('id')['5']))       # incluido o Modelo da NF
    C170_SF.insert(6, 'ind_emit', C170_SF['id_pai'].map(C100_SF.set_index('id')['3']))       # incluido o IND_EMIT (0-propria; 1-terceiros)
    C170_SF.insert(6, 'ind_oper', C170_SF['id_pai'].map(C100_SF.set_index('id')['2']))       # incluido o IND_OPER (0-entrada; 1-saida)
    C170_SF.insert(6, 'part_cod', C170_SF['id_pai'].map(C100_SF.set_index('id')['4']))       # incluido COD_PART 
    C170_SF.insert(7, 'part_nome', C170_SF['part_cod'].map(REG_0150_SF.set_index('2')['3']))       # incluido a Descrição do Participante
    C170_SF.insert(7, 'part_cnpj', C170_SF['part_cod'].map(REG_0150_SF.set_index('2')['5']))       # incluido o CNPJ do Participante
    C170_SF.insert(7, 'part_uf_cod', C170_SF['part_cod'].map(REG_0150_SF.set_index('2')['8'].astype(str).str[:2]))       # incluido a UF do Participante
    C170_SF.insert(7, 'part_ie', C170_SF['part_cod'].map(REG_0150_SF.set_index('2')['7']))       # incluido a IE do Participante
    C170_SF.insert(7, 'part_uf', C170_SF['part_uf_cod'].map(cod_uf)) 
    C170_SF.drop('part_uf_cod', axis=1, inplace=True)
    # update column names
    C170_SF = C170_SF.rename(columns={
        '1': 'REG',
        '2': 'NUM_ITEM',
        '3': 'COD_ITEM',
        '4': 'DESCR_COMPL',
        '5': 'QTD',
        '6': 'UNID',
        '7': 'VL_ITEM',
        '8': 'VL_DESC',
        '9': 'IND_MOV',
        '10': 'CST_ICMS',
        '11': 'CFOP',
        '12': 'COD_NAT',
        '13': 'VL_BC_ICMS',
        '14': 'ALIQ_ICMS',
        '15': 'VL_ICMS',
        '16': 'VL_BC_ICMS_ST',
        '17': 'ALIQ_ST',
        '18': 'VL_ICMS_ST',
        '19': 'IND_APUR',
        '20': 'CST_IPI',
        '21': 'COD_ENQ',
        '22': 'VL_BC_IPI',
        '23': 'ALIQ_IPI',
        '24': 'VL_IPI',
        '25': 'CST_PIS',
        '26': 'VL_BC_PIS',
        '27': 'ALIQ_PIS',
        '28': 'QUANT_BC_PIS',
        '29': 'ALIQ_PIS_QUANT',
        '30': 'VL_PIS',
        '31': 'CST_COFINS',
        '32': 'VL_BC_COFINS',
        '33': 'ALIQ_COFINS',
        '34': 'QUANT_BC_COFINS',
        '35': 'ALIQ_COFINS_QUANT',
        '36': 'VL_COFINS',
        '37': 'COD_CTA',
        '38': 'VL_ABAT_NT'
    })
        
    # C190
    C190_SF = (df[df['1'] == 'C190']
            .iloc[:,0:18]
            .replace(',', '.', regex=True))
    C190_SF[['4', '5', '6', '7', '8', '9', '10', '11']] = C190_SF[['4', '5', '6', '7', '8', '9', '10', '11']].apply(pd.to_numeric, errors='coerce')
    C190_SF.insert(8, 'cst_icms_descr', C190_SF['2'].map(cst_icms))     # incluido a descrição do CST ICMS
    C190_SF.insert(10, 'cfop_descr', C190_SF['3'].map(cfop_cod_descr))     # incluido a descrição do CFOP


    # C197
    C197_SF = (df[df['1'] == 'C197']
            .iloc[:,0:14]
            .replace(',', '.', regex=True))
    C197_SF[['5', '6', '7', '8']] = C197_SF[['5', '6', '7', '8']].apply(pd.to_numeric, errors='coerce')
    C197_SF.insert(8, 'cod_aj_doc', C197_SF['2'].map(sped_fiscal_tab_5_3_AM))     # incluido a descrição do cod_aj_doc da NF
    C197_SF.insert(11, 'ncm', C197_SF['4'].map(REG_0200_SF.set_index('2')['8']))       # incluido a NCM
    C197_SF.insert(12, 'item_descr', C197_SF['4'].map(REG_0200_SF.set_index('2')['3']))       # incluido a DESCR_ITEM
    C197_SF.insert(6, 'ind_oper', C197_SF['id_pai'].map(C100_SF.set_index('id')['2']))       # incluido o IND_OPER (0-entrada; 1-saida)
    C197_SF.insert(7, 'chave_nf', C197_SF['id_pai'].map(C100_SF.set_index('id')['9']))       # incluido a CHAVE_NF

    



    # C590
    C590_SF = (df[df['1'] == 'C590']
            .iloc[:,0:17]
            .replace(',', '.', regex=True))
    C590_SF[['4', '5', '6', '7', '8', '9', '10']] = C590_SF[['4', '5', '6', '7', '8', '9', '10']].apply(pd.to_numeric, errors='coerce')
    C590_SF.insert(8, 'cst_icms_descr', C590_SF['2'].map(cst_icms))     # incluido a descrição do CST ICMS
    C590_SF.insert(10, 'cfop_descr', C590_SF['3'].map(cfop_cod_descr))     # incluido a descrição do CFOP

    

    return C100_SF, C170_SF, C190_SF, C197_SF, C590_SF


def Bloco_D_Sped_Fiscal(df):

    # D190 - CTe Frete
    D190_SF = (df[df['1'] == 'D190']
            .iloc[:,0:15]
            .replace(',', '.', regex=True))
    D190_SF[['4', '5', '6', '7', '8']] = D190_SF[['4', '5', '6', '7', '8']].apply(pd.to_numeric, errors='coerce')
    D190_SF.insert(8, 'cst_icms_descr', D190_SF['2'].map(cst_icms))     # incluido a descrição do CST ICMS
    D190_SF.insert(10, 'cfop_descr', D190_SF['3'].map(cfop_cod_descr))     # incluido a descrição do CFOP

    # D590 - NF Serviço Comunicação
    D590_SF = (df[df['1'] == 'D590']
            .iloc[:,0:17]
            .replace(',', '.', regex=True))
    D590_SF[['4', '5', '6', '7', '8', '9', '10']] = D590_SF[['4', '5', '6', '7', '8', '9', '10']].apply(pd.to_numeric, errors='coerce')
    D590_SF.insert(8, 'cst_icms_descr', D590_SF['2'].map(cst_icms))     # incluido a descrição do CST ICMS
    D590_SF.insert(10, 'cfop_descr', D590_SF['3'].map(cfop_cod_descr))     # incluido a descrição do CFOP

    return D190_SF, D590_SF


def Bloco_E_Sped_Fiscal(df):

    # E110 - Resumo Apuração Geral
    E110_SF = (df[df['1'] == 'E110']
            .iloc[:,0:21]
            .replace(',', '.', regex=True))
    E110_SF[['2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12' ,'13', '14', '15']] = E110_SF[['2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12' ,'13', '14', '15']].apply(pd.to_numeric, errors='coerce')


    # E111 - AJUSTE/BENEFÍCIO/INCENTIVO DA APURAÇÃO DO ICMS
    E111_SF = (df[df['1'] == 'E111']
            .iloc[:,0:10]
            .replace(',', '.', regex=True))
    E111_SF['4'] = E111_SF['4'].apply(pd.to_numeric, errors='coerce')
    E111_SF.insert(8, 'cod_aj_apur', E111_SF['2'].map(sped_fiscal_tab_5_1_1))     # incluido a descrição do cod_aj_apur


    # E116 - OBRIGAÇÕES DO ICMS RECOLHIDO OU A RECOLHER – OPERAÇÕES PRÓPRIAS
    E116_SF = (df[df['1'] == 'E116']
            .iloc[:,0:16]
            .replace(',', '.', regex=True))
    E116_SF['4'] = E116_SF['4'].apply(pd.to_numeric, errors='coerce')
    E116_SF.insert(8, 'cod_obg_recolher', E116_SF['2'].map(sped_fiscal_tab_5_4))     # incluido a descrição do codigo da obrigação a recolher
    E116_SF.insert(12, 'cod_receita', E116_SF['5'].map(sped_fiscal_cod_receita_AM))     # incluido a descrição do codigo da receita

    return E110_SF, E111_SF, E116_SF


def Bloco_1_Sped_Fiscal(df):

    # 1900 - INDICADOR DE SUB-APURAÇÃO DO ICMS
    REG_1900_SF = (df[df['1'] == '1900']
            .iloc[:,0:9]
            .replace(',', '.', regex=True))
    REG_1900_SF.insert(8, 'ind_apur_icms', REG_1900_SF['2'].map(sped_fiscal_tab_ind_apur_icms_AM))     # incluido a descrição do ind_apur_icms


    # 1920 - SUB-APURAÇÃO DO ICMS
    REG_1920_SF = (df[df['1'] == '1920']
            .iloc[:,0:19]
            .replace(',', '.', regex=True))
    REG_1920_SF[['2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12' ,'13']] = REG_1920_SF[['2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12' ,'13']].apply(pd.to_numeric, errors='coerce')


    # 1921 - AJUSTE/BENEFÍCIO/INCENTIVO DA SUB-APURAÇÃO DO ICMS
    REG_1921_SF = (df[df['1'] == '1921']
            .iloc[:,0:10]
            .replace(',', '.', regex=True))
    REG_1921_SF['4'] = REG_1921_SF['4'].apply(pd.to_numeric, errors='coerce')
    REG_1921_SF.insert(8, 'cod_aj_apur', REG_1921_SF['2'].map(sped_fiscal_tab_5_1_1))     # incluido a descrição do cod_aj_apur


    # 1925 - INFORMAÇÕES ADICIONAIS DA SUB-APURAÇÃO – VALORES DECLARATÓRIOS
    REG_1925_SF = (df[df['1'] == '1925']
            .iloc[:,0:10]
            .replace(',', '.', regex=True))
    REG_1925_SF['3'] = REG_1925_SF['3'].apply(pd.to_numeric, errors='coerce')
    REG_1925_SF.insert(8, 'cod_info_adic', REG_1925_SF['2'].map(sped_fiscal_tab_5_2))     # incluido a descrição do cod_info_adic


    # 1926 - OBRIGAÇÕES DO ICMS A RECOLHER – OPERAÇÕES REFERENTES À SUB-APURAÇÃO
    REG_1926_SF = (df[df['1'] == '1926']
            .iloc[:,0:16]
            .replace(',', '.', regex=True))
    REG_1926_SF['3'] = REG_1926_SF['3'].apply(pd.to_numeric, errors='coerce')
    REG_1926_SF.insert(8, 'cod_obg_recolher', REG_1926_SF['2'].map(sped_fiscal_tab_5_4))     # incluido a descrição do cod_info_adic
    REG_1926_SF.insert(12, 'cod_receita', REG_1926_SF['5'].map(sped_fiscal_cod_receita_AM))     # incluido a descrição do codigo da receita



    return REG_1900_SF, REG_1920_SF, REG_1921_SF, REG_1925_SF, REG_1926_SF


# filtro blocos ECD

def Bloco_I_ECD(df):
    REG_I050_ECD = (df[df['1'] == 'I050'].iloc[:,0:12])
    REG_I051_ECD = (df[df['1'] == 'I051'].iloc[:,0:7])
    REG_I150_ECD = (df[df['1'] == 'I150'].iloc[:,0:7])
    REG_I155_ECD = (df[df['1'] == 'I155']
                    .iloc[:,0:13]
                    .replace(',', '.', regex=True))
    REG_I155_ECD[['4', '6', '7', '8']] = REG_I155_ECD[['4', '6', '7', '8']].apply(pd.to_numeric, errors='coerce')

    REG_I200_ECD = (df[df['1'] == 'I200'].iloc[:,0:10])
    REG_I250_ECD = (df[df['1'] == 'I250'].iloc[:,0:13])

    REG_I050_ECD.insert(10, 'CTA_REF', REG_I050_ECD['id'].map(REG_I051_ECD.set_index('id_pai')['3']))       # incluido COD_CTA_REF
    REG_I050_ECD.insert(11, 'CTA_REF_DESCR', REG_I050_ECD['CTA_REF'].map(PLANO_CONTAS_REF))     # incluido descrição da COD_CTA_REF

    REG_I155_ECD.insert(4, 'periodo_inicio', REG_I155_ECD['id_pai'].map(REG_I150_ECD.set_index('id')['2']))       # incluido periodo_inicio
    REG_I155_ECD.insert(5, 'periodo_final', REG_I155_ECD['id_pai'].map(REG_I150_ECD.set_index('id')['3']))       # incluido periodo_final
    REG_I155_ECD.insert(8, 'CTA_DESCR', REG_I155_ECD['2'].map(REG_I050_ECD.set_index('6')['8']))       # incluido a descrição da conta contabil
    REG_I155_ECD.insert(9, 'CTA_REF', REG_I155_ECD['2'].map(REG_I050_ECD.set_index('6')['CTA_REF']))       # incluido COD_CTA_REF
    REG_I155_ECD.insert(10, 'CTA_REF_DESCR', REG_I155_ECD['2'].map(REG_I050_ECD.set_index('6')['CTA_REF_DESCR']))       # incluido descrição da COD_CTA_REF
    REG_I155_ECD = REG_I155_ECD.rename(columns={
        '1': 'REG',
        '2': 'COD_CTA',
        '3': 'COD_CCUS',
        '4': 'VL_SLD_INI',
        '5': 'IND_DC_INI',
        '6': 'VL_DEB',
        '7': 'VL_CRED',
        '8': 'VL_SLD_FIN',
        '9': 'IND_DC_FIN',
    })

    REG_I350_ECD = (df[df['1'] == 'I350'].iloc[:,0:6])
    REG_I355_ECD = (df[df['1'] == 'I355']
                    .iloc[:,0:9]
                    .replace(',', '.', regex=True))
    REG_I355_ECD[['4']] = REG_I355_ECD[['4']].apply(pd.to_numeric, errors='coerce')
    REG_I355_ECD.insert(5, 'periodo_final', REG_I355_ECD['id_pai'].map(REG_I350_ECD.set_index('id')['2']))       # incluido periodo_final
    REG_I355_ECD.insert(7, 'CTA_DESCR', REG_I355_ECD['2'].map(REG_I050_ECD.set_index('6')['8']))       # incluido a descrição da conta contabil

    REG_I355_ECD.insert(8, 'CTA_REF', REG_I355_ECD['2'].map(REG_I050_ECD.set_index('6')['CTA_REF']))       # incluido COD_CTA_REF
    REG_I355_ECD.insert(9, 'CTA_REF_DESCR', REG_I355_ECD['2'].map(REG_I050_ECD.set_index('6')['CTA_REF_DESCR']))       # incluido descrição da COD_CTA_REF
    REG_I355_ECD = REG_I355_ECD.rename(columns={
        '1': 'REG',
        '2': 'COD_CTA',
        '3': 'COD_CCUS',
        '4': 'VL_CTA',
        '5': 'IND_DC'
    })

    return REG_I050_ECD, REG_I051_ECD, REG_I150_ECD, REG_I155_ECD, REG_I200_ECD, REG_I250_ECD, REG_I355_ECD



# --------------------------------------------------------------------------------------------------------------------
# DATAFRAMES PARA ANALISE SPED CONTRIBUICOES
# --------------------------------------------------------------------------------------------------------------------


def blobo_M_filtering(M105, M110, M210, M400, M510, M610):

    #M110
    df_ajuste_acresc_pis = M110[M110['2'] == '1'][['4', 'tipo_ajuste', '6', '3', '5', '7']].sort_values(by='3', ascending=False)
    vlr_ajuste_acresc_pis = df_ajuste_acresc_pis['3'].sum()

    #M510
    df_ajuste_acresc_cofins = M510[M510['2'] == '1'][['4', 'tipo_ajuste', '6', '3', '5', '7']].sort_values(by='3', ascending=False)
    vlr_ajuste_acresc_cofins = df_ajuste_acresc_cofins['3'].sum()

    #M105
    df_cred_por_tipo = M105.groupby(['2', 'NAT_BC_CRED'])['7'].sum().round(2).reset_index().sort_values(by='7', ascending=False)
        # adicionando o valor dos Ajustes de Acréscimo (M110/M550) no Dataframe
    new_row = ['-', 'ajuste de crédito', ((vlr_ajuste_acresc_pis + vlr_ajuste_acresc_cofins)/0.0925)]  
    df_cred_por_tipo.loc[len(df_cred_por_tipo)] = new_row
    cred_bc_total = df_cred_por_tipo['7'].sum()      # soma das bc de cada tipo de credito
    df_cred_por_tipo['VLR_CRED_PIS_COFINS'] = (df_cred_por_tipo['7'] * 0.0925).apply(lambda x: round(x,2))
    df_cred_por_tipo['proporção'] = (df_cred_por_tipo['7'] / cred_bc_total * 100).apply(lambda x: f"{x:.0f}%")
    df_cred_por_tipo = df_cred_por_tipo.rename(columns={
        '2': 'CST_PIS_COFINS',
        '7': 'BC_CRED_PIS_COFINS'
    })
    cred_total = df_cred_por_tipo['VLR_CRED_PIS_COFINS'].sum()

    # tabela completa com todas as NAT CRED
        # ---------- Complete NAT table (codes + descriptions) ----------
    nat_series = pd.Series(tab_4_3_7, name="NAT_BC_CRED")  # code -> description
    df_nat_universe = nat_series.rename_axis("NAT_CODE").reset_index()
    src = df_cred_por_tipo.copy()

    # Make sure src has NAT_CODE
    inv_tab_4_3_7 = {v: k for k, v in tab_4_3_7.items()}  # description -> code
    if "NAT_CODE" not in src.columns:
        if "NAT_BC_CRED" in src.columns:
            # If df_cred_por_tipo has descriptions, map back to codes
            src["NAT_CODE"] = src["NAT_BC_CRED"].map(inv_tab_4_3_7)
        elif "2" in src.columns:
            # Fallback: if the raw NAT code remained in column '2'
            src = src.rename(columns={"2": "NAT_CODE"})

    cols_keep = ["NAT_CODE", "BC_CRED_PIS_COFINS", "VLR_CRED_PIS_COFINS"]
    if "proporção" in src.columns:
        cols_keep.append("proporção")

    df_cred_por_tipo_all = df_nat_universe.merge(src[cols_keep], on="NAT_CODE", how="left")

    # Fill missing values
    for c in ("BC_CRED_PIS_COFINS", "VLR_CRED_PIS_COFINS"):
        if c in df_cred_por_tipo_all.columns:
            df_cred_por_tipo_all[c] = df_cred_por_tipo_all[c].fillna(0)
    if "proporção" in df_cred_por_tipo_all.columns:
        df_cred_por_tipo_all["proporção"] = df_cred_por_tipo_all["proporção"].fillna("0%")


    #M210
    df_receitas_com_debito = M210[['2', 'cod_contribuicao', '3', '4', '8', '11']]
    df_receitas_com_debito = df_receitas_com_debito.copy()

        # Create a composite key for mapping in df_receitas_com_debito
    df_receitas_com_debito.loc[:, 'key'] = list(zip(df_receitas_com_debito['2'], df_receitas_com_debito['3']))

        # Create a mapping Series from M610 using the composite key
    mapping_series_aliq = M610.set_index(['2', '3'])['8']
    mapping_series_valor = M610.set_index(['2', '3'])['11']

        # Map the composite key to get 'valor_cofins'
    df_receitas_com_debito.loc[:, 'aliq_cofins'] = df_receitas_com_debito['key'].map(mapping_series_aliq)
    df_receitas_com_debito.loc[:, 'valor_cofins'] = df_receitas_com_debito['key'].map(mapping_series_valor)
    df_receitas_com_debito = df_receitas_com_debito.drop(columns=['key'])
    df_receitas_com_debito[['aliq_cofins', 'valor_cofins']] = df_receitas_com_debito[['aliq_cofins', 'valor_cofins']].apply(pd.to_numeric, errors='coerce')


    #M400
    df_receitas_sem_debito = M400[['2', 'cst_descr', '3']].sort_values(by='3', ascending=False)

    
    return df_cred_por_tipo_all, df_cred_por_tipo, cred_bc_total, cred_total, df_receitas_com_debito, df_receitas_sem_debito, df_ajuste_acresc_pis, vlr_ajuste_acresc_pis, df_ajuste_acresc_cofins, vlr_ajuste_acresc_cofins


def blobo_A_filtering(A100, A170):

    df_serv_tomados = A170[A170['ind_oper'] == '0'].groupby(['ind_oper','3', 'descr_serv_0200', '9', '11', '15'], dropna=False)[['5', '10', '12', '16', 'iss']].sum().round(2).sort_values(by='5', ascending=False).reset_index()
    df_serv_tomados['IBS'] = 0
    df_serv_tomados['CBS'] = 0
    df_serv_tomados = df_serv_tomados.rename(columns={
        "3":"cod_serv",
        "descr_serv_0200":"descricao_servico",
        "9":"cst_pis_cofins",
        "11":"pis_aliq",
        "15":"cofins_aliq",
        "5":"vlr_servico",
        "10":"pis_cofins_bc",
        "12":"pis_vlr",
        "16":"cofins_vlr"
    })

    df_serv_prestados = A170[A170['ind_oper'] == '1'].groupby(['ind_oper','3', 'descr_serv_0200', '9', '11', '15'], dropna=False)[['5', '10', '12', '16', 'iss']].sum().round(2).sort_values(by='5', ascending=False).reset_index()
    df_serv_prestados['IBS'] = 0
    df_serv_prestados['CBS'] = 0
    df_serv_prestados = df_serv_prestados.rename(columns={
        "3":"cod_serv",
        "descr_serv_0200":"descricao_servico",
        "9":"cst_pis_cofins",
        "11":"pis_aliq",
        "15":"cofins_aliq",
        "5":"vlr_servico",
        "10":"pis_cofins_bc",
        "12":"pis_vlr",
        "16":"cofins_vlr"
    })
    

    return df_serv_tomados, df_serv_prestados


def blobo_C_filtering(C100, C170, C175, C181, C185):


    df_C170_saidas = C170[(C170['ind_oper'] == '1') & (C170['cfop_descr'].str.lower().fillna('').str.startswith("venda"))]



    # NF-e MOD 55
    df_C170_por_mod55_aliq = C170.groupby(['ind_oper', 'mod_nf', '27', '33'], dropna=False)[['7', '26', '30', '36']].sum().round(2).sort_values(by='7', ascending=False).reset_index()
    
    # C170
    df_C170_COMPRA_por_item_cfop_cst_aliq_ncm = C170[C170['cfop_descr'].str.lower().fillna('').str.startswith("compra")].groupby(['3', 'item_descr', '11', 'cfop_descr', 'uf_empresa', 'part_uf', '25', '27', 'ncm'], dropna=False)[['7', '26', '30']].sum().round(2).sort_values(by='7', ascending=False).reset_index()
    df_C170_venda_por_ncm = C170[C170['cfop_descr'].str.lower().fillna('').str.startswith("venda")].groupby('ncm', dropna=False)[['7', '26', '30', '36']].sum().round(2).sort_values(by='7', ascending=False).reset_index()

    # NFC-e MOD 65
    df_C175_por_mod65_aliq = C175.groupby(['ind_oper', 'mod_nf', '7', '13'], dropna=False)[['3', '6', '10', '16']].sum().round(2).sort_values(by='3', ascending=False).reset_index()

    # tabela_venda_por_estab
    df_C170_venda_por_estab = C170[C170['cfop_descr'].str.lower().fillna('').str.startswith("venda")].groupby(['uf_empresa', 'cnpj'], dropna=False)[['7', '26', '30', '36']].sum().round(2).sort_values(by='7', ascending=False).reset_index()
    df_C170_venda_por_estab = df_C170_venda_por_estab.rename(columns={
        'uf_empresa': 'uf_empresa',
        'cnpj': 'cnpj',
        '7': 'valor_opr',
        '26': 'bc',
        '30': 'vlr_pis',
        '36': 'vlr_cofins'
    })

    df_C175_venda_por_estab = C175.groupby(['uf_empresa', 'cnpj'], dropna=False)[['3', '6', '10', '16']].sum().round(2).sort_values(by='3', ascending=False).reset_index()
    df_C175_venda_por_estab = df_C175_venda_por_estab.rename(columns={
        'uf_empresa': 'uf_empresa',
        'cnpj': 'cnpj',
        '3': 'valor_opr',
        '6': 'bc',
        '10': 'vlr_pis',
        '16': 'vlr_cofins'
    })

    df_final_venda_por_estab = pd.concat([df_C170_venda_por_estab, df_C175_venda_por_estab], ignore_index=True)
    df_final_venda_por_estab = df_final_venda_por_estab.groupby(['uf_empresa', 'cnpj'], dropna=False)[['valor_opr', 'bc', 'vlr_pis', 'vlr_cofins']].sum().round(2).sort_values(by='valor_opr', ascending=False).reset_index()

    # tabela_venda_por_uf_do_estab
    df_final_venda_por_uf_estab = df_final_venda_por_estab.groupby('uf_empresa', dropna=False)[['valor_opr', 'bc', 'vlr_pis', 'vlr_cofins']].sum().round(2).sort_values(by='valor_opr', ascending=False).reset_index()

    # tabela_venda_por_cfop
    df_C170_venda_por_cfop = C170[C170['cfop_descr'].str.lower().fillna('').str.startswith("venda")].groupby(['11','cfop_descr'], dropna=False)[['7', '26', '30', '36']].sum().round(2).sort_values(by='7', ascending=False).reset_index()
    df_C170_venda_por_cfop = df_C170_venda_por_cfop.rename(columns={
        '11': 'CFOP',
        'cfop_descr': 'cfop_descr',
        '7': 'valor_opr',
        '26': 'bc',
        '30': 'vlr_pis',
        '36': 'vlr_cofins'
    })
    df_C175_venda_por_cfop = C175.groupby(['2', 'cfop_descr'], dropna=False)[['3', '6', '10', '16']].sum().round(2).sort_values(by='3', ascending=False).reset_index()
    df_C175_venda_por_cfop = df_C175_venda_por_cfop.rename(columns={
        '2': 'CFOP',
        'cfop_descr': 'cfop_descr',
        '3': 'valor_opr',
        '6': 'bc',
        '10': 'vlr_pis',
        '16': 'vlr_cofins'
    })
    df_C181_venda_por_cfop = C181.groupby(['3', 'cfop_descr'], dropna=False)[['4']].sum().round(2).sort_values(by='4', ascending=False).reset_index()


    df_final_venda_por_cfop = pd.concat([df_C170_venda_por_cfop, df_C175_venda_por_cfop], ignore_index=True)
    df_final_venda_por_cfop = df_final_venda_por_cfop.groupby(['CFOP', 'cfop_descr'], dropna=False)[['valor_opr', 'bc', 'vlr_pis', 'vlr_cofins']].sum().round(2).sort_values(by='valor_opr', ascending=False).reset_index()



    return df_C170_por_mod55_aliq, df_C170_COMPRA_por_item_cfop_cst_aliq_ncm, df_C175_por_mod65_aliq, df_C170_venda_por_ncm, df_final_venda_por_estab, df_final_venda_por_uf_estab, df_final_venda_por_cfop, df_C170_saidas


# --------------------------------------------------------------------------------------------------------------------
# DATAFRAMES PARA ANALISE SPED FISCAL
# --------------------------------------------------------------------------------------------------------------------









# --------------------------------------------------------------------------------------------------------------------
# DATAFRAMES PARA REFORMA TRIBUTARIA
# --------------------------------------------------------------------------------------------------------------------


def base_saidas_reforma(C100_SF, C197_SF, C170_SC):

    C170_SC = C170_SC[C170_SC['ind_oper'] == '1'].copy()


    # ----------------------------------------------------------------
    # incluindo a IE no registro C170_SC
    # ----------------------------------------------------------------

    # Create composite keys and a unique mapping (avoid non-unique index in map)
    c100_map = C100_SF[["9", "2", "ie_estab"]].copy()
    c100_map = c100_map.dropna(subset=["9", "2"])  # ensure tuple is well-formed
    c100_map["key"] = list(zip(c100_map["9"], c100_map["2"]))
    c100_map = c100_map.drop_duplicates(subset=["key"], keep="last")[ ["key", "ie_estab"] ].set_index("key")

    C170_SC["key"] = list(zip(C170_SC["chave_nf"], C170_SC["ind_oper"]))
    C170_SC.insert(loc=4, column="ie_estab", value=C170_SC["key"].map(c100_map["ie_estab"]))
    C170_SC.drop(columns="key", inplace=True)


    # ----------------------------------------------------------------
    # incluindo o COD_AJUSTE no registro C170_SC
    # ----------------------------------------------------------------

    # Create composite keys and a unique mapping for C197 → avoid non-unique index in map
    c197_map = C197_SF[["chave_nf", "4", "2"]].copy()
    c197_map = c197_map.dropna(subset=["chave_nf", "4"])  # ensure tuple is well-formed
    c197_map["key2"] = list(zip(c197_map["chave_nf"], c197_map["4"]))
    c197_map = c197_map.drop_duplicates(subset=["key2"], keep="last")[ ["key2", "2"] ].set_index("key2")

    C170_SC["key2"] = list(zip(C170_SC["chave_nf"], C170_SC["3"]))
    C170_SC.insert(10, "cod_aj_doc", C170_SC["key2"].map(c197_map["2"]))
    C170_SC.insert(11, 'cod_aj_doc_descr', C170_SC['cod_aj_doc'].map(sped_fiscal_tab_5_3_AM))
    C170_SC.drop(columns="key2", inplace=True)

    # Incluir colunas IBS e CBS com valor zerado por enquanto 
    C170_SC['IBS'] = 0
    C170_SC['CBS'] = 0


    # ----------------------------------------------------------------
    # incluindo a tributação do IBS e CBS
    # ----------------------------------------------------------------

    C170_SC["ie_estab_prefix"] = C170_SC["ie_estab"].astype(str).str[:4]

    # For each row, get the IBS rate from `regras_ibs_saidas_zfm`
    def get_ibs(row):
        cfop = str(row["11"])
        prefix = row["ie_estab_prefix"]

        # Try exact rule (prefix, cfop); if not found, fall back to ('*', cfop)
        rate = regras_ibs_saidas_zfm.get((prefix, cfop))
        if rate is None:
            rate = regras_ibs_saidas_zfm.get(('*', cfop), 0.0)

        return rate * row["7"]

    # For each row, get the CBS rate from `regras_cbs_saidas_zfm`
    def get_cbs(row):
        cfop = str(row["11"])
        prefix = row["ie_estab_prefix"]

        # Try exact rule (prefix, cfop); if not found, fall back to ('*', cfop)
        rate = regras_cbs_saidas_zfm.get((prefix, cfop))
        if rate is None:
            rate = regras_cbs_saidas_zfm.get(('*', cfop), 0.0)

        return rate * row["7"]
    

    # Now apply them
    C170_SC["IBS"] = C170_SC.apply(get_ibs, axis=1)
    C170_SC["CBS"] = C170_SC.apply(get_cbs, axis=1)

    # update column names
    C170_SC = C170_SC.rename(columns={
        '1': 'REG',
        '2': 'NUM_ITEM',
        '3': 'COD_ITEM',
        '4': 'DESCR_COMPL',
        '5': 'QTD',
        '6': 'UNID',
        '7': 'VL_ITEM',
        '8': 'VL_DESC',
        '9': 'IND_MOV',
        '10': 'CST_ICMS',
        '11': 'CFOP',
        '12': 'COD_NAT',
        '13': 'VL_BC_ICMS',
        '14': 'ALIQ_ICMS',
        '15': 'VL_ICMS',
        '16': 'VL_BC_ICMS_ST',
        '17': 'ALIQ_ST',
        '18': 'VL_ICMS_ST',
        '19': 'IND_APUR',
        '20': 'CST_IPI',
        '21': 'COD_ENQ',
        '22': 'VL_BC_IPI',
        '23': 'ALIQ_IPI',
        '24': 'VL_IPI',
        '25': 'CST_PIS',
        '26': 'VL_BC_PIS',
        '27': 'ALIQ_PIS',
        '28': 'QUANT_BC_PIS',
        '29': 'ALIQ_PIS_QUANT',
        '30': 'VL_PIS',
        '31': 'CST_COFINS',
        '32': 'VL_BC_COFINS',
        '33': 'ALIQ_COFINS',
        '34': 'QUANT_BC_COFINS',
        '35': 'ALIQ_COFINS_QUANT',
        '36': 'VL_COFINS',
        '37': 'COD_CTA',
    })


    return C170_SC



# ----------------------------------------------------------------
# Streamlit
# Setup Session State
# ----------------------------------------------------------------

if "processing_done" not in st.session_state:
    st.session_state["processing_done"] = False



# ----------------------------------------------------------------
# Sidebar Navigation
# ----------------------------------------------------------------

with st.sidebar:
    st.header("TaxDash", divider='gray')

    selected_area = st.radio(
        "Selecione a Seção:",
        [
            "Área 1: Importar Arquivos SPED",
            "Área 2: Compras/Entradas",
            "Área 3: Vendas/Saídas",
            "Área 4: Serviços",
            "Área 5: Reforma Tributária"
        ]
    )
    
    # CSS adicional
    st.markdown("""
    <style>

        .stTabs [data-baseweb="tab-list"] {
            gap: 10px;
        }

        .stTabs [data-baseweb="tab"] {
            height: 40px;
            white-space: pre-wrap;
            background-color: #F0F2F6;
            border-radius: 4px 4px 0px 0px;
            gap: 1px;
            padding-top: 10px;
            padding-bottom: 10px;
            padding-left: 10px;
            padding-right: 10px;
        }

        .stTabs [aria-selected="true"] {
            background-color: red;
            color: white;
        }

    </style>""", unsafe_allow_html=True)




# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Área 1: Página Inicial

if selected_area == "Área 1: Importar Arquivos SPED":

    st.header(":material/database_upload: Importar Arquivos SPED", divider='red')
    st.markdown('##')

    col1, col2 = st.columns([1, 1])
    
    with col1:
    
        st.subheader("ECD")
        uploaded_sped_ecd_file = st.file_uploader(
            label="*Selecione apenas um arquivo ECD*",
            type="txt",
            accept_multiple_files=False
        )
        st.markdown('###')
    
        st.subheader("SPED Contribuições")
        uploaded_contrib_file = st.file_uploader(
            label="*Selecione um ou mais arquivos SPED Contribuições*",
            type="txt",
            accept_multiple_files=True
        )
        st.markdown('###')

        st.subheader("SPED Fiscal")
        uploaded_sped_fiscal_files = st.file_uploader(
            label="*Selecione um ou mais arquivos SPED FISCAL (do mesmo período)*",
            type="txt",
            accept_multiple_files=True
        )
        st.markdown('###')


        if st.button("Processar Arquivos", type='primary'):
            
            # -----------------------------------------------------
            # Visual feedback: list selected files + stage progress
            # -----------------------------------------------------
            feedback = st.container()
            with feedback:
                st.subheader(":material/hourglass_top: Processando arquivos…")


                # Placeholders for stage messages + progress bars
                ph_fiscal_msg = st.empty()
                ph_fiscal_prog = st.progress(0, text="Aguardando importação do SPED Fiscal…")
                ph_contrib_msg = st.empty()
                ph_contrib_prog = st.progress(0, text="Aguardando importação do SPED Contribuições…")
                ph_ecd_msg = st.empty()
                ph_ecd_prog = st.progress(0, text="Aguardando importação da ECD…")
            

            # 0) Check if user provided at least one file (either type)
            if (not uploaded_contrib_file) & (not uploaded_sped_fiscal_files):
                st.warning("É necessário selecionar os arquivo SPED acima.")
                st.stop()
                
            # 1) Check if user provided exactly 1 Contribuições file
            if not uploaded_contrib_file:
                st.warning("É necessário selecionar 1 arquivo SPED Contribuições.")
                st.stop()

            # 2) Check if user provided at least 1 SPED Fiscal file
            if not uploaded_sped_fiscal_files or len(uploaded_sped_fiscal_files) == 0:
                st.warning("É necessário selecionar ao menos 1 arquivo SPED Fiscal.")
                st.stop()


            #-----------------------------------------------------
            # importar SPED FISCAL
            #-----------------------------------------------------
            ph_fiscal_msg.info(f"Importando SPED Fiscal ({len(uploaded_sped_fiscal_files)} arquivo(s))…")
            ph_fiscal_prog.progress(10, text="Lendo SPED Fiscal…")


            # carregar arquivos efd-fiscal 
            df_sped_fiscal = load_and_process_sped_fiscal(uploaded_sped_fiscal_files)
            ph_fiscal_prog.progress(100, text="SPED Fiscal importado com sucesso.")
            ph_fiscal_msg.success(":material/task_alt: SPED Fiscal concluído")

            # filtrar registros efd-fiscal
            REG_0150_SF, REG_0200_SF = Bloco_0_Sped_Fiscal(df_sped_fiscal)
            C100_SF, C170_SF, C190_SF, C197_SF, C590_SF = Bloco_C_Sped_Fiscal(df_sped_fiscal, REG_0150_SF, REG_0200_SF)
            D190_SF, D590_SF = Bloco_D_Sped_Fiscal(df_sped_fiscal)
            E110_SF, E111_SF, E116_SF = Bloco_E_Sped_Fiscal(df_sped_fiscal)
            REG_1900_SF, REG_1920_SF, REG_1921_SF, REG_1925_SF, REG_1926_SF = Bloco_1_Sped_Fiscal(df_sped_fiscal)

            # sessions
            st.session_state["df_sped_fiscal"] = df_sped_fiscal
            st.session_state["REG_0150_SF"] = REG_0150_SF
            st.session_state["REG_0200_SF"] = REG_0200_SF
            st.session_state["C100_SF"] = C100_SF
            st.session_state["C170_SF"] = C170_SF
            st.session_state["C190_SF"] = C190_SF
            st.session_state["C197_SF"] = C197_SF
            st.session_state["C590_SF"] = C590_SF
            st.session_state["D190_SF"] = D190_SF
            st.session_state["D590_SF"] = D590_SF
            st.session_state["E110_SF"] = E110_SF
            st.session_state["E111_SF"] = E111_SF
            st.session_state["E116_SF"] = E116_SF
            st.session_state["REG_1900_SF"] = REG_1900_SF
            st.session_state["REG_1920_SF"] = REG_1920_SF
            st.session_state["REG_1921_SF"] = REG_1921_SF
            st.session_state["REG_1925_SF"] = REG_1925_SF
            st.session_state["REG_1926_SF"] = REG_1926_SF



            #-----------------------------------------------------
            # importar SPED CONTRIBUICOES
            #-----------------------------------------------------
            ph_contrib_msg.info(f"Importando SPED Contribuições ({len(uploaded_contrib_file)} arquivo(s))…")
            ph_contrib_prog.progress(10, text="Lendo SPED Contribuições…")

            # Process the files
            df_contrib = load_and_process_data(uploaded_contrib_file)
            ph_contrib_prog.progress(100, text="SPED Contribuições importado com sucesso.")
            ph_contrib_msg.success(":material/task_alt: SPED Contribuições concluído")

            # Example: extract company details from SPED Contribuições
            empresa = df_contrib.iloc[0, 11]
            raiz_cnpj = df_contrib.iloc[0, 12]

            # Process SPED Contribuições blocks
            reg_0140, reg_0150, reg_0200 = Bloco_0(df_contrib)
            M100, M105, M110, M210, M400, M510, M610 = Bloco_M(df_contrib)
            (df_cred_por_tipo_all, df_cred_por_tipo, cred_bc_total, cred_total, df_receitas_com_debito, 
            df_receitas_sem_debito, df_ajuste_acresc_pis, vlr_ajuste_acresc_pis, 
            df_ajuste_acresc_cofins, vlr_ajuste_acresc_cofins) = blobo_M_filtering(M105, M110, M210, M400, M510, M610)
            A100, A170 = Bloco_A(df_contrib, reg_0140, reg_0150, reg_0200)
            df_serv_tomados, df_serv_prestados = blobo_A_filtering(A100, A170)
            C100, C170, C175, C181, C185 = Bloco_C(df_contrib, reg_0140, reg_0150, reg_0200)
            (df_C170_por_mod55_aliq, df_C170_COMPRA_por_item_cfop_cst_aliq_ncm, 
            df_C175_por_mod65_aliq, df_C170_venda_por_ncm, df_final_venda_por_estab, 
            df_final_venda_por_uf_estab, df_final_venda_por_cfop, df_C170_saidas) = blobo_C_filtering(C100, C170, C175, C181, C185)
            #-----------------------------------------------------
            # importar ECD
            #-----------------------------------------------------
            if uploaded_sped_ecd_file is not None:
                ph_ecd_msg.info(f"Importando ECD ({getattr(uploaded_sped_ecd_file, 'name', '')})…")
                ph_ecd_prog.progress(10, text="Lendo ECD…")
            else:
                ph_ecd_prog.progress(100, text="Nenhum arquivo ECD selecionado.")
                ph_ecd_msg.warning("ECD não informado — etapa ignorada.")

            # Process the files (only if ECD provided)
            if uploaded_sped_ecd_file is not None:
                df_ecd = load_and_process_ecd(uploaded_sped_ecd_file)
                ph_ecd_prog.progress(100, text="ECD importado com sucesso.")
                ph_ecd_msg.success(":material/task_alt: ECD concluído")

                REG_I050_ECD, REG_I051_ECD, REG_I150_ECD, REG_I155_ECD, REG_I200_ECD, REG_I250_ECD, REG_I355_ECD = Bloco_I_ECD(df_ecd)

                st.session_state["df_ecd"] = df_ecd
                st.session_state["REG_I155_ECD"] = REG_I155_ECD
                st.session_state["REG_I355_ECD"] = REG_I355_ECD









            #-----------------------------------------------------
            # importar funções para REFORMA TRIBUTARIA
            #-----------------------------------------------------

            df_saidas_reforma = base_saidas_reforma(C100_SF, C197_SF, C170)

            st.session_state["df_saidas_reforma"] = df_saidas_reforma

            st.session_state["df_cred_por_tipo_all"] = df_cred_por_tipo_all

            # Store results in session_state
            st.session_state["processing_done"] = True

            st.session_state["df"] = df_contrib
            st.session_state["empresa"] = empresa
            st.session_state["raiz_cnpj"] = raiz_cnpj
            st.session_state["df_sped_fiscal"] = df_sped_fiscal
            st.session_state["C170"] = C170
            st.session_state["df_receitas_com_debito"] = df_receitas_com_debito
            st.session_state["df_receitas_sem_debito"] = df_receitas_sem_debito
            st.session_state["df_cred_por_tipo"] = df_cred_por_tipo
            st.session_state["cred_bc_total"] = cred_bc_total
            st.session_state["cred_total"] = cred_total
            st.session_state["df_serv_tomados"] = df_serv_tomados
            st.session_state["df_serv_prestados"] = df_serv_prestados
            st.session_state["df_ajuste_acresc_pis"] = df_ajuste_acresc_pis
            st.session_state["vlr_ajuste_acresc_pis"] = vlr_ajuste_acresc_pis
            st.session_state["df_ajuste_acresc_cofins"] = df_ajuste_acresc_cofins
            st.session_state["vlr_ajuste_acresc_cofins"] = vlr_ajuste_acresc_cofins
            st.session_state["df_C170_por_mod55_aliq"] = df_C170_por_mod55_aliq
            st.session_state["df_C170_COMPRA_por_item_cfop_cst_aliq_ncm"] = df_C170_COMPRA_por_item_cfop_cst_aliq_ncm
            st.session_state["df_C170_venda_por_ncm"] = df_C170_venda_por_ncm
            st.session_state["df_C175_por_mod65_aliq"] = df_C175_por_mod65_aliq
            st.session_state["df_final_venda_por_estab"] = df_final_venda_por_estab
            st.session_state["df_final_venda_por_uf_estab"] = df_final_venda_por_uf_estab
            st.session_state["df_final_venda_por_cfop"] = df_final_venda_por_cfop
            st.session_state["df_C170_saidas"] = df_C170_saidas


            st.toast("✅ Importação concluída.")
            #st.success("Arquivos importados e processados com sucesso!")
            st.header(f"Empresa: {empresa}")
            st.subheader(f"CNPJ: {raiz_cnpj[:2]}.{raiz_cnpj[2:5]}.{raiz_cnpj[5:8]}/{raiz_cnpj[8:12]}-{raiz_cnpj[-2:]}")
            #st.info("Dados já processados. Abaixo você pode ir para o Cenário Atual ou Reforma Tributária.")



# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Área 2: Compras/Entradas

elif selected_area == "Área 2: Compras/Entradas":
    st.header("Compras/Entradas")
    st.write('\n')



    # Show the tabs relevant to Área 2
    tab_entrada_resumo_pc, tab_entrada_resumo_icms, tab_entrada_industria, tab_entrada_revenda, tab_entrada_uso_consumo, tab_entrada_ativo, tab_entrada_tranf, tab_entrada_outras  = st.tabs([
        "Resumo PIS/Cofins",
        "Resumo ICMS",
        "Compra para Industrialização",
        "Compra para Revenda",
        "Compra para Uso/Consumo",
        "Compra de Ativo Imob",
        "Tansferências",
        "Outras Entradas"
    ])
    
    # ---------------------------------------------------------------------------------------------

    with tab_entrada_resumo_pc:
        
        if not st.session_state["processing_done"]:
            st.write('\n')
            st.warning("Por favor, vá para a Página Inicial e processe o arquivo primeiro.")
            st.stop()
        
        # Access the already-processed DataFrame
        df = st.session_state["df"]
        empresa = st.session_state["empresa"]
        raiz_cnpj = st.session_state["raiz_cnpj"]
        df_receitas_com_debito = st.session_state["df_receitas_com_debito"] 
        df_receitas_sem_debito = st.session_state["df_receitas_sem_debito"]
        df_cred_por_tipo = st.session_state["df_cred_por_tipo"]
        cred_bc_total = st.session_state["cred_bc_total"]
        cred_total = st.session_state["cred_total"]
        df_ajuste_acresc_pis = st.session_state["df_ajuste_acresc_pis"]
        vlr_ajuste_acresc_pis = st.session_state["vlr_ajuste_acresc_pis"]
        df_ajuste_acresc_cofins = st.session_state["df_ajuste_acresc_cofins"]
        vlr_ajuste_acresc_cofins = st.session_state["vlr_ajuste_acresc_cofins"]
        df_cred_por_tipo_all = st.session_state["df_cred_por_tipo_all"]

        # PAGE HEADER
        st.write(f"**Empresa:** {empresa}")
        st.write(f"**CNPJ:** {raiz_cnpj[:2]}.{raiz_cnpj[2:5]}.{raiz_cnpj[5:8]}/{raiz_cnpj[8:12]}-{raiz_cnpj[-2:]}")
        st.divider()

        # RECEITAS
        st.header("Débitos")
        st.write('\n')

        receita_total = df_receitas_com_debito['3'].sum() + df_receitas_sem_debito['3'].sum()
        st.write("**Receita Total:**", f"R$ {receita_total:,.2f}")

        receita_não_tributada = df_receitas_sem_debito['3'].sum()
        st.write("**Receitas Não Tributadas:**", f"R$ {receita_não_tributada:,.2f}", f"({(receita_não_tributada/receita_total*100):.0f}%)")

        if not df_receitas_sem_debito.empty:
            on = st.toggle("_Ver detalhamento das 'Receitas Não Tributadas'_", key="toggle_receitas_nao_tributadas")
            if on:
                st.dataframe(df_receitas_sem_debito, hide_index=True, use_container_width=False)


        bc_pis_cofins = df_receitas_com_debito['3'].sum()
        st.write("**Receitas Tributadas:**", f"R$ {bc_pis_cofins:,.2f}", f"({(bc_pis_cofins/receita_total*100):.0f}%)")

        if not df_receitas_com_debito.empty:
            on = st.toggle("_Ver detalhamento da 'Receitas Tributadas'_", key="toggle_receitas_tributadas")
            if on:
                st.dataframe(df_receitas_com_debito, hide_index=True, use_container_width=False)

        debito_pis_cofins = df_receitas_com_debito['11'].sum() + df_receitas_com_debito['valor_cofins'].sum()     
        st.write("**Débito Total de PIS/Cofins no período:**", f"R$ {debito_pis_cofins:,.2f}")


        st.write('\n')
        st.write('\n')
        st.write('\n')
        st.write('\n')


        # CREDITOS
        col1, col2, col3  = st.columns([1,1,1])  

        with col1:
            st.header("Créditos")
            st.write('\n')

            if df_cred_por_tipo.empty:
                st.write("**Valor Base do Crédito:**", f"R$ {cred_bc_total:,.2f}")
            else:
                st.write("**Valor Base do Crédito:**", f"R$ {cred_bc_total:,.2f}")
                on = st.toggle("_Ver detalhamento por 'Tipo de Crédito' escriturado_", key="toggle_credito_por_tipo")
                if on:
                    # Create a styled DataFrame
                    st.dataframe(df_cred_por_tipo_all, hide_index=True, use_container_width=False)

    

            if not df_ajuste_acresc_pis.empty:
                st.write("**Lançamentos de Ajuste no Crédito:**", f"R$ {(vlr_ajuste_acresc_pis + vlr_ajuste_acresc_cofins):,.2f}")
                on = st.toggle("_Ver detalhamento dos 'Ajustes no Crédito'_", key="toggle_ajustes_credito")
                if on:
                    st.write("Lançamentos de PIS")
                    st.dataframe(df_ajuste_acresc_pis, hide_index=True, use_container_width=False)
                    st.write("Lançamentos de COFINS")
                    st.dataframe(df_ajuste_acresc_cofins, hide_index=True, use_container_width=False)

            st.write("**Crédito Total de PIS/Cofins no período:**", f"R$ {cred_total:,.2f}")


        with col2:
            fig, ax = plt.subplots()
            wedges, texts = ax.pie(
                df_cred_por_tipo['BC_CRED_PIS_COFINS'],
                labels=None,
                autopct=None,
                labeldistance=1.2,
                pctdistance=1.3
            )
            ax.axis('equal')
            ax.legend(
                wedges,
                (df_cred_por_tipo['NAT_BC_CRED'].astype(str) + ' ' + df_cred_por_tipo['proporção']).tolist(),
                title="Tipo de Crédito",
                loc="center left",
                bbox_to_anchor=(1, 0, 0.5, 1)
            )
            st.pyplot(fig)

    # ---------------------------------------------------------------------------------------------

    with tab_entrada_industria:
        st.header("Compras")
        st.divider()

        if not st.session_state["processing_done"]:
            st.warning("Por favor, vá para Página Inicial e processe o arquivo primeiro.")
            st.stop()

        empresa = st.session_state["empresa"]
        raiz_cnpj = st.session_state["raiz_cnpj"]
        df = st.session_state["df"]
        
        df_C170_COMPRA_por_item_cfop_cst_aliq_ncm = st.session_state["df_C170_COMPRA_por_item_cfop_cst_aliq_ncm"]

        
        
        st.write(f"**Empresa:** {empresa}")
        st.write(f"**CNPJ:** {raiz_cnpj[:2]}.{raiz_cnpj[2:5]}.{raiz_cnpj[5:8]}/{raiz_cnpj[8:12]}-{raiz_cnpj[-2:]}")
        st.divider()

        st.dataframe(df_C170_COMPRA_por_item_cfop_cst_aliq_ncm, hide_index=True, use_container_width=False)

    # ---------------------------------------------------------------------------------------------

    # ---------------------------------------------------------------------------------------------

    # ---------------------------------------------------------------------------------------------

    # ---------------------------------------------------------------------------------------------





# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Área 3: Vendas/Saídas

elif selected_area == "Área 3: Vendas/Saídas":
    tab_saida_resumo_pc, tab_saida_resumo_icms, tab_saida_venda_producao, tab_saida_revenda, tab_saida_transf, tab_saida_outras = st.tabs([
        "Resumo PIS/Cofins",
        "Resumo ICMS",
        "Venda Produção Própria",
        "Revenda de Mercadoria",
        "Transferências",
        "Outras Saídas"
    ])

    # ---------------------------------------------------------------------------------------------

    with tab_saida_resumo_pc:
        st.header("Vendas")
        st.divider()

        if not st.session_state["processing_done"]:
            st.warning("Por favor, vá para Página Inicial e processe o arquivo primeiro.")
            st.stop()

        empresa = st.session_state["empresa"]
        raiz_cnpj = st.session_state["raiz_cnpj"]
        df_C170_por_mod55_aliq = st.session_state["df_C170_por_mod55_aliq"]
        df_C175_por_mod65_aliq = st.session_state["df_C175_por_mod65_aliq"]
        df_C170_venda_por_ncm = st.session_state["df_C170_venda_por_ncm"]
        df_final_venda_por_estab = st.session_state["df_final_venda_por_estab"]
        df_final_venda_por_uf_estab = st.session_state["df_final_venda_por_uf_estab"]
        df_final_venda_por_cfop = st.session_state["df_final_venda_por_cfop"]
        


        st.write(f"**Empresa:** {empresa}")
        st.write(f"**CNPJ:** {raiz_cnpj[:2]}.{raiz_cnpj[2:5]}.{raiz_cnpj[5:8]}/{raiz_cnpj[8:12]}-{raiz_cnpj[-2:]}")
        st.divider()

        # 1.1) NF-e (Modelo 55)


        vlr_venda_mod_55 = df_C170_por_mod55_aliq[(df_C170_por_mod55_aliq['ind_oper'] == '1') & (df_C170_por_mod55_aliq['mod_nf'] == '55')]['7'].sum()
        vlr_bc_mod_55 = df_C170_por_mod55_aliq[(df_C170_por_mod55_aliq['ind_oper'] == '1') & (df_C170_por_mod55_aliq['mod_nf'] == '55')]['26'].sum()
        vlr_pis_venda_mod_55 = df_C170_por_mod55_aliq[(df_C170_por_mod55_aliq['ind_oper'] == '1') & (df_C170_por_mod55_aliq['mod_nf'] == '55')]['30'].sum()
        vlr_cofins_venda_mod_55 = df_C170_por_mod55_aliq[(df_C170_por_mod55_aliq['ind_oper'] == '1') & (df_C170_por_mod55_aliq['mod_nf'] == '55')]['36'].sum()

        # 1.2) NFC-e (Modelo 65)
        vlr_venda_mod_65 = df_C175_por_mod65_aliq[(df_C175_por_mod65_aliq['ind_oper'] == '1') & (df_C175_por_mod65_aliq['mod_nf'] == '65')]['3'].sum()
        vlr_bc_mod_65 = df_C175_por_mod65_aliq[(df_C175_por_mod65_aliq['ind_oper'] == '1') & (df_C175_por_mod65_aliq['mod_nf'] == '65')]['6'].sum()
        vlr_pis_venda_mod_65 = df_C175_por_mod65_aliq[(df_C175_por_mod65_aliq['ind_oper'] == '1') & (df_C175_por_mod65_aliq['mod_nf'] == '65')]['10'].sum()
        vlr_cofins_venda_mod_65 = df_C175_por_mod65_aliq[(df_C175_por_mod65_aliq['ind_oper'] == '1') & (df_C175_por_mod65_aliq['mod_nf'] == '65')]['16'].sum()


        st.header("1) Vendas por Tipo de Nota Fiscal")
        st.subheader("1.1) NF-e (Modelo 55)")
        st.write("**Valor Total:**", f"R$ {vlr_venda_mod_55:,.2f}")
        st.write("**Base de Cálculo:**", f"R$ {vlr_bc_mod_55:,.2f}", f"({(vlr_bc_mod_55/vlr_venda_mod_55*100):.0f}%)")
        st.write("**PIS/Cofins:**", f"R$ {(vlr_pis_venda_mod_55 + vlr_cofins_venda_mod_55):,.2f}")
        st.write("**Alíquota Média:**", f"{((vlr_pis_venda_mod_55 + vlr_cofins_venda_mod_55)/vlr_venda_mod_55)*100:,.2f}%")
        st.write('\n')
        st.write('\n')


        st.subheader("1.2) NFC-e (Modelo 65)")
        st.write("**Valor Total:**", f"R$ {vlr_venda_mod_65:,.2f}")
        if vlr_venda_mod_65 != 0:
            perc = (vlr_bc_mod_65 / vlr_venda_mod_65) * 100
            st.write("**Base de Cálculo:**", f"R$ {vlr_bc_mod_65:,.2f}", f"({perc:.0f}%)")
            st.write("**PIS/Cofins:**", f"R$ {(vlr_pis_venda_mod_65 + vlr_cofins_venda_mod_65):,.2f}")
            st.write("**Alíquota Média:**", f"{((vlr_pis_venda_mod_65 + vlr_cofins_venda_mod_65)/vlr_venda_mod_65)*100:,.2f}%")
        else:
            st.write("**Base de Cálculo:**", "R$ 0.00 (0%)")
            st.write("**PIS/Cofins:**", "R$ 0.00")
            st.write("**Alíquota Média:**", "0%")  
        st.write('\n')
        st.write('\n')
        st.write('\n')


        st.header("2) Vendas por CFOP")
        if df_final_venda_por_cfop.empty:
            st.warning("Não foi declarado nenhum CFOP de Venda.")
        else:
            st.write('*Obs.: Foram considerados apenas CFOPs de Venda.*')
            st.dataframe(df_final_venda_por_cfop, hide_index=True, use_container_width=False)
        st.write('\n')
        st.write('\n')
        
        

        st.header("3) Vendas por NCM")
        if df_C170_venda_por_ncm.empty:
            st.warning("Não foi declarada a NCM de nenhum item vendido.")
        else:
            st.write('*Obs.: NFC-e não possuem detalhamento por NCM.*')
            st.dataframe(df_C170_venda_por_ncm, hide_index=True, use_container_width=False)
        st.write('\n')
        st.write('\n')


        st.header("4) Vendas por Estabelecimento")
        if df_final_venda_por_estab.empty:
            st.warning("Não foram declaradas as vendas por Estabelecimento.")
        else:
            st.write('*Obs.: A tabela abaixo inclui as vendas de NF-e (Mod 55) e NFC-e (Mod 65).*') 
            st.dataframe(df_final_venda_por_estab, hide_index=True, use_container_width=False)
        st.write('\n')
        st.write('\n')


        st.header("5) Vendas por UF dos Estabelecimentos")
        if df_final_venda_por_uf_estab.empty:
            st.warning("Não foram declaradas as vendas por UF dos Estabelecimentos.")
        else:
            st.write('*Obs.: A tabela abaixo inclui as vendas de NF-e (Mod 55) e NFC-e (Mod 65).*')
            st.dataframe(df_final_venda_por_uf_estab, hide_index=True, use_container_width=False)
        st.write('\n')
        st.write('\n')




# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Área 4: Serviços

elif selected_area == "Área 4: Serviços":
    tab_serv_tomados, tab_serv_prestados = st.tabs([
        "Serviços Tomados", 
        "Serviços Prestados"
    ])

    # ---------------------------------------------------------------------------------------------

    with tab_serv_tomados:
        st.header("Serviços")
        st.divider()

        if not st.session_state["processing_done"]:
            st.warning("Por favor, vá para Página Inicial e processe o arquivo primeiro.")
            st.stop()

        empresa = st.session_state["empresa"]
        raiz_cnpj = st.session_state["raiz_cnpj"]
        df_serv_tomados = st.session_state["df_serv_tomados"]
        df_serv_prestados = st.session_state["df_serv_prestados"]

        st.write(f"**Empresa:** {empresa}")
        st.write(f"**CNPJ:** {raiz_cnpj[:2]}.{raiz_cnpj[2:5]}.{raiz_cnpj[5:8]}/{raiz_cnpj[8:12]}-{raiz_cnpj[-2:]}")
        st.divider()


        vlr_total_serv_tom = df_serv_tomados['vlr_servico'].sum()

        df_serv_tomados_com_cred = df_serv_tomados[df_serv_tomados['cst_pis_cofins'].isin(["50","51","52","53","54","55","56","60","61","62","63","64","65","66"])]
        vlr_total_serv_tom_com_cred = df_serv_tomados_com_cred['vlr_servico'].sum()
        vlr_bc_serv_tom = df_serv_tomados_com_cred['pis_cofins_bc'].sum()
        vlr_pis_serv_tom = df_serv_tomados_com_cred['pis_vlr'].sum()
        vlr_cofins_serv_tom = df_serv_tomados_com_cred['cofins_vlr'].sum()

        df_serv_tomados_sem_cred = df_serv_tomados[~df_serv_tomados['cst_pis_cofins'].isin(["50","51","52","53","54","55","56","60","61","62","63","64","65","66"])]
        vlr_total_serv_tom_sem_cred = df_serv_tomados_sem_cred['vlr_servico'].sum()


        st.header("1) Serviços Tomados")

        if df_serv_tomados.empty:
            st.warning("Não foi declarado nenhum serviço tomado.")
        else:

            if df_serv_tomados_com_cred.empty:
                st.subheader("**1.1) Serviços com Crédito**")
                st.warning("Não foi declarado nenhum serviço com crédito de PIS/Cofins.")
                st.write('\n')

            else:
                st.write("**Valor Total de Serviços Tomados:**", f"R$ {vlr_total_serv_tom:,.2f}")
                st.write('\n')

                st.subheader("**1.1) Serviços com Crédito**")
                st.write("**Valor Total de Serviços Tomados com crédito:**", f"R$ {vlr_total_serv_tom_com_cred:,.2f}", f"({(vlr_total_serv_tom_com_cred/vlr_total_serv_tom*100):.0f}% do total)")
                st.write("**Valor BC de Serviços Tomados:**", f"R$ {vlr_bc_serv_tom:,.2f}")
                st.write("**Valor PIS/Cofins:**", f"R$ {(vlr_pis_serv_tom + vlr_cofins_serv_tom):,.2f}")
                on = st.toggle("_Ver detalhamento dos 'Serviços que geraram créditos'_", key="toggle_serv_tom_com_cred")
                if on:
                    st.dataframe(df_serv_tomados_com_cred, hide_index=True, use_container_width=False)
                st.write('\n')
                st.write('\n')
                

            if df_serv_tomados_sem_cred.empty:
                st.subheader("1.2) Serviços sem Crédito")
                st.warning("Não foi declarado nenhum serviço sem crédito de PIS/Cofins.")
                st.write('\n')

            else:
                st.subheader("1.2) Serviços sem Crédito")
                st.write("**Valor Total de Serviços Tomados sem crédito:**", f"R$ {vlr_total_serv_tom_sem_cred:,.2f}", f"({(vlr_total_serv_tom_sem_cred/vlr_total_serv_tom*100):.0f}% do total)")
                on = st.toggle("_Ver detalhamento dos 'Serviços que não geraram créditos'_", key="toggle_serv_tom_sem_cred")
                if on:
                    st.dataframe(df_serv_tomados_sem_cred, hide_index=True, use_container_width=False)


    # ---------------------------------------------------------------------------------------------

    with tab_serv_prestados:


        vlr_serv_prest = df_serv_prestados['vlr_servico'].sum()

        df_serv_prest_com_tribut = df_serv_prestados[(df_serv_prestados['pis_vlr'] > 0) | (df_serv_prestados['cofins_vlr'] > 0)]
        vlr_serv_com_tributo = df_serv_prest_com_tribut['vlr_servico'].sum()
        vlr_bc_serv_com_tributo = df_serv_prest_com_tribut['pis_cofins_bc'].sum()
        vlr_pis_serv_com_tributo = df_serv_prest_com_tribut['pis_vlr'].sum()
        vlr_cofins_serv_com_tributo = df_serv_prest_com_tribut['cofins_vlr'].sum()

        df_serv_prest_sem_tribut = df_serv_prestados[(df_serv_prestados['pis_vlr'] == 0) & (df_serv_prestados['cofins_vlr'] == 0)]
        vlr_serv_sem_tributo = df_serv_prest_sem_tribut['vlr_servico'].sum()


        st.header("2) Serviços Prestados")
        
        if df_serv_prestados.empty:
            st.warning("Não foi declarado nenhum serviço prestado.")
        else:
            if df_serv_prest_com_tribut.empty:
                st.warning("Não foi declarado nenhum serviço com débito de PIS/Cofins.")
                st.write('\n')

            else:
                st.write("**Valor Total de Serviços Prestados:**", f"R$ {vlr_serv_prest:,.2f}")
                st.write('\n')

                st.subheader("**2.1) Serviços com Dédito**")
                st.write("**Valor Total de Serviços Prestados com débito:**", f"R$ {vlr_serv_com_tributo:,.2f}", f"({(vlr_serv_com_tributo/vlr_serv_prest*100):.0f}% do total)")
                st.write("**Valor BC de Serviços Prestados:**", f"R$ {vlr_bc_serv_com_tributo:,.2f}")
                st.write("**Valor PIS/Cofins:**", f"R$ {(vlr_pis_serv_com_tributo + vlr_cofins_serv_com_tributo):,.2f}")
                on = st.toggle("_Ver detalhamento dos 'Serviços que geraram déditos'_", key="toggle_serv_prest_com_debito")
                if on:
                    st.dataframe(df_serv_prest_com_tribut, hide_index=True, use_container_width=False)
                st.write('\n')
                st.write('\n')
                
            if df_serv_prest_sem_tribut.empty:
                st.subheader("2.2) Serviços sem Dédito")
                st.warning("Não foi declarado nenhum serviço sem débito de PIS/Cofins.")
                st.write('\n')

            else:
                st.subheader("2.2) Serviços sem Dédito")
                st.write("**Valor Total de Serviços Prestados sem dédito:**", f"R$ {vlr_serv_sem_tributo:,.2f}", f"({(vlr_serv_sem_tributo/vlr_serv_prest*100):.0f}% do total)")
                on = st.toggle("_Ver detalhamento dos 'Serviços que não geraram déditos'_", key="toggle_serv_prest_sem_debito")
                if on:
                    st.dataframe(df_serv_prest_sem_tribut, hide_index=True, use_container_width=False)



# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Área 5: Reforma Tributária


elif selected_area == "Área 5: Reforma Tributária":
    st.header("Reforma Tributária")
    st.write('\n')
    
    empresa = st.session_state["empresa"]
    raiz_cnpj = st.session_state["raiz_cnpj"]
    st.write(f"**Empresa:** {empresa}")
    st.write(f"**CNPJ:** {raiz_cnpj[:2]}.{raiz_cnpj[2:5]}.{raiz_cnpj[5:8]}/{raiz_cnpj[8:12]}-{raiz_cnpj[-2:]}")
    st.divider()

    #(tab_reforma,) = st.tabs(["Resumo"])

    #with tab_reforma:       

    # if not st.session_state["processing_done"]:
    #    st.warning("Por favor, vá para Página Inicial e processe o arquivo primeiro.")
    #    st.stop()

    
    df_ecd = st.session_state["df_ecd"]
    df_sped_fiscal = st.session_state["df_sped_fiscal"]
    df_saidas_reforma = st.session_state["df_saidas_reforma"]

    df_cred_por_tipo_all = st.session_state["df_cred_por_tipo_all"]
    df_cred_por_tipo = st.session_state["df_cred_por_tipo"]
    cred_bc_total = st.session_state["cred_bc_total"]
    cred_total = st.session_state["cred_total"]
    df_ajuste_acresc_pis = st.session_state["df_ajuste_acresc_pis"]
    vlr_ajuste_acresc_pis = st.session_state["vlr_ajuste_acresc_pis"]
    df_ajuste_acresc_cofins = st.session_state["df_ajuste_acresc_cofins"]
    vlr_ajuste_acresc_cofins = st.session_state["vlr_ajuste_acresc_cofins"]
    
    C100_SF = st.session_state["C100_SF"]
    C170_SF = st.session_state["C170_SF"]
    C190_SF = st.session_state["C190_SF"]
    C197_SF = st.session_state["C197_SF"]
    C590_SF = st.session_state["C590_SF"]
    D190_SF = st.session_state["D190_SF"]
    D590_SF = st.session_state["D590_SF"]
    E110_SF = st.session_state["E110_SF"]
    E111_SF = st.session_state["E111_SF"]
    E116_SF = st.session_state["E116_SF"]
    REG_1900_SF = st.session_state["REG_1900_SF"]
    REG_1920_SF = st.session_state["REG_1920_SF"]
    REG_1921_SF = st.session_state["REG_1921_SF"]
    REG_1925_SF = st.session_state["REG_1925_SF"]
    REG_1926_SF = st.session_state["REG_1926_SF"]

    REG_I155_ECD = st.session_state["REG_I155_ECD"]
    REG_I355_ECD = st.session_state["REG_I355_ECD"]

    df_serv_prestados = st.session_state["df_serv_prestados"]


    
    
    ####################################################################
    # CONTABILIDADE
    ####################################################################


    #st.header("ECD - Contas de Resultado (para teste)")
    #df_ecd_groupby_plano_ref = (
    #    REG_I155_ECD
    #    .loc[REG_I155_ECD['CTA_REF'].astype('string').str.startswith('3', na=False)]
    #    .groupby(['ano', 'CTA_REF', 'CTA_REF_DESCR'], dropna=False)[['VL_DEB', 'VL_CRED']]
    #    .sum()
    #    .round(2)
    #    .sort_values(by='CTA_REF', ascending=True)
    #    .reset_index()
    #)

    #if not df_ecd_groupby_plano_ref.empty:
    #    on = st.toggle("_Clique para ver a tabela I155_", key="toggle_ecd_contas_table")
    #    if on:
    #        st.dataframe(df_ecd_groupby_plano_ref, hide_index=True, use_container_width=False)
    




    ####################################################################
    # DEBITOS
    ####################################################################


    st.header("1) Análise Débitos")


    
    # -------------------------------------------------------------------
    st.subheader("**> Resumo por CFOPs de Venda**")
    df_C170_SF_cfop = df_saidas_reforma[df_saidas_reforma['cfop_descr'].str.lower().fillna('').str.startswith("venda de")].groupby(['CFOP', 'cfop_descr'], dropna=False)[['VL_ITEM', 'VL_ICMS', 'VL_IPI', 'VL_PIS', 'VL_COFINS']].sum().round(2).sort_values(by='VL_ITEM', ascending=False).reset_index()
    st.dataframe(df_C170_SF_cfop, hide_index=True, use_container_width=False)

    # -------------------------------------------------------------------
    st.subheader("**> Base de Vendas (NCMxCFOPxCSTxALIQ)**")
    df_vendas_reforma = df_saidas_reforma[df_saidas_reforma['cfop_descr'].str.lower().fillna('').str.startswith("venda de")].groupby(['uf_empresa','part_uf','CFOP', 'cfop_descr','ncm', 'CST_ICMS', 'ALIQ_ICMS','CST_PIS','ALIQ_PIS','ALIQ_COFINS'], dropna=False)[['VL_ITEM', 'VL_DESC', 'VL_ICMS', 'VL_ICMS_ST', 'VL_IPI', 'VL_PIS', 'VL_COFINS']].sum().round(2).sort_values(by='VL_ITEM', ascending=False).reset_index()
    st.dataframe(df_vendas_reforma, hide_index=True, use_container_width=False)

    # -------------------------------------------------------------------
    st.subheader("> Base Completa de Saídas")
    st.dataframe(df_saidas_reforma, hide_index=True, use_container_width=False)
    
    st.divider()

    # -----  Venda de Produção Própria --------------------------------
    df_vendas_prod_reforma = df_saidas_reforma[df_saidas_reforma['cfop_descr'].str.lower().fillna('').str.startswith("venda de produção")]
    df_vendas_prod_reforma_por_cfop = df_vendas_prod_reforma.groupby(['ie_estab','CFOP'], dropna=False)[['VL_ITEM', 'VL_ICMS', 'VL_PIS', 'VL_COFINS', 'IBS', 'CBS']].sum().round(2).sort_values(by='VL_ITEM', ascending=False).reset_index() 

    st.subheader("**1.1) Venda de Produção Própria**")
    venda_prod_prop_icms = df_vendas_prod_reforma['VL_ICMS'].sum()
    venda_prod_prop_ibs = df_vendas_prod_reforma['IBS'].sum()
    st.markdown(f"""
        <div style="line-height:1.45; margin: 0;">
            <strong>Valor Total:</strong> R$ {df_vendas_prod_reforma['VL_ITEM'].sum():,.2f}<br>
            <strong>ICMS Destacado:</strong> R$ {venda_prod_prop_icms:,.2f}<br>
            <strong>PIS/Cofins Destacado:</strong> R$ {(df_vendas_prod_reforma['VL_PIS'].sum() + df_vendas_prod_reforma['VL_COFINS'].sum()):,.2f}<br>
            <strong>IBS Projetado:</strong> R$ {venda_prod_prop_ibs:,.2f}<br>
            <strong>CBS Projetado:</strong> R$ {df_vendas_prod_reforma['CBS'].sum():,.2f}
        </div>
        """, unsafe_allow_html=True)
    st.markdown('#####')

    if not df_vendas_prod_reforma.empty:
        st.dataframe(df_vendas_prod_reforma_por_cfop, 
                     hide_index=True, 
                     use_container_width=False, 
                     column_config={
                        "VL_ITEM": st.column_config.NumberColumn("Valor", format="%.2f")
                    })
        st.markdown('#####')




    col1, col2, col3 = st.columns([1,1,2])

    with col1:
        fig, ax = plt.subplots()
        bars = ax.bar(
            ['ICMS Destacado', 'IBS a ser destacado'],
            [(venda_prod_prop_icms), venda_prod_prop_ibs],
            color=["#2099d2", '#0eff93']  
        )
        ax.set_ylabel('Valor (R$)')
        ax.set_title('ICMS vs IBS')
        ax.tick_params(left=False, bottom=False)
        for side in ('top','right','left'):
            ax.spines[side].set_visible(False)
        ax.bar_label(bars, fmt='R$ %.2f')  

        st.pyplot(fig)

    with col2:
        fig, ax = plt.subplots()
        bars = ax.bar(
            ['PIS/Cofins Destacado', 'CBS a ser destacada'],
            [(df_vendas_prod_reforma['VL_PIS'].sum() + df_vendas_prod_reforma['VL_COFINS'].sum()), df_vendas_prod_reforma['CBS'].sum()],
            color=['#2099d2', "#0eff93"]  
        )
        ax.set_ylabel('Valor (R$)')
        ax.set_title('PIS/Cofins vs CBS')
        ax.tick_params(left=False, bottom=False)
        for side in ('top','right','left'):
            ax.spines[side].set_visible(False)
        ax.bar_label(bars, fmt='R$ %.2f')  

        st.pyplot(fig)



    # -----  Revendas --------------------------------------------------

    st.markdown('##')
    st.subheader("**1.2) Revendas**")
    df_reforma_revendas = df_saidas_reforma[df_saidas_reforma['cfop_descr'].str.lower().fillna('').str.startswith("venda de mercadoria adquirida")].groupby(['ie_estab', 'CFOP','cfop_descr'], dropna=False)[['VL_ITEM','VL_ICMS','VL_PIS','VL_COFINS','IBS','CBS']].sum().round(2).sort_values(by='VL_ITEM', ascending=False).reset_index()
    

    vlr_total_revendas = df_reforma_revendas['VL_ITEM'].sum()
    vlr_icms_revendas = df_reforma_revendas['VL_ICMS'].sum()
    vlr_piscofins_revendas = (df_reforma_revendas['VL_PIS'].sum() + df_reforma_revendas['VL_COFINS'].sum())
    vlr_ibs_revendas = df_reforma_revendas['IBS'].sum()
    vlr_cbs_revendas = df_reforma_revendas['CBS'].sum()

    st.markdown(f"""
        <div style="line-height:1.45; margin: 0;">
            <strong>Valor Total:</strong> R$ {vlr_total_revendas:,.2f}<br>
            <strong>ICMS Destacado:</strong> R$ {vlr_icms_revendas:,.2f}<br>
            <strong>PIS/Cofins Destacado:</strong> R$ {vlr_piscofins_revendas:,.2f}<br>
            <strong>IBS Projetado:</strong> R$ {vlr_ibs_revendas:,.2f}<br>
            <strong>CBS Projetado:</strong> R$ {vlr_cbs_revendas:,.2f}
        </div>
        """, unsafe_allow_html=True)
    st.markdown('#####')


    if not df_vendas_prod_reforma.empty:
        st.dataframe(df_reforma_revendas, hide_index=True, use_container_width=False)
        st.markdown('#####')

            
    
    col1, col2, col3 = st.columns([1,1,2])

    with col1:
        fig, ax = plt.subplots()
        bars = ax.bar(
            ['ICMS Destacado', 'IBS a ser destacado'],
            [df_reforma_revendas['VL_ICMS'].sum(), df_reforma_revendas['IBS'].sum()],
            color=['#2099d2', '#0eff93']  
        )
        ax.set_ylabel('Valor (R$)')
        ax.set_title('ICMS vs IBS')
        ax.tick_params(left=False, bottom=False)
        for side in ('top','right','left'):
            ax.spines[side].set_visible(False)
        ax.bar_label(bars, fmt='R$ %.2f')  

        st.pyplot(fig)

    with col2:
        fig, ax = plt.subplots()
        bars = ax.bar(
            ['PIS/Cofins Destacado', 'CBS a ser destacada'],
            [(df_reforma_revendas['VL_PIS'].sum() + df_reforma_revendas['VL_COFINS'].sum()), df_reforma_revendas['CBS'].sum()],
            color=['#2099d2', '#0eff93'] 
        )
        ax.set_ylabel('Valor (R$)')
        ax.set_title('PIS/Cofins vs CBS')
        ax.tick_params(left=False, bottom=False)
        for side in ('top','right','left'):
            ax.spines[side].set_visible(False)
        ax.bar_label(bars, fmt='R$ %.2f')  

        st.pyplot(fig)



    # -----  Transferências de Produção Própria --------------------------------
    
    #st.markdown('##')
    #st.subheader("**1.3) Transferência de Produção Própria**")
    #df_sf_C190_transf_prod_cfop = C190_SF[C190_SF['cfop_descr'].str.lower().fillna('').str.startswith("transferência de produção")].groupby(['ie_estab', '3','cfop_descr'], dropna=False)[['5', '7']].sum().round(2).sort_values(by='5', ascending=False).reset_index()
    #st.dataframe(df_sf_C190_transf_prod_cfop, hide_index=True, use_container_width=False)


    # ------ Outras Saídas -----------------------------------------------------------

    #st.markdown('##')
    #st.subheader("**1.4) Outras Saídas de Mercadoria**")
    #df_reforma_outras_saidas = df_saidas_reforma[~df_saidas_reforma['cfop_descr'].str.lower().fillna('').str.startswith((
    #                "venda de produção",
    #                "transferência de produção",
    #                "venda de mercadoria adquirida"))].groupby(['ie_estab', 'CFOP','cfop_descr'], dropna=False)[['VL_ITEM','VL_ICMS','VL_PIS','VL_COFINS','IBS','CBS']].sum().round(2).sort_values(by='VL_ITEM', ascending=False).reset_index()
    #st.dataframe(df_reforma_outras_saidas, hide_index=True, use_container_width=False)





    # ------- Serviços Prestados -------------------------------------------------

    st.markdown('##')
    st.subheader("**1.3) Serviços Prestados**")
    df_serv_prestados['IBS'] = (df_serv_prestados['vlr_servico'] * 0.187).round(2)
    df_serv_prestados['CBS'] = (df_serv_prestados['vlr_servico'] * 0.093).round(2)

    vlr_total_serv_prestados = df_serv_prestados['vlr_servico'].sum()
    vlr_iss_serv_prestados = df_serv_prestados['iss'].sum()
    vlr_piscofins_serv_prestados = (df_serv_prestados['pis_vlr'].sum() + df_serv_prestados['cofins_vlr'].sum())
    vlr_ibs_serv_prestados = df_serv_prestados['IBS'].sum()
    vlr_cbs_serv_prestados = df_serv_prestados['CBS'].sum()

    st.markdown(f"""
    <div style="line-height:1.45; margin: 0;">
      <strong>Valor Total:</strong> R$ {vlr_total_serv_prestados:,.2f}<br>
      <strong>ISS Destacado:</strong> R$ {vlr_iss_serv_prestados:,.2f}<br>
      <strong>PIS/Cofins Destacado:</strong> R$ {vlr_piscofins_serv_prestados:,.2f}<br>
      <strong>IBS Projetado:</strong> R$ {vlr_ibs_serv_prestados:,.2f}<br>
      <strong>CBS Projetada:</strong> R$ {vlr_cbs_serv_prestados:,.2f}
    </div>
    """, unsafe_allow_html=True)
    st.markdown('#####')

    if not df_vendas_prod_reforma.empty:
        st.dataframe(df_serv_prestados, hide_index=True, use_container_width=False)
        st.markdown('#####')
    

    col1, col2, col3 = st.columns([1,1,2])

    with col1:
        fig, ax = plt.subplots()
        bars = ax.bar(
            ['ISS Destacado', 'IBS a ser destacado'],
            [vlr_iss_serv_prestados, vlr_ibs_serv_prestados],
            color=['#2099d2', '#0eff93']  
        )
        ax.set_ylabel('Valor (R$)')
        ax.set_title('ISS vs IBS')
        ax.tick_params(left=False, bottom=False)
        for side in ('top','right','left'):
            ax.spines[side].set_visible(False)
        ax.bar_label(bars, fmt='R$ %.2f')  
        st.pyplot(fig)

    with col2:
        fig, ax = plt.subplots()
        bars = ax.bar(
            ['PIS/Cofins Destacado', 'CBS a ser destacada'],
            [vlr_piscofins_serv_prestados, vlr_cbs_serv_prestados],
            color=['#2099d2', '#0eff93']  
        )
        ax.set_ylabel('Valor (R$)')
        ax.set_title('PIS/Cofins vs CBS')
        ax.tick_params(left=False, bottom=False)
        for side in ('top','right','left'):
            ax.spines[side].set_visible(False)
        ax.bar_label(bars, fmt='R$ %.2f') 
        st.pyplot(fig)


    



    ####################################################################
    # CREDITOS
    ####################################################################

    st.divider()
    st.header("2) Análise de Créditos")

    # -------------------------------------------------------------------
    #st.subheader("**2.1) Insumos ZFM**")
    #df_C197_agrupado = C197_SF.groupby(['ind_oper', '1', '2', 'cod_aj_doc', 'ncm', '6'], dropna=False)[['5', '7', '8']].sum().round(2).sort_values(by='5', ascending=False).reset_index()
    #df_C197_agrupado_entradas = df_C197_agrupado[df_C197_agrupado['ind_oper'] == '0'].iloc[:,1:]
    #df_C197_agrupado_saidas = df_C197_agrupado[df_C197_agrupado['ind_oper'] == '1'].iloc[:,1:]
    #st.dataframe(df_C197_agrupado_entradas, hide_index=True, use_container_width=False)  
    
    # -------------------------------------------------------------------
    st.subheader("**2.1) Base de Entradas C170 Sped Fiscal (apenas CFOP de compra)**")
    df_C170_SF_compras = C170_SF[C170_SF['cfop_descr'].str.lower().fillna('').str.startswith("compra")]
    st.dataframe(df_C170_SF_compras, hide_index=True, use_container_width=False)

    # -------------------------------------------------------------------
    st.subheader("**2.2) Compras (NCMxCFOPxCSTxALIQ)**")
    df_C170_SF_por_ncm_cfop_cst = C170_SF[C170_SF['cfop_descr'].str.lower().fillna('').str.startswith("compra")].groupby(['uf_estab','part_uf','CFOP', 'cfop_descr','ncm', 'CST_ICMS', 'ALIQ_ICMS','CST_PIS','ALIQ_PIS','ALIQ_COFINS'], dropna=False)[['VL_ITEM', 'VL_DESC', 'VL_ICMS', 'VL_ICMS_ST', 'VL_IPI', 'VL_PIS', 'VL_COFINS']].sum().round(2).sort_values(by='VL_ITEM', ascending=False).reset_index()
    st.dataframe(df_C170_SF_por_ncm_cfop_cst, hide_index=True, use_container_width=False)

    # -------------------------------------------------------------------
    st.subheader("**2.3) Resumo por CFOPs de Compra**")
    df_C170_SF_cfop = C170_SF[C170_SF['cfop_descr'].str.lower().fillna('').str.startswith("compra")].groupby(['CFOP', 'cfop_descr'], dropna=False)[['VL_ITEM', 'VL_ICMS', 'VL_IPI', 'VL_PIS', 'VL_COFINS']].sum().round(2).sort_values(by='VL_ITEM', ascending=False).reset_index()
    st.dataframe(df_C170_SF_cfop, hide_index=True, use_container_width=False)

    # -------------------------------------------------------------------
    st.subheader("2.4) Bloco M")
    st.write('\n')

    if df_cred_por_tipo.empty:
        st.write("**Valor Base do Crédito:**", f"R$ {cred_bc_total:,.2f}")
        st.write("**Crédito Total de PIS/Cofins no período:**", f"R$ {cred_total:,.2f}")
    else:
        st.write("**Valor Base do Crédito:**", f"R$ {cred_bc_total:,.2f}")
        st.write("**Crédito Total de PIS/Cofins no período:**", f"R$ {cred_total:,.2f}")
        st.dataframe(df_cred_por_tipo_all, hide_index=True, use_container_width=False)


    if not df_ajuste_acresc_pis.empty:
        st.write("**Lançamentos de Ajuste no Crédito:**", f"R$ {(vlr_ajuste_acresc_pis + vlr_ajuste_acresc_cofins):,.2f}")
        on = st.toggle("_Ver detalhamento dos 'Ajustes no Crédito'_", key="toggle_ajustes_credito")
        if on:
            st.write("Lançamentos de PIS")
            st.dataframe(df_ajuste_acresc_pis, hide_index=True, use_container_width=False)
            st.write("Lançamentos de COFINS")
            st.dataframe(df_ajuste_acresc_cofins, hide_index=True, use_container_width=False)


    
    
    # -------------------------------------------------------------------
    st.subheader("2.5) ECD I355 com Plano de Contas Referencial")
    st.write('_A coluna "CREDITAVEL" pode ser editada._')
    st.write('\n')

    # Build base I355 table (one-time), including initial 'CREDITAVEL' guess
    df_REG_I355_ECD = REG_I355_ECD.loc[
        REG_I355_ECD['VL_CTA'].fillna(0) != 0,
        ['ano', 'COD_CTA', 'CTA_DESCR', 'CTA_REF', 'CTA_REF_DESCR', 'VL_CTA']
    ].sort_values(by='CTA_REF', ascending=True).reset_index(drop=True)

    # ensure numeric
    df_REG_I355_ECD['VL_CTA'] = pd.to_numeric(df_REG_I355_ECD['VL_CTA'], errors='coerce')

    # seed CREDITAVEL from your reference list
    df_REG_I355_ECD['CREDITAVEL'] = np.where(
        df_REG_I355_ECD['CTA_REF'].isin(cta_ref_creditavel),
        "sim",
        "não"
    )

    # Create a deterministic row key for stable identity in the editor
    if "ROW_KEY" not in df_REG_I355_ECD.columns:
        df_REG_I355_ECD["ROW_KEY"] = (
            df_REG_I355_ECD["ano"].astype(str) + "|" +
            df_REG_I355_ECD["COD_CTA"].astype(str) + "|" +
            df_REG_I355_ECD["CTA_REF"].astype(str)
        ).astype("string")

    # Keep a single source of truth in session_state
    if "ecd_i355_df" not in st.session_state:
        st.session_state["ecd_i355_df"] = df_REG_I355_ECD.copy()

    state_df = st.session_state["ecd_i355_df"]

    # Safety: make sure required columns exist
    if "CREDITAVEL" not in state_df.columns:
        state_df["CREDITAVEL"] = "sim"
    if "ROW_KEY" not in state_df.columns:
        state_df["ROW_KEY"] = (
            state_df["ano"].astype(str) + "|" +
            state_df["COD_CTA"].astype(str) + "|" +
            state_df["CTA_REF"].astype(str)
        ).astype("string")
    state_df["VL_CTA"] = pd.to_numeric(state_df["VL_CTA"], errors="coerce")

    # ----- Build VIEW (computed columns) from current state; do not mutate state directly
    view_df = state_df.copy()
    mask_live = view_df["CREDITAVEL"].astype(str).eq("sim")
    view_df["CREDITO_CBS"] = np.where(mask_live, view_df["VL_CTA"] * 0.093, np.nan)
    view_df["CREDITO_IBS"] = np.where(mask_live, view_df["VL_CTA"] * 0.187, np.nan)
    view_df[["CREDITO_CBS", "CREDITO_IBS"]] = view_df[["CREDITO_CBS", "CREDITO_IBS"]].round(2)
    view_df["STATUS"] = np.where(mask_live, "🧮 calculado", "—")

    # Use stable index (older Streamlit doesn't accept row_key=...)
    view_df = view_df.set_index("ROW_KEY", drop=False)

    # Render the editor; capture edited result
    edited_ecd = st.data_editor(
        view_df,
        key="editor_ecd_i355",
        hide_index=True,
        use_container_width=False,
        column_order=[
            "ano", "COD_CTA", "CTA_DESCR", "CTA_REF", "CTA_REF_DESCR", "VL_CTA",
            "CREDITAVEL", "CREDITO_CBS", "CREDITO_IBS", "STATUS"
        ],  # hide ROW_KEY from UI (still available via index)
        column_config={
            "CREDITAVEL":  st.column_config.SelectboxColumn(
                "CREDITAVEL",
                options=["sim", "não"],
                required=True,
                help="Marque 'sim' para habilitar os créditos."
            ),
            "VL_CTA":      st.column_config.NumberColumn(
                "VL_CTA (R$)", format="%.2f", disabled=True,
                help="Valor contábil base; apenas leitura."
            ),
            "CREDITO_CBS": st.column_config.NumberColumn(
                "CREDITO_CBS 🔒", format="%.2f", disabled=True,
                help="Calculado automaticamente: VL_CTA × 9,3% quando CREDITAVEL = 'sim'."
            ),
            "CREDITO_IBS": st.column_config.NumberColumn(
                "CREDITO_IBS 🔒", format="%.2f", disabled=True,
                help="Calculado automaticamente: VL_CTA × 18,7% quando CREDITAVEL = 'sim'."
            ),
            "STATUS": st.column_config.TextColumn(
                "STATUS", disabled=True,
                help="Indicador visual: '🧮 calculado' quando a linha é creditável."
            ),
        },
    )

    # If user changed CREDITAVEL, sync it back by the index (ROW_KEY) and refresh immediately
    try:
        if isinstance(edited_ecd, pd.DataFrame) and "CREDITAVEL" in edited_ecd.columns:
            dest = st.session_state["ecd_i355_df"]
            dest_idx = dest.set_index("ROW_KEY", drop=False)

            before = dest_idx["CREDITAVEL"].astype(str).copy()
            # Align assignment by index; keep existing where not present
            dest_idx.loc[edited_ecd.index, "CREDITAVEL"] = edited_ecd["CREDITAVEL"].astype(str)

            # Write back to session state preserving original row order
            st.session_state["ecd_i355_df"]["CREDITAVEL"] = dest_idx.loc[dest["ROW_KEY"], "CREDITAVEL"].values

            if not before.equals(dest_idx["CREDITAVEL"].astype(str)):
                st.rerun()
    except Exception:
        pass

    # Totals from the recomputed VIEW
    tot_cbs = float(view_df["CREDITO_CBS"].sum(skipna=True))
    tot_ibs = float(view_df["CREDITO_IBS"].sum(skipna=True))
    st.write("**Crédito CBS (calculado automaticamente):**", f"{tot_cbs:,.2f}")
    st.write("**Crédito IBS (calculado automaticamente):**", f"{tot_ibs:,.2f}")



















    #st.dataframe(C197_SF, hide_index=True, use_container_width=False)
    #st.dataframe(C190_SF, hide_index=True, use_container_width=False)
    #st.dataframe(C590_SF, hide_index=True, use_container_width=False)
    #st.dataframe(D190_SF, hide_index=True, use_container_width=False)
    #st.dataframe(D590_SF, hide_index=True, use_container_width=False)
    #st.dataframe(E110_SF, hide_index=True, use_container_width=False)
    #st.dataframe(E111_SF, hide_index=True, use_container_width=False)
    #st.dataframe(E116_SF, hide_index=True, use_container_width=False)
    #st.dataframe(REG_1900_SF, hide_index=True, use_container_width=False)
    #st.dataframe(REG_1920_SF, hide_index=True, use_container_width=False)
    #st.dataframe(REG_1921_SF, hide_index=True, use_container_width=False)
    #st.dataframe(REG_1925_SF, hide_index=True, use_container_width=False)
    #st.dataframe(REG_1926_SF, hide_index=True, use_container_width=False)
