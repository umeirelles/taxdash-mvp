import csv
import io

import numpy as np
import pandas as pd
import streamlit as st


@st.cache_data
def load_and_process_data(uploaded_file):
    """Load one or more SPED Contribuições files and build stable, cross-file unique IDs."""
    if uploaded_file is None or (isinstance(uploaded_file, list) and len(uploaded_file) == 0):
        st.error("Por favor, carregue pelo menos um arquivo .txt")
        st.stop()

    delimiter = '|'
    encoding = 'latin-1'
    column_names = [str(i) for i in range(40)]
    parent_reg_codes = [
        "0000", "0140", "A100", "C100", "C180", "C190", "C380", "C400", "C500", "C600", "C800", "D100", "D500",
        "F100", "F120", "F130", "F150", "F200", "F500", "F600", "F700", "F800", "I100", "M100", "M200",
        "M300", "M350", "M400", "M500", "M600", "M700", "M800", "P100", "P200", "1010", "1020", "1050",
        "1100", "1200", "1300", "1500", "1600", "1700", "1800", "1900"
    ]

    files = uploaded_file if isinstance(uploaded_file, list) else [uploaded_file]
    dfs = []

    for i, f in enumerate(files):
        f.seek(0)
        reader = pd.read_csv(
            f,
            header=None,
            delimiter=delimiter,
            names=column_names,
            encoding=encoding,
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
        df_temp = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=column_names)
        if not df_temp.empty and '1' in df_temp.columns:
            mask_all = df_temp['1'].astype(str).eq('9999')
            if mask_all.any():
                cut = int(np.argmax(mask_all.to_numpy()))
                df_temp = df_temp.iloc[:cut+1].copy()
        if df_temp.empty:
            continue

        data_efd = df_temp.loc[0, '7'][2:] if pd.notna(df_temp.loc[0, '7']) else None
        cnpj_header = df_temp.loc[0, '9'] if pd.notna(df_temp.loc[0, '9']) else None

        if '0' in df_temp.columns:
            df_temp.drop(columns=['0'], inplace=True)

        prefix_file = str(i)
        prefix_periodo = str(data_efd) if data_efd is not None else "NA"
        prefix_cnpj = str(cnpj_header) if cnpj_header is not None else "NA"
        row_no = pd.Series(df_temp.index, index=df_temp.index).astype(str).str.zfill(7)
        composite_id = (prefix_file + "|" + prefix_periodo + "|" + prefix_cnpj + "|" + row_no).astype("string")

        df_temp.insert(0, 'cnpj', None)
        df_temp.insert(0, 'periodo', data_efd)
        df_temp.insert(0, 'id_pai', None)
        df_temp.insert(0, 'id', composite_id)

        df_temp.loc[df_temp['1'].isin(["0000", "C001", "D001", "M001", "1001"]), 'cnpj'] = cnpj_header
        df_temp.loc[df_temp['1'] == "0140", 'cnpj'] = df_temp['4']
        df_temp.loc[df_temp['1'].isin(["A010", "C010", "D010", "F010", "I010", "P010"]), 'cnpj'] = df_temp['2']

        df_temp.loc[df_temp['1'].isin(parent_reg_codes), 'id_pai'] = df_temp['id']
        df_temp['id_pai'] = df_temp['id_pai'].ffill()
        df_temp['cnpj'] = df_temp['cnpj'].ffill()

        dfs.append(df_temp)

    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=column_names + ['id', 'id_pai', 'periodo', 'cnpj'])
    if df.empty:
        st.error("Falha ao ler o SPED Contribuições: nenhum arquivo válido ou todas as linhas foram descartadas como inválidas.")
        st.stop()

    return df


@st.cache_data
def load_and_process_sped_fiscal(uploaded_files):
    if not uploaded_files:
        st.error("Por favor, carregue no mínimo um arquivo .txt")
        st.stop()

    delimiter = '|'
    encoding = 'latin-1'
    column_names = [str(i) for i in range(42)]
    parent_reg_codes = [
        "0000",
        "C100", "C300", "C350", "C400", "C495", "C500", "C600", "C700", "C800", "C860",
        "D100", "D300", "D350", "D400", "D500", "D600", "D695", "D700", "D750",
        "E100", "E200", "E300", "E500",
        "G110",
        "H005",
        "K100", "K200", "K210", "K220", "K230", "K250", "K260", "K270", "K280", "K290", "K300",
        "1100", "1200", "1300", "1350", "1390", "1400", "1500", "1600", "1601", "1700", "1800", "1900", "1960", "1970", "1980"
    ]

    dfs = []
    for i, single_file in enumerate(uploaded_files):
        try:
            single_file.seek(0)
            df_temp = pd.read_csv(
                single_file,
                header=None,
                delimiter=delimiter,
                names=column_names,
                low_memory=False,
                encoding=encoding,
                dtype=str,
                engine="c",
                on_bad_lines="skip"
            )
            if not df_temp.empty and '1' in df_temp.columns:
                mask_all = df_temp['1'].astype(str).eq('9999')
                if mask_all.any():
                    cut = int(np.argmax(mask_all.to_numpy()))
                    df_temp = df_temp.iloc[:cut+1].copy()
        except pd.errors.ParserError:
            single_file.seek(0)
            reader = pd.read_csv(
                single_file,
                header=None,
                delimiter=delimiter,
                names=column_names,
                encoding=encoding,
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
            df_temp = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=column_names)
        if df_temp.empty:
            continue

        data_efd = df_temp.loc[0, '4'][2:] if pd.notna(df_temp.loc[0, '4']) else None
        cnpj_estab = df_temp.loc[0, '7'] if pd.notna(df_temp.loc[0, '7']) else None
        ie_estab = df_temp.loc[0, '10'] if pd.notna(df_temp.loc[0, '10']) else None
        uf_estab = df_temp.loc[0, '9'] if pd.notna(df_temp.loc[0, '9']) else None

        if '0' in df_temp.columns:
            df_temp.drop(columns=['0'], inplace=True)

        prefix_file = str(i)
        prefix_periodo = str(data_efd) if data_efd is not None else "NA"
        prefix_cnpj = str(cnpj_estab) if cnpj_estab is not None else "NA"
        row_no = pd.Series(df_temp.index, index=df_temp.index).astype(str).str.zfill(7)
        composite_id = (prefix_file + "|" + prefix_periodo + "|" + prefix_cnpj + "|" + row_no).astype("string")

        df_temp.insert(0, 'uf_estab', uf_estab)
        df_temp.insert(0, 'ie_estab', ie_estab)
        df_temp.insert(0, 'cnpj_estab', cnpj_estab)
        df_temp.insert(0, 'periodo', data_efd)
        df_temp.insert(0, 'id_pai', None)
        df_temp.insert(0, 'id', composite_id)

        df_temp.loc[df_temp['1'].isin(parent_reg_codes), 'id_pai'] = df_temp['id']
        df_temp['id_pai'] = df_temp['id_pai'].ffill()
        df_temp['cnpj_estab'] = df_temp['cnpj_estab'].ffill()

        dfs.append(df_temp)

    df_sped_fiscal = pd.concat(dfs, ignore_index=True)
    if "id" in df_sped_fiscal.columns:
        df_sped_fiscal = df_sped_fiscal.drop_duplicates(subset=["id"], keep="last")

    return df_sped_fiscal


def _process_single_ecd_file(uploaded_file, file_index):
    delimiter = '|'
    encoding = 'latin-1'
    column_names = [str(i) for i in range(40)]
    parent_reg_codes = [
        "0000", "0001", "C001", "C040", "C050", "C150", "C600", "I001", "I010", "I050", "I150"
    ]

    try:
        uploaded_file.seek(0)
        reader = pd.read_csv(
            uploaded_file,
            header=None,
            delimiter=delimiter,
            names=column_names,
            encoding=encoding,
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
            delimiter=delimiter,
            names=column_names,
            encoding=encoding,
            dtype=str,
            engine="python",
            on_bad_lines="skip",
            chunksize=200_000,
            quoting=csv.QUOTE_NONE
        )

    dfs = []
    in_skip = False
    after_resume = False

    try:
        for chunk in reader:
            codes = chunk['1'].astype(str)
            mask_i200 = codes.eq('I200')
            mask_i350 = codes.eq('I350')

            if not in_skip and not after_resume:
                has200 = mask_i200.any()
                has350 = mask_i350.any()

                if not has200 and not has350:
                    dfs.append(chunk)
                    continue

                if has200 and not has350:
                    first200 = int(np.argmax(mask_i200.to_numpy()))
                    if first200 > 0:
                        dfs.append(chunk.iloc[:first200])
                    in_skip = True
                    continue

                if not has200 and has350:
                    dfs.append(chunk)
                    after_resume = True
                    continue

                first200 = int(np.argmax(mask_i200.to_numpy()))
                first350 = int(np.argmax(mask_i350.to_numpy()))
                if first200 < first350:
                    if first200 > 0:
                        dfs.append(chunk.iloc[:first200])
                    dfs.append(chunk.iloc[first350:])
                    after_resume = True
                    in_skip = False
                else:
                    dfs.append(chunk)
                    after_resume = True
                    in_skip = False
                continue

            if in_skip and not after_resume:
                if mask_i350.any():
                    first350 = int(np.argmax(mask_i350.to_numpy()))
                    dfs.append(chunk.iloc[first350:])
                    in_skip = False
                    after_resume = True
                continue

            if after_resume:
                dfs.append(chunk)
                continue
    except Exception:
        uploaded_file.seek(0)
        raw = uploaded_file.read()
        text = raw.decode(encoding, errors='ignore') if isinstance(raw, (bytes, bytearray)) else str(raw)

        trimmed_lines = []
        for ln in text.splitlines():
            trimmed_lines.append(ln)
            if ln.startswith('|I990|'):
                break
        buf = io.StringIO('\n'.join(trimmed_lines))

        df_fallback = pd.read_csv(
            buf,
            header=None,
            delimiter=delimiter,
            names=column_names,
            encoding=encoding,
            dtype=str,
            engine='python',
            on_bad_lines='skip',
            quoting=csv.QUOTE_NONE
        )

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

    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=column_names)

    if not df.empty and '1' in df.columns:
        mask_all = df['1'].astype(str).eq('I990')
        if mask_all.any():
            cut = int(np.argmax(mask_all.to_numpy()))
            df = df.iloc[:cut+1].copy()

    ano_ecd = df.loc[0, '3'][4:] if pd.notna(df.loc[0, '3']) else None
    cnpj = df.loc[0, '6'] if pd.notna(df.loc[0, '6']) else None

    if '0' in df.columns:
        df.drop(columns=['0'], inplace=True)

    df.insert(0, 'cnpj', cnpj)
    df.insert(0, 'ano', ano_ecd)
    df.insert(0, 'id_pai', None)
    row_no = pd.Series(df.index, index=df.index).astype(str)
    composite_id = (str(file_index) + "|" + row_no).astype("string")
    df.insert(0, 'id', composite_id)

    df.loc[df['1'].isin(parent_reg_codes), 'id_pai'] = df['id']
    df['id_pai'] = df['id_pai'].ffill()

    return df


@st.cache_data
def load_and_process_ecd(uploaded_files):
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
