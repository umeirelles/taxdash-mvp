import csv
import io

import numpy as np
import pandas as pd
import streamlit as st

from . import config


@st.cache_data
def load_and_process_data(uploaded_file):
    """Load one or more SPED Contribuições files and build stable, cross-file unique IDs."""
    if uploaded_file is None or (isinstance(uploaded_file, list) and len(uploaded_file) == 0):
        st.error("Por favor, carregue pelo menos um arquivo .txt")
        st.stop()

    delimiter = config.DELIMITER
    encoding = config.ENCODING
    column_names = [str(i) for i in range(config.COLUMN_COUNT_CONTRIB)]
    parent_reg_codes = config.PARENT_REG_CONTRIB

    files = uploaded_file if isinstance(uploaded_file, list) else [uploaded_file]
    dfs = []

    for i, f in enumerate(files):
        f.seek(0)
        # Pre-scan file to find |9999| (end of valid SPED data, before digital certificate)
        # This allows C engine to avoid binary certificate data
        file_content = f.read()
        if isinstance(file_content, bytes):
            file_content = file_content.decode(encoding, errors='ignore')

        # Find the line that starts with |9999| - this marks end of valid data
        lines = file_content.split('\n')
        end_idx = None
        for idx, line in enumerate(lines):
            if line.startswith('|9999|'):
                end_idx = idx
                break

        if end_idx is not None:
            # Truncate to valid data only (up to and including |9999|)
            valid_content = '\n'.join(lines[:end_idx+1])
        else:
            valid_content = file_content

        # Now read the cleaned content with fast C engine
        from io import StringIO
        f_clean = StringIO(valid_content)

        try:
            reader = pd.read_csv(
                f_clean,
                header=None,
                delimiter=delimiter,
                names=column_names,
                encoding=None,  # Already decoded
                dtype=str,
                engine="c",
                on_bad_lines="skip",
                chunksize=config.CHUNK_SIZE
            )
            parts = []
            for chunk in reader:
                parts.append(chunk)
        except (pd.errors.ParserError, ValueError) as e:
            # Fallback to Python engine if C engine still fails
            f_clean.seek(0)
            reader = pd.read_csv(
                f_clean,
                header=None,
                delimiter=delimiter,
                names=column_names,
                encoding=None,
                dtype=str,
                engine="python",
                on_bad_lines="skip",
                chunksize=config.CHUNK_SIZE,
                quoting=csv.QUOTE_NONE
            )
            parts = []
            for chunk in reader:
                parts.append(chunk)

        df_temp = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=column_names)
        if not df_temp.empty and '1' in df_temp.columns:
            mask_all = df_temp['1'].astype(str).eq('9999')
            if mask_all.any():
                cut = int(np.argmax(mask_all.to_numpy()))
                df_temp = df_temp.iloc[:cut+1].copy()
        if df_temp.empty:
            continue

        # Bounds check: ensure df_temp has at least one row before accessing row 0
        if len(df_temp) == 0:
            continue

        data_efd = df_temp.loc[0, '7'][2:] if pd.notna(df_temp.loc[0, '7']) else None
        cnpj_header = df_temp.loc[0, '9'] if pd.notna(df_temp.loc[0, '9']) else None

        if '0' in df_temp.columns:
            df_temp.drop(columns=['0'], inplace=True)

        # Vectorized ID generation with numpy (faster than pandas string ops)
        prefix_file = str(i)
        prefix_periodo = str(data_efd) if data_efd is not None else "NA"
        prefix_cnpj = str(cnpj_header) if cnpj_header is not None else "NA"
        prefix_combined = f"{prefix_file}|{prefix_periodo}|{prefix_cnpj}|"
        row_no_array = np.char.zfill(np.arange(len(df_temp)).astype(str), 7)
        composite_id = np.char.add(prefix_combined, row_no_array)

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

    # Convert register code to categorical for memory efficiency (50 unique values, millions of rows)
    if '1' in df.columns:
        df['1'] = df['1'].astype('category')

    return df


@st.cache_data
def load_and_process_sped_fiscal(uploaded_files):
    if not uploaded_files:
        st.error("Por favor, carregue no mínimo um arquivo .txt")
        st.stop()

    delimiter = config.DELIMITER
    encoding = config.ENCODING
    column_names = [str(i) for i in range(config.COLUMN_COUNT_FISCAL)]
    parent_reg_codes = config.PARENT_REG_FISCAL

    dfs = []
    for i, single_file in enumerate(uploaded_files):
        single_file.seek(0)
        # Pre-scan file to find |9999| (end of valid SPED data, before digital certificate)
        file_content = single_file.read()
        if isinstance(file_content, bytes):
            file_content = file_content.decode(encoding, errors='ignore')

        # Find the line that starts with |9999| - this marks end of valid data
        lines = file_content.split('\n')
        end_idx = None
        for idx, line in enumerate(lines):
            if line.startswith('|9999|'):
                end_idx = idx
                break

        if end_idx is not None:
            # Truncate to valid data only (up to and including |9999|)
            valid_content = '\n'.join(lines[:end_idx+1])
        else:
            valid_content = file_content

        # Now read the cleaned content with fast C engine
        from io import StringIO
        f_clean = StringIO(valid_content)

        try:
            df_temp = pd.read_csv(
                f_clean,
                header=None,
                delimiter=delimiter,
                names=column_names,
                low_memory=False,
                encoding=None,  # Already decoded
                dtype=str,
                engine="c",
                on_bad_lines="skip"
            )
        except pd.errors.ParserError:
            # Fallback to Python engine if C engine still fails
            f_clean.seek(0)
            reader = pd.read_csv(
                f_clean,
                header=None,
                delimiter=delimiter,
                names=column_names,
                encoding=None,
                dtype=str,
                engine="python",
                on_bad_lines="skip",
                chunksize=config.CHUNK_SIZE,
                quoting=csv.QUOTE_NONE
            )
            parts = []
            for chunk in reader:
                parts.append(chunk)
            df_temp = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=column_names)
        if df_temp.empty:
            continue

        # Bounds check: ensure df_temp has at least one row before accessing row 0
        if len(df_temp) == 0:
            continue

        data_efd = df_temp.loc[0, '4'][2:] if pd.notna(df_temp.loc[0, '4']) else None
        cnpj_estab = df_temp.loc[0, '7'] if pd.notna(df_temp.loc[0, '7']) else None
        ie_estab = df_temp.loc[0, '10'] if pd.notna(df_temp.loc[0, '10']) else None
        uf_estab = df_temp.loc[0, '9'] if pd.notna(df_temp.loc[0, '9']) else None

        if '0' in df_temp.columns:
            df_temp.drop(columns=['0'], inplace=True)

        # Vectorized ID generation with numpy (faster than pandas string ops)
        prefix_file = str(i)
        prefix_periodo = str(data_efd) if data_efd is not None else "NA"
        prefix_cnpj = str(cnpj_estab) if cnpj_estab is not None else "NA"
        prefix_combined = f"{prefix_file}|{prefix_periodo}|{prefix_cnpj}|"
        row_no_array = np.char.zfill(np.arange(len(df_temp)).astype(str), 7)
        composite_id = np.char.add(prefix_combined, row_no_array)

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

    # Convert register code to categorical for memory efficiency (50 unique values, millions of rows)
    if '1' in df_sped_fiscal.columns:
        df_sped_fiscal['1'] = df_sped_fiscal['1'].astype('category')

    return df_sped_fiscal


def _process_single_ecd_file(uploaded_file, file_index):
    delimiter = config.DELIMITER
    encoding = config.ENCODING
    column_names = [str(i) for i in range(config.COLUMN_COUNT_ECD)]
    parent_reg_codes = config.PARENT_REG_ECD

    # Pre-scan file to find |9999| (end of valid ECD data, before digital certificate)
    uploaded_file.seek(0)
    file_content = uploaded_file.read()
    if isinstance(file_content, bytes):
        file_content = file_content.decode(encoding, errors='ignore')

    # Find the line that starts with |9999| - this marks end of valid data
    lines = file_content.split('\n')
    end_idx = None
    for idx, line in enumerate(lines):
        if line.startswith('|9999|'):
            end_idx = idx
            break

    if end_idx is not None:
        # Truncate to valid data only (up to and including |9999|)
        valid_content = '\n'.join(lines[:end_idx+1])
    else:
        valid_content = file_content

    # Now read the cleaned content with fast C engine
    from io import StringIO
    f_clean = StringIO(valid_content)

    try:
        reader = pd.read_csv(
            f_clean,
            header=None,
            delimiter=delimiter,
            names=column_names,
            encoding=None,  # Already decoded
            dtype=str,
            engine="c",
            on_bad_lines="skip",
            chunksize=config.CHUNK_SIZE
        )
    except (pd.errors.ParserError, UnicodeDecodeError, csv.Error) as e:
        # Fallback to Python engine if C engine fails
        f_clean.seek(0)
        reader = pd.read_csv(
            f_clean,
            header=None,
            delimiter=delimiter,
            names=column_names,
            encoding=None,
            dtype=str,
            engine="python",
            on_bad_lines="skip",
            chunksize=config.CHUNK_SIZE,
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
    except (pd.errors.ParserError, UnicodeDecodeError, csv.Error, StopIteration) as e:
        # Fallback: read entire file at once if chunk processing fails
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

    # Bounds check: ensure df has at least one row before accessing row 0
    if len(df) == 0:
        return pd.DataFrame(columns=['id', 'id_pai', 'ano', 'cnpj'] + [str(i) for i in range(1, 40)])

    ano_ecd = df.loc[0, '3'][4:] if pd.notna(df.loc[0, '3']) else None
    cnpj = df.loc[0, '6'] if pd.notna(df.loc[0, '6']) else None

    if '0' in df.columns:
        df.drop(columns=['0'], inplace=True)

    # Build deterministic, cross-file row id (vectorized with numpy for speed)
    # id := "<fileidx>|<ano>|<cnpj>|<row_no_zfilled>"
    prefix_file = str(file_index)
    prefix_ano = str(ano_ecd) if ano_ecd is not None else "NA"
    prefix_cnpj = str(cnpj) if cnpj is not None else "NA"
    prefix_combined = f"{prefix_file}|{prefix_ano}|{prefix_cnpj}|"
    row_no_array = np.char.zfill(np.arange(len(df)).astype(str), 7)
    composite_id = np.char.add(prefix_combined, row_no_array)

    df.insert(0, 'cnpj', cnpj)
    df.insert(0, 'ano', ano_ecd)
    df.insert(0, 'id_pai', None)
    df.insert(0, 'id', composite_id)

    df.loc[df['1'].isin(parent_reg_codes), 'id_pai'] = df['id']
    df['id_pai'] = df['id_pai'].ffill()

    return df


@st.cache_data
def load_and_process_ecd(uploaded_files):
    """Load one or more ECD files and build stable, cross-file unique IDs.
    Backwards-compatible: accepts a single file or a list of files.
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

    # Convert register code to categorical for memory efficiency (50 unique values, millions of rows)
    if '1' in df_ecd.columns:
        df_ecd['1'] = df_ecd['1'].astype('category')

    return df_ecd
