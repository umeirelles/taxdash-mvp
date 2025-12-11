import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from dicts import *
import csv
import io
from taxdash import load_and_process_data, load_and_process_sped_fiscal, load_and_process_ecd
from taxdash import config, processors

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('future.no_silent_downcasting', True)

# Configure Streamlit page
st.set_page_config(page_title="TaxDash", page_icon=":material/code:", layout="wide")


# --------------------------------------------------------------------------------------------------------------------
# HELPER FUNCTIONS FOR TABLE DISPLAY AND DOWNLOAD
# --------------------------------------------------------------------------------------------------------------------

@st.cache_data
def convert_df_to_csv(df):
    """Cache CSV conversion to avoid regenerating on every rerun."""
    return df.to_csv(index=False, sep=';', decimal=',').encode('utf-8-sig')

def display_table_with_download(df, filename, max_rows=1000):
    """Display table with row limit and cached download button."""
    total_rows = len(df)

    if total_rows > max_rows:
        st.warning(f"⚠️ Exibindo {max_rows:,} de {total_rows:,} linhas. Baixe o CSV para dados completos.")
        st.dataframe(df.head(max_rows), hide_index=True)
    else:
        st.dataframe(df, hide_index=True)

    csv = convert_df_to_csv(df)
    st.download_button("📥 Baixar CSV", csv, filename, "text/csv")


# layout dos datadrames
def style_df(df):
    return df.style.set_properties(**{'background-color': "#d619b0", 'color': "#d0d619"})


# --------------------------------------------------------------------------------------------------------------------
# FUNÇOES PARA FILTRAR REGISTROS ORIGINAIS SPED FISCAL E CONTRIBUIÇOES
# --------------------------------------------------------------------------------------------------------------------

# filtro blocos SPED CONTRIBUICOES



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

    # Vectorized IBS calculation - much faster than row-wise apply
    C170_SC["cfop_str"] = C170_SC["11"].astype(str)
    # Create lookup key as tuple (prefix, cfop)
    lookup_keys = list(zip(C170_SC["ie_estab_prefix"], C170_SC["cfop_str"]))
    # Convert dict to Series for vectorized lookups
    ibs_rates_series = pd.Series(regras_ibs_saidas_zfm)
    # Try exact lookup
    ibs_rates = pd.Series(lookup_keys).map(ibs_rates_series)
    # For NaN values, try fallback with wildcard
    fallback_keys = list(zip(['*'] * len(C170_SC), C170_SC["cfop_str"]))
    ibs_rates_fallback = pd.Series(fallback_keys).map(ibs_rates_series)
    ibs_rates = ibs_rates.fillna(ibs_rates_fallback).fillna(0.0)
    C170_SC["IBS"] = ibs_rates.values * C170_SC["7"]

    # Vectorized CBS calculation - much faster than row-wise apply
    cbs_rates_series = pd.Series(regras_cbs_saidas_zfm)
    # Try exact lookup
    cbs_rates = pd.Series(lookup_keys).map(cbs_rates_series)
    # For NaN values, try fallback with wildcard
    cbs_rates_fallback = pd.Series(fallback_keys).map(cbs_rates_series)
    cbs_rates = cbs_rates.fillna(cbs_rates_fallback).fillna(0.0)
    C170_SC["CBS"] = cbs_rates.values * C170_SC["7"]

    # Clean up temporary column
    C170_SC = C170_SC.drop(columns=["cfop_str"])

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

    # Welcome section
    st.markdown("""
    ### Bem-vindo ao TaxDash
    Sistema de análise de dados fiscais SPED para suporte à Reforma Tributária brasileira.
    """)
    st.markdown('##')

    col1, col2 = st.columns([3, 2])

    with col2:
        # Information panel
        st.markdown("### :material/info: Informações")

        st.info("""
        **Arquivos obrigatórios:**
        - SPED Contribuições
        - SPED Fiscal

        **Arquivo opcional:**
        - ECD (Escrituração Contábil)
        """)

        st.markdown("---")

        st.markdown("### :material/analytics: O que você pode fazer")
        st.markdown("""
        - Análise de compras e vendas
        - Cálculo de créditos PIS/COFINS
        - Simulação da Reforma Tributária
        - Análise de operações por CFOP/NCM
        - Relatórios por estabelecimento e UF
        """)

    with col1:
        # ECD Section
        with st.container():
            st.markdown("### :material/account_balance: ECD - Escrituração Contábil Digital")
            uploaded_sped_ecd_file = st.file_uploader(
                label="Selecione um ou mais arquivos ECD",
                type="txt",
                accept_multiple_files=True,
                help="Arquivos ECD em formato .txt",
                key="ecd_uploader"
            )
            if uploaded_sped_ecd_file:
                st.success(f"✓ {len(uploaded_sped_ecd_file)} arquivo(s) selecionado(s)")

        st.markdown('###')

        # SPED Contribuições Section
        with st.container():
            st.markdown("### :material/receipt_long: SPED Contribuições (PIS/COFINS)")
            uploaded_contrib_file = st.file_uploader(
                label="Selecione um ou mais arquivos SPED Contribuições",
                type="txt",
                accept_multiple_files=True,
                help="Arquivos SPED Contribuições (PIS/COFINS) em formato .txt",
                key="contrib_uploader"
            )
            if uploaded_contrib_file:
                st.success(f"✓ {len(uploaded_contrib_file)} arquivo(s) selecionado(s)")

        st.markdown('###')

        # SPED Fiscal Section
        with st.container():
            st.markdown("### :material/inventory_2: SPED Fiscal (ICMS/IPI)")
            uploaded_sped_fiscal_files = st.file_uploader(
                label="Selecione um ou mais arquivos SPED Fiscal do mesmo período",
                type="txt",
                accept_multiple_files=True,
                help="Arquivos SPED Fiscal (ICMS/IPI) em formato .txt",
                key="fiscal_uploader"
            )
            if uploaded_sped_fiscal_files:
                st.success(f"✓ {len(uploaded_sped_fiscal_files)} arquivo(s) selecionado(s)")

        st.markdown('###')

        if st.button("🚀 Processar Arquivos", type='primary', use_container_width=True):

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
            REG_0150_SF, REG_0200_SF = processors.Bloco_0_Sped_Fiscal(df_sped_fiscal)
            C100_SF, C170_SF, C190_SF, C197_SF, C590_SF = processors.Bloco_C_Sped_Fiscal(df_sped_fiscal, REG_0150_SF, REG_0200_SF, cfop_cod_descr, cod_uf, cst_icms, sped_fiscal_tab_5_3_AM)
            D190_SF, D590_SF = processors.Bloco_D_Sped_Fiscal(df_sped_fiscal, cst_icms, cfop_cod_descr)
            E110_SF, E111_SF, E116_SF = processors.Bloco_E_Sped_Fiscal(df_sped_fiscal, sped_fiscal_tab_5_1_1, sped_fiscal_tab_5_4, sped_fiscal_cod_receita_AM)
            REG_1900_SF, REG_1920_SF, REG_1921_SF, REG_1925_SF, REG_1926_SF = processors.Bloco_1_Sped_Fiscal(df_sped_fiscal, sped_fiscal_tab_ind_apur_icms_AM, sped_fiscal_tab_5_1_1, sped_fiscal_tab_5_2, sped_fiscal_tab_5_4, sped_fiscal_cod_receita_AM)

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
            reg_0140, reg_0150, reg_0200 = processors.Bloco_0(df_contrib)
            M100, M105, M110, M210, M400, M510, M610 = processors.Bloco_M(df_contrib, tab_4_3_7, tab_4_3_8, tab_4_3_5, cst_pis_cofins)
            (df_cred_por_tipo_all, df_cred_por_tipo, cred_bc_total, cred_total, df_receitas_com_debito, 
            df_receitas_sem_debito, df_ajuste_acresc_pis, vlr_ajuste_acresc_pis, 
            df_ajuste_acresc_cofins, vlr_ajuste_acresc_cofins) = processors.bloco_M_filtering(M105, M110, M210, M400, M510, M610, tab_4_3_7)
            A100, A170 = processors.Bloco_A(df_contrib, reg_0140, reg_0150, reg_0200, cod_uf)
            df_serv_tomados, df_serv_prestados = processors.bloco_A_filtering(A100, A170)
            C100, C170, C175, C181, C185 = processors.Bloco_C(df_contrib, reg_0140, reg_0150, reg_0200, cfop_cod_descr, cod_uf, cst_pis_cofins)
            (df_C170_por_mod55_aliq, df_C170_COMPRA_por_item_cfop_cst_aliq_ncm,
            df_C175_por_mod65_aliq, df_C170_venda_por_ncm, df_final_venda_por_estab,
            df_final_venda_por_uf_estab, df_final_venda_por_cfop, df_C170_saidas) = processors.bloco_C_filtering(C100, C170, C175, C181, C185)
            #-----------------------------------------------------
            # importar ECD
            #-----------------------------------------------------
            if uploaded_sped_ecd_file and len(uploaded_sped_ecd_file) > 0:
                ph_ecd_msg.info(f"Importando ECD ({len(uploaded_sped_ecd_file)} arquivo(s))…")
                ph_ecd_prog.progress(10, text="Lendo ECD…")
            else:
                ph_ecd_prog.progress(100, text="Nenhum arquivo ECD selecionado.")
                ph_ecd_msg.warning("ECD não informado — etapa ignorada.")

            # Process the files (only if ECD provided)
            if uploaded_sped_ecd_file and len(uploaded_sped_ecd_file) > 0:
                df_ecd = load_and_process_ecd(uploaded_sped_ecd_file)
                ph_ecd_prog.progress(100, text="ECD importado com sucesso.")
                ph_ecd_msg.success(":material/task_alt: ECD concluído")

                REG_I050_ECD, REG_I051_ECD, REG_I150_ECD, REG_I155_ECD, REG_I200_ECD, REG_I250_ECD, REG_I355_ECD = processors.Bloco_I_ECD(df_ecd, PLANO_CONTAS_REF)

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

    # Check if files have been processed
    if not st.session_state.get("processing_done", False):
        st.warning("⚠️ Por favor, importe e processe os arquivos SPED na Área 1 primeiro.")
        st.stop()

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
                display_table_with_download(df_receitas_sem_debito, "receitas_nao_tributadas.csv")


        bc_pis_cofins = df_receitas_com_debito['3'].sum()
        st.write("**Receitas Tributadas:**", f"R$ {bc_pis_cofins:,.2f}", f"({(bc_pis_cofins/receita_total*100):.0f}%)")

        if not df_receitas_com_debito.empty:
            on = st.toggle("_Ver detalhamento da 'Receitas Tributadas'_", key="toggle_receitas_tributadas")
            if on:
                display_table_with_download(df_receitas_com_debito, "receitas_tributadas.csv")

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
                    display_table_with_download(df_cred_por_tipo_all, "credito_por_tipo.csv")

    

            if not df_ajuste_acresc_pis.empty:
                st.write("**Lançamentos de Ajuste no Crédito:**", f"R$ {(vlr_ajuste_acresc_pis + vlr_ajuste_acresc_cofins):,.2f}")
                on = st.toggle("_Ver detalhamento dos 'Ajustes no Crédito'_", key="toggle_ajustes_credito")
                if on:
                    st.write("Lançamentos de PIS")
                    display_table_with_download(df_ajuste_acresc_pis, "ajustes_pis.csv")
                    st.write("Lançamentos de COFINS")
                    display_table_with_download(df_ajuste_acresc_cofins, "ajustes_cofins.csv")

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

        display_table_with_download(df_C170_COMPRA_por_item_cfop_cst_aliq_ncm, "compras_por_item.csv")

    # ---------------------------------------------------------------------------------------------

    # ---------------------------------------------------------------------------------------------

    # ---------------------------------------------------------------------------------------------

    # ---------------------------------------------------------------------------------------------





# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Área 3: Vendas/Saídas

elif selected_area == "Área 3: Vendas/Saídas":
    # Check if files have been processed
    if not st.session_state.get("processing_done", False):
        st.warning("⚠️ Por favor, importe e processe os arquivos SPED na Área 1 primeiro.")
        st.stop()

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
            display_table_with_download(df_final_venda_por_cfop, "vendas_por_cfop.csv")
        st.write('\n')
        st.write('\n')



        st.header("3) Vendas por NCM")
        if df_C170_venda_por_ncm.empty:
            st.warning("Não foi declarada a NCM de nenhum item vendido.")
        else:
            st.write('*Obs.: NFC-e não possuem detalhamento por NCM.*')
            display_table_with_download(df_C170_venda_por_ncm, "vendas_por_ncm.csv")
        st.write('\n')
        st.write('\n')


        st.header("4) Vendas por Estabelecimento")
        if df_final_venda_por_estab.empty:
            st.warning("Não foram declaradas as vendas por Estabelecimento.")
        else:
            st.write('*Obs.: A tabela abaixo inclui as vendas de NF-e (Mod 55) e NFC-e (Mod 65).*')
            display_table_with_download(df_final_venda_por_estab, "vendas_por_estab.csv")
        st.write('\n')
        st.write('\n')


        st.header("5) Vendas por UF dos Estabelecimentos")
        if df_final_venda_por_uf_estab.empty:
            st.warning("Não foram declaradas as vendas por UF dos Estabelecimentos.")
        else:
            st.write('*Obs.: A tabela abaixo inclui as vendas de NF-e (Mod 55) e NFC-e (Mod 65).*')
            display_table_with_download(df_final_venda_por_uf_estab, "vendas_por_uf_estab.csv")
        st.write('\n')
        st.write('\n')




# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Área 4: Serviços

elif selected_area == "Área 4: Serviços":
    # Check if files have been processed
    if not st.session_state.get("processing_done", False):
        st.warning("⚠️ Por favor, importe e processe os arquivos SPED na Área 1 primeiro.")
        st.stop()

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
                    display_table_with_download(df_serv_tomados_com_cred, "servicos_tomados_com_credito.csv")
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
                    display_table_with_download(df_serv_tomados_sem_cred, "servicos_tomados_sem_credito.csv")


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
                    display_table_with_download(df_serv_prest_com_tribut, "servicos_prestados_com_debito.csv")
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
                    display_table_with_download(df_serv_prest_sem_tribut, "servicos_prestados_sem_debito.csv")



# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Área 5: Reforma Tributária


elif selected_area == "Área 5: Reforma Tributária":
    st.header("Reforma Tributária")
    st.write('\n')

    # Check if files have been processed
    if not st.session_state.get("processing_done", False):
        st.warning("⚠️ Por favor, importe e processe os arquivos SPED na Área 1 primeiro.")
        st.stop()

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
    display_table_with_download(df_C170_SF_cfop, "resumo_cfop_venda.csv")

    # -------------------------------------------------------------------
    st.subheader("**> Base de Vendas (NCMxCFOPxCSTxALIQ)**")
    df_vendas_reforma = df_saidas_reforma[df_saidas_reforma['cfop_descr'].str.lower().fillna('').str.startswith("venda de")].groupby(['uf_empresa','part_uf','CFOP', 'cfop_descr','ncm', 'CST_ICMS', 'ALIQ_ICMS','CST_PIS','ALIQ_PIS','ALIQ_COFINS'], dropna=False)[['VL_ITEM', 'VL_DESC', 'VL_ICMS', 'VL_ICMS_ST', 'VL_IPI', 'VL_PIS', 'VL_COFINS']].sum().round(2).sort_values(by='VL_ITEM', ascending=False).reset_index()
    display_table_with_download(df_vendas_reforma, "base_vendas_reforma.csv")

    # -------------------------------------------------------------------
    st.subheader("> Base Completa de Saídas")
    st.write(f"Total de registros: {len(df_saidas_reforma):,}")
    display_table_with_download(df_saidas_reforma, "base_completa_saidas.csv")
    
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
        display_table_with_download(df_vendas_prod_reforma_por_cfop, "venda_producao_propria.csv")
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
        display_table_with_download(df_reforma_revendas, "revendas.csv")
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
        display_table_with_download(df_serv_prestados, "servicos_prestados_reforma.csv")
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
    st.write(f"Total de registros: {len(df_C170_SF_compras):,}")
    display_table_with_download(df_C170_SF_compras, "base_completa_compras.csv")

    # -------------------------------------------------------------------
    st.subheader("**2.2) Compras (NCMxCFOPxCSTxALIQ)**")
    df_C170_SF_por_ncm_cfop_cst = C170_SF[C170_SF['cfop_descr'].str.lower().fillna('').str.startswith("compra")].groupby(['uf_estab','part_uf','CFOP', 'cfop_descr','ncm', 'CST_ICMS', 'ALIQ_ICMS','CST_PIS','ALIQ_PIS','ALIQ_COFINS'], dropna=False)[['VL_ITEM', 'VL_DESC', 'VL_ICMS', 'VL_ICMS_ST', 'VL_IPI', 'VL_PIS', 'VL_COFINS']].sum().round(2).sort_values(by='VL_ITEM', ascending=False).reset_index()
    display_table_with_download(df_C170_SF_por_ncm_cfop_cst, "compras_ncm_cfop_cst.csv")

    # -------------------------------------------------------------------
    st.subheader("**2.3) Resumo por CFOPs de Compra**")
    df_C170_SF_cfop = C170_SF[C170_SF['cfop_descr'].str.lower().fillna('').str.startswith("compra")].groupby(['CFOP', 'cfop_descr'], dropna=False)[['VL_ITEM', 'VL_ICMS', 'VL_IPI', 'VL_PIS', 'VL_COFINS']].sum().round(2).sort_values(by='VL_ITEM', ascending=False).reset_index()
    display_table_with_download(df_C170_SF_cfop, "resumo_cfop_compra.csv")

    # -------------------------------------------------------------------
    st.subheader("2.4) Bloco M")
    st.write('\n')

    if df_cred_por_tipo.empty:
        st.write("**Valor Base do Crédito:**", f"R$ {cred_bc_total:,.2f}")
        st.write("**Crédito Total de PIS/Cofins no período:**", f"R$ {cred_total:,.2f}")
    else:
        st.write("**Valor Base do Crédito:**", f"R$ {cred_bc_total:,.2f}")
        st.write("**Crédito Total de PIS/Cofins no período:**", f"R$ {cred_total:,.2f}")
        display_table_with_download(df_cred_por_tipo_all, "credito_por_tipo_reforma.csv")


    if not df_ajuste_acresc_pis.empty:
        st.write("**Lançamentos de Ajuste no Crédito:**", f"R$ {(vlr_ajuste_acresc_pis + vlr_ajuste_acresc_cofins):,.2f}")
        on = st.toggle("_Ver detalhamento dos 'Ajustes no Crédito'_", key="toggle_ajustes_credito_reforma")
        if on:
            st.write("Lançamentos de PIS")
            display_table_with_download(df_ajuste_acresc_pis, "ajustes_pis_reforma.csv")
            st.write("Lançamentos de COFINS")
            display_table_with_download(df_ajuste_acresc_cofins, "ajustes_cofins_reforma.csv")


    
    
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
