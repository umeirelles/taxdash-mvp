"""
Data processing functions for SPED files.

This module contains all the Bloco_* and filtering functions that extract
and transform data from SPED Contribuições, SPED Fiscal, and ECD files.
"""

import pandas as pd
from taxdash import config


def Bloco_0(df):
    """Extract registro blocks 0140, 0150, and 0200."""
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


def Bloco_M(df, tab_4_3_7, tab_4_3_8, tab_4_3_5, cst_pis_cofins):
    """Extract and process M-block registers."""
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


def Bloco_A(df, reg_0140, reg_0150, reg_0200, cod_uf):
    """Extract and process A-block registers."""
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


def Bloco_C(df, reg_0140, reg_0150, reg_0200, cfop_cod_descr, cod_uf, cst_pis_cofins):
    """Extract and process C-block registers."""
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
    """Extract SPED Fiscal register blocks 0150 and 0200."""
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


def Bloco_C_Sped_Fiscal(df, REG_0150_SF, REG_0200_SF, cfop_cod_descr, cod_uf, cst_icms, sped_fiscal_tab_5_3_AM):
    """Extract and process C-block registers from SPED Fiscal."""
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


def Bloco_D_Sped_Fiscal(df, cst_icms, cfop_cod_descr):
    """Extract and process D-block registers from SPED Fiscal."""
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


def Bloco_E_Sped_Fiscal(df, sped_fiscal_tab_5_1_1, sped_fiscal_tab_5_4, sped_fiscal_cod_receita_AM):
    """Extract and process E-block registers from SPED Fiscal."""
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


def Bloco_1_Sped_Fiscal(df, sped_fiscal_tab_ind_apur_icms_AM, sped_fiscal_tab_5_1_1, sped_fiscal_tab_5_2, sped_fiscal_tab_5_4, sped_fiscal_cod_receita_AM):
    """Extract and process 1-block registers from SPED Fiscal."""
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

def Bloco_I_ECD(df, PLANO_CONTAS_REF):
    """Extract and process I-block registers from ECD."""
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

    # For multiple monthly ECD files: deduplicate I050 by COD_CTA (column '6') before mapping
    # Chart of Accounts should be consistent across periods, so we keep the first occurrence
    REG_I050_unique = REG_I050_ECD.drop_duplicates(subset=['6'], keep='first')
    REG_I050_mapping = REG_I050_unique.set_index('6')

    REG_I155_ECD.insert(8, 'CTA_DESCR', REG_I155_ECD['2'].map(REG_I050_mapping['8']))       # incluido a descrição da conta contabil
    REG_I155_ECD.insert(9, 'CTA_REF', REG_I155_ECD['2'].map(REG_I050_mapping['CTA_REF']))       # incluido COD_CTA_REF
    REG_I155_ECD.insert(10, 'CTA_REF_DESCR', REG_I155_ECD['2'].map(REG_I050_mapping['CTA_REF_DESCR']))       # incluido descrição da COD_CTA_REF
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
    REG_I355_ECD.insert(7, 'CTA_DESCR', REG_I355_ECD['2'].map(REG_I050_mapping['8']))       # incluido a descrição da conta contabil
    REG_I355_ECD.insert(8, 'CTA_REF', REG_I355_ECD['2'].map(REG_I050_mapping['CTA_REF']))       # incluido COD_CTA_REF
    REG_I355_ECD.insert(9, 'CTA_REF_DESCR', REG_I355_ECD['2'].map(REG_I050_mapping['CTA_REF_DESCR']))       # incluido descrição da COD_CTA_REF
    REG_I355_ECD = REG_I355_ECD.rename(columns={
        '1': 'REG',
        '2': 'COD_CTA',
        '3': 'COD_CCUS',
        '4': 'VL_CTA',
        '5': 'IND_DC'
    })

    return REG_I050_ECD, REG_I051_ECD, REG_I150_ECD, REG_I155_ECD, REG_I200_ECD, REG_I250_ECD, REG_I355_ECD


# --------------------------------------------------------------------------------------------------------------------
# FILTERING FUNCTIONS FOR ANALYSIS
# --------------------------------------------------------------------------------------------------------------------

def bloco_M_filtering(M105, M110, M210, M400, M510, M610, tab_4_3_7):
    """Filter and analyze M-block data for SPED Contribuições."""
    #M110
    df_ajuste_acresc_pis = M110[M110['2'] == '1'][['4', 'tipo_ajuste', '6', '3', '5', '7']].sort_values(by='3', ascending=False)
    vlr_ajuste_acresc_pis = df_ajuste_acresc_pis['3'].sum()

    #M510
    df_ajuste_acresc_cofins = M510[M510['2'] == '1'][['4', 'tipo_ajuste', '6', '3', '5', '7']].sort_values(by='3', ascending=False)
    vlr_ajuste_acresc_cofins = df_ajuste_acresc_cofins['3'].sum()

    #M105
    df_cred_por_tipo = M105.groupby(['2', 'NAT_BC_CRED'])['7'].sum().round(2).reset_index().sort_values(by='7', ascending=False)
        # adicionando o valor dos Ajustes de Acréscimo (M110/M550) no Dataframe
    new_row = ['-', 'ajuste de crédito', ((vlr_ajuste_acresc_pis + vlr_ajuste_acresc_cofins)/config.PIS_COFINS_RATE)]
    df_cred_por_tipo.loc[len(df_cred_por_tipo)] = new_row
    cred_bc_total = df_cred_por_tipo['7'].sum()      # soma das bc de cada tipo de credito
    df_cred_por_tipo['VLR_CRED_PIS_COFINS'] = (df_cred_por_tipo['7'] * config.PIS_COFINS_RATE).apply(lambda x: round(x,2))
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


def bloco_A_filtering(A100, A170):
    """Filter and analyze A-block data for services."""
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


def bloco_C_filtering(C100, C170, C175, C181, C185):
    """Filter and analyze C-block data for sales."""
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
