import csv
import pandas as pd
from datetime import datetime
import io
import sys

# -----
# --
# -- gerarcsvleitura.py
# -- Read a file .csv check the column name and write a new file .csv
# -- RPA4All by Antonio Carneiro
# -- 17/01/22
# --
# -----
# Saida em csv
layout_saida = ['id_instrumento', 'Código do Instrumento', 'Tipo de Instrumento', 'Data de Medição', 'Hora de medição',
                'Situação da Medição', 'Valor', 'Tipo de Coleta', 'Justificativa de não Medição', 'Número de OS',
                'Condição Adversa',
                'Observação', 'Unidade de Medida (U)', 'Leitura Manual (m)', 'Unidade de Medida (Y)',
                'Unidade de Medida (W)',
                'Unidade de Medida (AA)', 'Unidade de Medida (AC)', 'Leitura (m)', 'Leitura', 'Cota (Z)',
                'Leitura do Sensor Ultrassônico ',
                'Unidade de Medida (AE)', 'Unidade de Medida (ALL)', 'desv_padrao']

# - Estrutura do csv entrada
# - 'id_instrumento' pegar o conteudo abaixo e colocar na de estrutura de cima
# - cod_instrumento - colocar conteudo na saida
# - 'desv_padrao' - OK

fieldnames = ['id_instrumento', 'cd_instrumento', 'tp_instrumento', 'stp_instrumento', 'dt_medicao', 'hr_medicao',
              'sit_medicao',
              'valor', 'tp_coleta', 'just_n_medicao', 'numero_OS', 'cond_adversa', 'obs', 'unid_medida_u',
              'unid_medida_y',
              'leitura_m', 'unid_medida_w', 'unid_medida_aa', 'unid_medida_ac', 'cd_instrumento_1', 'tp_instrumento_1',
              'stp_instrumento_1', 'dt_medicao_1', 'hr_medicao_1', 'sit_medicao_1', 'valor_1', 'tp_coleta_1',
              'just_n_medicao_1',
              'numero_OS_1', 'cond_adversa_1', 'obs_1', 'unid_medida_u_1', 'lei_manual_m', 'unid_medida_y_1',
              'unid_medida_w_1',
              'unid_medida_aa_1', 'unid_medida_ac_1', 'cd_instrumento_1_1', 'tp_instrumento_1_1',
              'stp_instrumento_1_1', 'dt_medicao_1_1',
              'hr_medicao_1_1', 'sit_medicao_1_1', 'valor_1_1', 'tp_coleta_1_1', 'just_n_medicao_1_1', 'numero_OS_1_1',
              'cond_adversa_1_1',
              'obs_1_1', 'unid_medida_u_1_1', 'unid_medida_y_1_1', 'leitura', 'unid_medida_w_1_1', 'leitua_sensor_u1',
              'cd_instrumento_1_1_1',
              'tp_instrumento_1_1_1', 'stp_instrumento_1_1_1', 'dt_medicao_1_1_1', 'hr_medicao_1_1_1',
              'sit_medicao_1_1_1', 'valor_1_1_1',
              'tp_coleta_1_1_1', 'just_n_medicao_1_1_1', 'numero_OS_1_1_1', 'cond_adversa_1_1_1', 'obs_1_1_1',
              'unid_medida_u_1_1_1',
              'lei_manual_m_1', 'unid_medida_w_1_1_1', 'unid_medida_aa_1_1', 'unid_medida_ac_1_1',
              'cd_instrumento_1_1_1_1', 'tp_instrumento_1_1_1_1',
              'stp_instrumento_1_1_1_1', 'dt_medicao_1_1_1_1', 'hr_medicao_1_1_1_1', 'sit_medicao_1_1_1_1',
              'valor_1_1_1_1', 'tp_coleta_1_1_1_1',
              'just_n_medicao_1_1_1_1', 'numero_OS_1_1_1_1', 'cond_adversa_1_1_1_1', 'obs_1_1_1_1',
              'unid_medida_u_1_1_1_1', 'unid_medida_y_1_1_1',
              'unid_medida_w_1_1_1_1', 'unid_medida_aa_1_1_1', 'cota_z', 'cd_instrumento_1_1_1_1_1',
              'tp_instrumento_1_1_1_1_1', 'stp_instrumento_1_1_1_1_1',
              'dt_medicao_1_1_1_1_1', 'hr_medicao_1_1_1_1_1', 'sit_medicao_1_1_1_1_1', 'valor_1_1_1_1_1',
              'tp_coleta_1_1_1_1_1',
              'just_n_medicao_1_1_1_1_1', 'numero_OS_1_1_1_1_1', 'cond_adversa_1_1_1_1_1', 'obs_1_1_1_1_1',
              'unid_medida_u_1_1_1_1_1',
              'unid_medida_w_1_1_1_1_1', 'leitura_sensor_u1_1', 'cd_instrumento_1_1_1_1_1_1',
              'tp_instrumento_1_1_1_1_1_1', 'stp_instrumento_1_1_1_1_1_1', 'dt_medicao_1_1_1_1_1_1',
              'hr_medicao_1_1_1_1_1_1', 'sit_medicao_1_1_1_1_1_1', 'valor_1_1_1_1_1_1', 'tp_coleta_1_1_1_1_1_1',
              'just_n_medicao_1_1_1_1_1_1',
              'unid_medida', 'numero_OS_1_1_1_1_1_1', 'cond_adversa_1_1_1_1_1_1', 'obs_1_1_1_1_1_1',
              'lei_manual_m_1_1', 'unid_medida_y_1_1_1_1',
              'unid_medida_aa_1_1_1_1', 'unid_medida_ac_1_1_1', 'cd_instrumento_1_1_1_1_1_1_1',
              'tp_instrumento_1_1_1_1_1_1_1', 'stp_instrumento_1_1_1_1_1_1_1',
              'dt_medicao_1_1_1_1_1_1_1', 'hr_medicao_1_1_1_1_1_1_1', 'sit_medicao_1_1_1_1_1_1_1',
              'Valor_1_1_1_1_1_1_1', 'tp_coleta_1_1_1_1_1_1_1',
              'just_n_medicao_1_1_1_1_1_1_1', 'unid_medida_1', 'numero_OS_1_1_1_1_1_1_1', 'cond_adversa_1_1_1_1_1_1_1',
              'obs_1_1_1_1_1_1_1',
              'lei_manual_m_1_1_1', 'unid_medida_y_1_1_1_1_1', 'unid_medida_aa_1_1_1_1_1', 'unid_medida_ac_1_1_1_1',
              'unid_medida_aec',
              'cota_z_1', 'cd_instrumento_1_1_1_1_1_1_1_1', 'tp_instrumento_1_1_1_1_1_1_1_1',
              'stp_instrumento_1_1_1_1_1_1_1_1',
              'dt_medicao_1_1_1_1_1_1_1_1', 'hr_medicao_1_1_1_1_1_1_1_1', 'sit_medicao_1_1_1_1_1_1_1_1',
              'valor_1_1_1_1_1_1_1_1',
              'tp_coleta_1_1_1_1_1_1_1_1', 'just_n_medicao_1_1_1_1_1_1_1_1', 'unid_medida_1_1',
              'numero_OS_1_1_1_1_1_1_1_1', 'cond_adversa_1_1_1_1_1_1_1_1',
              'obs_1_1_1_1_1_1_1_1', 'lei_manual_m_1_1_1_1', 'unid_medida_y_1_1_1_1_1_1', 'unid_medida_aa_1_1_1_1_1_1',
              'unid_medida_ac_1_1_1_1_1',
              'cd_instrumento_1_1_1_1_1_1_1_1_1', 'tp_instrumento_1_1_1_1_1_1_1_1_1',
              'stp_instrumento_1_1_1_1_1_1_1_1_1', 'dt_medicao_1_1_1_1_1_1_1_1_1',
              'hr_medicao_1_1_1_1_1_1_1_1_1', 'sit_medicao_1_1_1_1_1_1_1_1_1', 'valor_1_1_1_1_1_1_1_1_1',
              'tp_coleta_1_1_1_1_1_1_1_1_1',
              'just_n_medicao_1_1_1_1_1_1_1_1_1', 'unid_medida_1_1_1', 'numero_OS_1_1_1_1_1_1_1_1_1',
              'cond_adversa_1_1_1_1_1_1_1_1_1',
              'obs_1_1_1_1_1_1_1_1_1', 'lei_manual_m_1_1_1_1_1', 'unid_medida_y_1_1_1_1_1_1_1',
              'unid_medida_aa_1_1_1_1_1_1_1',
              'unid_medida_ac_1_1_1_1_1_1', 'cd_instrumento_1_1_1_1_1_1_1_1_1_1', 'tp_instrumento_1_1_1_1_1_1_1_1_1_1',
              'stp_instrumento_1_1_1_1_1_1_1_1_1_1',
              'dt_medicao_1_1_1_1_1_1_1_1_1_1', 'hr_medicao_1_1_1_1_1_1_1_1_1_1', 'sit_medicao_1_1_1_1_1_1_1_1_1_1',
              'valor_1_1_1_1_1_1_1_1_1_1',
              'tp_coleta_1_1_1_1_1_1_1_1_1_1', 'just_n_medicao_1_1_1_1_1_1_1_1_1_1', 'unid_medida_1_1_1_1',
              'numero_OS_1_1_1_1_1_1_1_1_1_1',
              'cond_adversa_1_1_1_1_1_1_1_1_1_1', 'obs_1_1_1_1_1_1_1_1_1_1', 'lei_manual_m_1_1_1_1_1_1',
              'unid_medida_y_1_1_1_1_1_1_1_1',
              'unid_medida_aa_1_1_1_1_1_1_1_1', 'unid_medida_ac_1_1_1_1_1_1_1', 'cd_instrumento_1_1_1_1_1_1_1_1_1_1_1',
              'tp_instrumento_1_1_1_1_1_1_1_1_1_1_1', 'stp_instrumento_1_1_1_1_1_1_1_1_1_1_1',
              'dt_medicao_1_1_1_1_1_1_1_1_1_1_1', 'hr_medicao_1_1_1_1_1_1_1_1_1_1_1',
              'sit_medicao_1_1_1_1_1_1_1_1_1_1_1', 'valor_1_1_1_1_1_1_1_1_1_1_1', 'tp_coleta_1_1_1_1_1_1_1_1_1_1_1',
              'just_n_medicao_1_1_1_1_1_1_1_1_1_1_1',
              'numero_OS_1_1_1_1_1_1_1_1_1_1_1', 'cond_adversa_1_1_1_1_1_1_1_1_1_1_1', 'obs_1_1_1_1_1_1_1_1_1_1_1',
              'desv_padrao']

df = pd.read_csv('2022_01_15_INOUT_CadastroXLSX.csv',
                 sep=';',
                 index_col=0,
                 parse_dates=['dt_medicao'],
                 header=1,
                 names=fieldnames,
                 low_memory=True,
                 encoding='UTF8')

# Criação do dataframe de saida
dfOut = pd.DataFrame()

print('*** Data Frame carregado ***')  # -------------------------------------------------------------------
# loop nas colunas
for idxc, column in enumerate(df):
    uniqueColumn = df[column].name.replace('1_', '').replace('_1', '')

    if uniqueColumn in dfOut.columns:
        print("Coluna Existente")
        print("Atualizando Coluna " + uniqueColumn)
        # dfOut.loc[uniqueColumn] = df[column]
    else:
        print("Coluna Inserida " + uniqueColumn)
        dfOut.insert(len(dfOut.columns), uniqueColumn, df[column].tolist(), True)

    # loop nas linhas
    for idxr, row in enumerate(df[column]):

        if "dt" in uniqueColumn:
            if (pd.isnull(row)):
                #row = '00:00:00'
                print('nulo')
            else:
                # print(row)
                try:
                    row = datetime.strptime(row[:10], '%m/%d/%y').strftime('%d/%m/%y')
                except Exception as e:
                    print('Data Inválida' + str(e))
                finally:
                    pass


            # dfOut[uniqueColumn] = df[column]

        if "hr" in uniqueColumn:
            # print('Formatando Hora: ' + str(row))

            if (pd.isnull(row)):
                # row = '00:00:00'
                print('null')
            else:
                time = row
                hours = int(time)
                minutes = (time * 60) % 60
                seconds = (time * 3600) % 60
                row = ("%0d:%02d.%02d" % (hours, minutes, seconds))
                # print(row)
                pass

        if  not pd.isnull(row):
            # print(dfOut.loc[idxr, uniqueColumn])
            dfOut.loc[idxr, uniqueColumn] = row

    # print(dfOut)

print("fim")
dfOut.to_csv('2022_01_15_CadastroXLSX.csv', sep=";", encoding='UTF8')
