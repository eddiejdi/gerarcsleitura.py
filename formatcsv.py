import csv
import math

import pandas as pd
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
layout_saida = ['id_instrumento','cd_instrumento','tp_instrumento','dt_medicao','hr_medicao','sit_medicao','valor','tp_coleta','just_n_medicao', 'numero_OS','cond_adversa','obs','unid_medida_u','lei_manual_m','unid_medida_y','unid_medida_w','unid_medida_aa','unid_medida_ac','Leitura (m)','leitura','cota_z','leitua_sensor_u1','unid_medida_aec','unid_medida_aa','desv_padrao']

fieldnames = ['id_instrumento', 'cd_instrumento', 'tp_instrumento', 'stp_instrumento', 'dt_medicao', 'hr_medicao',
               'sit_medicao',
               'valor', 'tp_coleta', 'just_n_medicao', 'numero_OS', 'cond_adversa', 'obs', 'unid_medida_u',
               'unid_medida_y',
               'unid_medida_w', 'unid_medida_aa', 'unid_medida_ac', 'cd_instrumento_1', 'tp_instrumento_1',
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

# -  -  -  Saída

df = pd.read_csv('2022_01_15_INOUT_CadastroXLSX.csv',
            sep=';',
            index_col=0,
            parse_dates=['dt_medicao'],
            header=1,
            names=fieldnames,
            low_memory=False,
            encoding='UTF8')

print('*** Data Frame carregado ***') #-------------------------------------------------------------------------------

# Criação do dataframe de saida
dfOut = pd.DataFrame()
line = ""
idxOut = 0

for idxc, column in enumerate(df):
    uniqueColumn = df[column].name.replace('1_', '').replace('_1', '')

    line = df[column]
    if uniqueColumn in layout_saida:
        for idx, line in enumerate(column):
            if "dt" in uniqueColumn:
                print('Formatando Data: ' + str(column))
                dfOut[uniqueColumn] = df[column].apply(pd.to_datetime)
                # print(dfOut[column])
                dfOut[uniqueColumn] = df[column]

            if "hr" in uniqueColumn:
                print('Formatando Hora: ' + str(column))
                for cLine in df[column]:
                    if (pd.isnull(cLine)):
                        cLine = '00:00:00'
                    else:
                        time = cLine
                        hours = int(time)
                        minutes = (time * 60) % 60
                        seconds = (time * 3600) % 60
                        cLine = ("%0d:%02d.%02d" % (hours, minutes, seconds))
                        print(cLine)
                        pass



                dfOut[uniqueColumn] = df[column]
                    #line = ("%0d:%02d.%02d" % (hours, minutes, seconds))
                print(dfOut[uniqueColumn])
                # Using DataFrame.insert() to add a column

idxOut = idxc