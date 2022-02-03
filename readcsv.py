import csv

from datetime import datetime
import pandas
import math
import numpy
# -----
# --
# -- gerarcsvleitura.py
# -- Read a file .csv check the column name and write a new file .csv
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
# -- RPA4All by Antonio Carneiro
# -- 17/01/22
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
# -- UPDATE CODE - 27/01/2022
# -- RPA4All by Edenilson Teixeira Paschoa
# --
# -----
# -  -  - Entrada
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


layout_saida = ['id_instrumento','cd_instrumento','tp_instrumento','dt_medicao','hr_medicao','sit_medicao','valor','tp_coleta','just_n_medicao', 'numero_OS','cond_adversa','obs','unid_medida_u','lei_manual_m','unid_medida_y','unid_medida_w','unid_medida_aa','unid_medida_ac','Leitura (m)','leitura','cota_z','leitua_sensor_u1','unid_medida_aec','unid_medida_aa','desv_padrao']

# ['id_instrumento', 'Código do Instrumento', 'Tipo de Instrumento', 'Data de Medição', 'Hora de medição',
#                 'Situação da Medição', 'Valor', 'Tipo de Coleta', 'Justificativa de não Medição', 'Número de OS',
#                 'Condição Adversa',
#                 'Observação', 'Unidade de Medida (U)', 'Leitura Manual (m)', 'Unidade de Medida (Y)',
#                 'Unidade de Medida (W)',
#                 'Unidade de Medida (AA)', 'Unidade de Medida (AC)', 'Leitura (m)', 'Leitura', 'Cota (Z)',
#                 'Leitura do Sensor Ultrassônico ',
#                 'Unidade de Medida (AE)', 'Unidade de Medida (ALL)', 'desv_padrao']

df = pandas.read_csv('2022_01_15_INOUT_CadastroXLSX.csv',
            sep=';',
            index_col=0,
            parse_dates=['dt_medicao'],
            header=1,
            names=fieldnames,
            low_memory=False,
            encoding='UTF8')

print('*** Data Frame carregado ***')

x = float("nan")
dfOut = pandas.DataFrame()

for column in df:
    uniqueColumn = df[column].name.replace('1_','').replace('_1','')

    if uniqueColumn in layout_saida: #['id_instrumento','cd_instrumento','tp_instrumento','dt_medicao','hr_medicao','sit_medicao','valor','tp_coleta','just_n_medicao','numero_OS','cond_adversa','obs','unid_medida_u','lei_manual_m','unid_medida_y','unid_medida_w','unid_medida_aa','unid_medida_ac','Leitura (m)','leitura','cota_z','leitua_sensor_u1','unid_medida_aec','unid_medida_aa','desv_padrao']:





        if "hr" in uniqueColumn:
            # df[column] = df[column].apply(pandas.to_datetime(df[column],format='%H:%M'))
            #for line in df[column]:
            #for (index_label, row_series) in df.iterrows():

            for index, namedTuple in enumerate(df[column]):

                if math.isnan(namedTuple) != True:
                    print('Formatando Hora: ' + str(namedTuple))
                    time = namedTuple
                    hours = int(time)
                    minutes = (time * 60) % 60
                    seconds = (time * 3600) % 60

                    print("%d:%02d.%02d" % (hours, minutes, seconds))
                    line = ("%0d:%02d.%02d" % (hours, minutes, seconds))

                    dfOut.iloc[index, df.columns.get_loc(uniqueColumn)] = line
            pass
            print(df[column])
            continue

        dfcolumn = df[column].tolist()
        # Using DataFrame.insert() to add a column
        if uniqueColumn in dfOut.columns:
            print("Atualizando Coluna " + uniqueColumn)
            try:
                for idx, line in enumerate(dfcolumn):
                    if not (pandas.isnull(line)):
                        if "dt" in uniqueColumn:
                            line = df[column].apply(pandas.to_datetime)
                            pass
                        if "hr" in uniqueColumn:
                            print('Formatando Hora: ' + str(line))
                            time = line
                            hours = int(time)
                            minutes = (time * 60) % 60
                            seconds = (time * 3600) % 60

                            print("%d:%02d.%02d" % (hours, minutes, seconds))
                            line = str("%d:%02d.%02d" % (hours, minutes, seconds))
                        dfOut.loc[idx, uniqueColumn] = line
                        line = ''
            except Exception as e:
                print(e)
        else:
            print("Coluna Inserida " + uniqueColumn)
            dfOut.insert(0, uniqueColumn, df[column].tolist(), True)
        pass
    pass

dfOut.to_csv('2022_01_15_CadastroXLSX.csv', sep=";", encoding='UTF8')


