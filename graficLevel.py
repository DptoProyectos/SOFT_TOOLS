#!/usr/aut_env/bin/python3.8

'''
version: 1.0.0

DESCRIPCION
Script para el analisis de los datos de exigeno disuelto de en PTAR PASO DE LOS TOROS

DEPENDENCIAS
sqlalchemy, pandas, matplotlib, datetime, psycopg2-binary
'''
import os
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine,text
import matplotlib.pyplot as plt


# CONFIGURATION DEL SCRIPT#
#URL_DB = 'mysql+pymysql://pablo:spymovil@192.168.0.8/GDA'
URL_DB = 'postgresql+psycopg2://admin:pexco599@192.168.0.6/GDA'
PRINT_LOG = True

## DATOS FILTROS PARA LA DESCARGA DE LA BASE DE DATOS
DLGID_LST   = ['FRPUL001', 'ODPT01','RIVPERF15','RIVPERF10']
#DLGID_LST   = ['ODPT01', 'RIVPERF01']
#DLGID_LST   = ['ODPT01', 'FRPUL001']

#TIPO_CONFIG = ['Alt_CA', 'BAT']
TIPO_CONFIG = ['CAUDAL ANALÓGICO', 'PH','TEMPERATURA', 'BAT', 'H_TQ', 'PUMP_PERF_STATE', 'RESPALDO_TIMER']
FECHA_INCIO = '2020-11-24 00:00:00'
FECHA_FIN   = '2021-11-24 07:00:00'   

## CONFIGURACION DEL SALVADO DEL CSV
PATCH_CSV = '/CSV/'
#NAME_CSV_TO_ANALZE = '201008170000&211007160000.csv'
NAME_CSV_TO_ANALZE = ''
SAVE_CSV  = True                # [True  => se descargan datos de la base de datos y se guardan en CSV]
                                 # [False => se realiza el analisis de datos a partir del CSV con nombre NAME_CSV_TO_ANALZE]

## CONFIGURACION DE GRAFICAS
### MAGNITUDES A PLOTEAR
QUERY_DATA_LIST = [
                    #'FRPUL001-CAUDAL ANALÓGICO',
                    #'FRPUL001-PH',
                    #'FRPUL001-TEMPERATURA',
                    'FRPUL001-BAT',
                    #'ODPT01-OXIGENO_DIS',
                    #'ODPT01-VAR_FREC',
                    #'ODPT01-CURRENT_PUMP',
                    #'ODPT02-OXIGENO_DIS',
                    #'PTCA001-Alt_CA',
                    #'RIVPERF15-H_TQ',
                    #'RIVPERF06-RESPALDO_TIMER',
                    #'RIVPERF10-PUMP_PERF_STATE',
                  ]
                  
### FECHA Y HORA ENTRE LAS CUALES SE QUIERE VER EL GRAFICO
START_GRAPH_TIME = '2020-11-23 23:00:00';
END_GRAPH_TIME   = '2020-11-24 05:00:00';

### PERIODO DE MUESTREO EN MINUTOS
DOWNSAMPLE = 10


def print_log(text_log):
    if PRINT_LOG: print(text_log)


class mySQL_db:

    def __init__(self):
        pass

    def connect_to_db(self):
        ''' Establece la coneccion con la base de datos y retorna el objero connection '''

        connection = None
    
        try:
            engine = create_engine(URL_DB)
        except Exception as err_var:
            engine = None
            print('ERROR: engine NOT created. ABORT !!')
            print('ERROR: EXCEPTION {0}'.format(err_var))
                    
        if engine:
            try:
                connection = engine.connect()
            except Exception as err_var:
                print('ERROR: NOT connected. ABORT !!')
                print('ERROR: EXCEPTION {0}'.format(err_var))
                        
        return connection

    def execute_sql_query(self,sql_connection,sql_query):
        ''' 
            ejecuta sql_query en la coneccion sql_connection y devuelve un dataframe con los datos
        '''
        query = text(sql_query)
        response = sql_connection.execute(sql_query)
        df_base = pd.DataFrame(response.fetchall())
        
        # Detecto si la consulta devuelve resultado nulo
        if (df_base.empty):
            print_log('ERROR: No data from bd. ABORT !!')
            exit(1);
        
        df_base.columns = response.keys()                   # seteo el nombre de las columnas 
        df_base.index.name = 'idx'                          # seteo el index
        return df_base


class dissolved_oxygen:

    def __init__(self,df_base=None):
        self.df_base = df_base

    def read_data_from_db(self):
        ''' lee los datos de la base de datos y los devuelve en un dataframe '''

        df_base = None
        connection = mySQL_db().connect_to_db()
        

        sql = """SELECT spx_unidades.dlgid, spx_datos.fechadata, spx_tipo_configuracion.tipo_configuracion, spx_datos.valor FROM spx_datos
                INNER JOIN spx_tipo_configuracion ON spx_tipo_configuracion.id=spx_datos.medida_id
                INNER JOIN spx_instalacion ON spx_instalacion.ubicacion_id=spx_datos.ubicacion_id 
                INNER JOIN spx_unidades ON spx_unidades.id=spx_instalacion.unidad_id
                WHERE spx_tipo_configuracion.tipo_configuracion IN {0}
                AND spx_unidades.dlgid IN {1}
                AND spx_datos.fechadata > '{2}'
                AND spx_datos.fechadata < '{3}'
                ORDER BY spx_datos.fechadata
        """.format(tuple(TIPO_CONFIG),tuple(DLGID_LST),FECHA_INCIO,FECHA_FIN)

        df_base = mySQL_db().execute_sql_query(connection,sql)
        return df_base

    def save_to_CSV(self,df_base):
        ''' guarda el dataframe en un archivo csv '''
        
        # damos formato al nombre del archivo      
        name_CSV_file = '{0}&{1}.csv'.format(
            datetime.strptime(FECHA_INCIO, '%Y-%m-%d %H:%M:%S').strftime('%y%m%d%H%M%S'), 
            datetime.strptime(FECHA_FIN  , '%Y-%m-%d %H:%M:%S').strftime('%y%m%d%H%M%S')
        )
        df_base.to_csv('{0}{1}'.format(PATCH_CSV,name_CSV_file))
      
    def read_from_CSV(self):
        ''' lee los datos del CSV y los devuelve en un dataFrame '''
        
        # chequeo si esta seleccionado para leer un csv distinto al ultimo descargado
        if not NAME_CSV_TO_ANALZE:
            print_log('se carga el ultimo archivo csv descargado')
            name_CSV_file = '{0}&{1}.csv'.format(
                datetime.strptime(FECHA_INCIO, '%Y-%m-%d %H:%M:%S').strftime('%y%m%d%H%M%S'), 
                datetime.strptime(FECHA_FIN  , '%Y-%m-%d %H:%M:%S').strftime('%y%m%d%H%M%S')
                )
            patch_csv = '{0}{1}'.format(PATCH_CSV,name_CSV_file)
        else:
            patch_csv = '{0}{1}'.format(PATCH_CSV,NAME_CSV_TO_ANALZE)

        # chequeo si existe el anrchivo CSV
        if not(os.path.isfile(patch_csv)):
            print_log('No existe archivo {}'.format(patch_csv))
            print_log('Configure => SAVE_CSV  = True para leer de la base de datos !!!')
            print_log('Revise que exista NAME_CSV_TO_ANALZE = "FileNameToAnalyze" !!!')
            exit(1)
            
            
        df_base = pd.read_csv(patch_csv, index_col='idx')
        print_log(df_base)
        return df_base

    def index_data(self,df_base):
        ''' reordena los datos de df_base y devuelve wdf_base ya indexado '''
        
        df_base.fechadata = pd.to_datetime(df_base.fechadata)                 # ejecuta un form en toda la columna fechadata y convierte cada elemento a datetime

        # obtenemos un diccionario con los dlgid y sus correpondientes configuraciones
        lst_dlgid = df_base['dlgid'].unique()                                 # obtengo una lista con todos los datalogger consultados
        config_loaded = {}                                                    # empty dictionary.
        for dlgid in lst_dlgid:
            tipo_conf_dlg = df_base.copy()  
            tipo_conf_dlg.set_index(['dlgid'], drop=True, inplace=True)       # indexamos por dlgid
            tipo_conf_dlg = tipo_conf_dlg.loc[dlgid,'tipo_configuracion']     # nos quedamos con todos los tipos de conf para el lst_dlgid[i] 
            tipo_conf_dlg = tipo_conf_dlg.unique()                            # elimino tipos de configuraciones duplicadas
            config_loaded[dlgid] = tipo_conf_dlg                              # diccionario {dlgid:[tipo_conf]}
        
        wdf_base = df_base.copy()                                             # copia del df original
        lst_index = ['fechadata','dlgid','tipo_configuracion']                # lista con columnas a indexar
        wdf_base.set_index(lst_index, drop=True, inplace=True)                # indexamos para una mejor busqueda
        wdf_base = wdf_base.unstack(['dlgid','tipo_configuracion'])           # convertimos en columnas las filas indexadas ['dlgid','tipo_configuracion']
        wdf_base = wdf_base.sort_values('fechadata',ascending=True)           # ordenamos los datos por fecha y hora

        df_boundle=pd.DataFrame()

        for dlgid in config_loaded.keys():
            for tipo_configuracion in config_loaded[dlgid]:
                data = wdf_base['valor', dlgid]
                data = data.copy()
                data = data.resample(f'{DOWNSAMPLE}Min', axis=0).mean()
                data = data[tipo_configuracion]
                data = data.to_frame(name=f'{dlgid}-{tipo_configuracion}')
                data = data[f'{dlgid}-{tipo_configuracion}']
                df_boundle = pd.concat([df_boundle, data], axis=1)
        
        return df_boundle

    def show_grafic(self,query_data_list):
        '''
            entra self.df_base y a partir del query_data_list hago las graficas que necesito
        '''
                
        # filtrado por magnitud
        try: 
            data_to_grafic = self.df_base.loc[:,query_data_list]
        except Exception as err_var:
            data_to_grafic = self.df_base 
            print_log('ERROR: EXCEPTION {0}'.format(err_var))
            print_log('ERROR: filtrado incorrecto por magnitud !!!')
            print_log('ERROR: revisar => QUERY_DATA_LIST  !!!')
            print_log('ERROR: se muestran todas las magnitudes !!!')
            
        # filtrado por datetime
        try: 
            data_to_grafic = data_to_grafic.loc[START_GRAPH_TIME:END_GRAPH_TIME]
        except Exception as err_var:
            print_log(self.df_base)
            print_log('ERROR: EXCEPTION {0}'.format(err_var))
            print_log('ERROR: filtrado incorrecto por timpo !!!')
            print_log('ERROR: revisar => START_GRAPH_TIME  !!!')
            print_log('ERROR: revisar => END_GRAPH_TIME  !!!')
            print_log('ERROR: se muestra todo todo el tiempo !!!')
            
        data_to_grafic.plot()
        plt.show()


def test_OD():
    OD = dissolved_oxygen()

    if(SAVE_CSV):OD.save_to_CSV(OD.read_data_from_db())       # leer los datos de la base de datos y guardarlos en un csv

    wdf_base = OD.index_data(OD.read_from_CSV())              # lee los datos del csv y los devuelve ya indexados

    dissolved_oxygen(wdf_base).show_grafic(QUERY_DATA_LIST)   # grafico los valores de QUERY_DATA_LIST



    


    

    

    





if __name__ == '__main__':
    test_OD()
    exit(0)
    



