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
DLGID_LST   = ['FRPUL001']

TIPO_CONFIG = ['CAUDAL ANALÓGICO', 'PH','TEMPERATURA', 'BAT']
FECHA_INCIO = '2020-12-11 00:00:00'
FECHA_FIN   = '2020-12-22 23:59:59'   

## CONFIGURACION DEL SALVADO DEL CSV
PATCH_CSV = '/CSV/'
#NAME_CSV_TO_ANALZE = '201008170000&211007160000.csv'
NAME_CSV_TO_ANALZE = ''
SAVE_CSV = True                 # [True  => se descargan datos de la base de datos y se guardan en CSV]
                                  # [False => se realiza el analisis de datos a partir del CSV con nombre NAME_CSV_TO_ANALZE]

## CONFIGURACION DE GRAFICAS
### MAGNITUDES A PLOTEAR
QUERY_DATA_LIST = [
                    #'NSEN26-CAUDAL ANALÓGICO',
                    #'NSEN26-P_PRESSURE',
                    'FRPUL001-CAUDAL ANALÓGICO',
                    #'PPOT04-CAUDAL ANALÓGICO',
                  ]
                  
### FECHA Y HORA ENTRE LAS CUALES SE QUIERE VER EL GRAFICO
START_GRAPH_TIME = '2020-12-18 00:00:00';
END_GRAPH_TIME   = '2020-12-18 23:59:59';

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


class processingData:

    def __init__(self,df_base=None,dlg=DLGID_LST,typeConf=TIPO_CONFIG,starDate=FECHA_INCIO,endDate=FECHA_FIN,pathCSV=PATCH_CSV,nameCSV2Analyse=NAME_CSV_TO_ANALZE,downsample=DOWNSAMPLE):
        self.df_base = df_base
        self.DLGID_LST = dlg
        self.TIPO_CONFIG = typeConf
        self.FECHA_INCIO = starDate
        self.FECHA_FIN = endDate
        self.PATCH_CSV = pathCSV
        self.NAME_CSV_TO_ANALZE = nameCSV2Analyse
        self.DOWNSAMPLE = downsample

        self.TIPO_CONFIG.append('')             # add a nul value for correct a bug when the list has only one value

    def read_data_from_db(self):
        ''' lee los datos de la base de datos y los devuelve en un dataframe '''

        df_base = None
        connection = mySQL_db().connect_to_db()
        self.DLGID_LST.append('')

        sql = """SELECT spx_unidades.dlgid, spx_datos.fechadata, spx_tipo_configuracion.tipo_configuracion, spx_datos.valor FROM spx_datos
                INNER JOIN spx_tipo_configuracion ON spx_tipo_configuracion.id=spx_datos.medida_id
                INNER JOIN spx_instalacion ON spx_instalacion.ubicacion_id=spx_datos.ubicacion_id 
                INNER JOIN spx_unidades ON spx_unidades.id=spx_instalacion.unidad_id
                WHERE spx_tipo_configuracion.tipo_configuracion IN {0}
                AND spx_unidades.dlgid IN {1}
                AND spx_datos.fechadata > '{2}'
                AND spx_datos.fechadata < '{3}'
                ORDER BY spx_datos.fechadata
        """.format(tuple(self.TIPO_CONFIG),tuple(self.DLGID_LST),self.FECHA_INCIO,self.FECHA_FIN)

        df_base = mySQL_db().execute_sql_query(connection,sql)
        return df_base

    def save_to_CSV(self,df_base):
        ''' guarda el dataframe en un archivo csv '''
        
        # damos formato al nombre del archivo      
        name_CSV_file = '{0}&{1}.csv'.format(
            datetime.strptime(self.FECHA_INCIO, '%Y-%m-%d %H:%M:%S').strftime('%y%m%d%H%M%S'), 
            datetime.strptime(self.FECHA_FIN  , '%Y-%m-%d %H:%M:%S').strftime('%y%m%d%H%M%S')
        )
        df_base.to_csv('{0}{1}'.format(self.PATCH_CSV,name_CSV_file))
      
    def read_from_CSV(self):
        ''' lee los datos del CSV y los devuelve en un dataFrame '''
        
        # chequeo si esta seleccionado para leer un csv distinto al ultimo descargado
        if not self.NAME_CSV_TO_ANALZE:
            print_log('se carga el ultimo archivo csv descargado')
            name_CSV_file = '{0}&{1}.csv'.format(
                datetime.strptime(self.FECHA_INCIO, '%Y-%m-%d %H:%M:%S').strftime('%y%m%d%H%M%S'), 
                datetime.strptime(self.FECHA_FIN  , '%Y-%m-%d %H:%M:%S').strftime('%y%m%d%H%M%S')
                )
            patch_csv = '{0}{1}'.format(self.PATCH_CSV,name_CSV_file)
        else:
            patch_csv = '{0}{1}'.format(self.PATCH_CSV,self.NAME_CSV_TO_ANALZE)

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
                if self.DOWNSAMPLE: data = data.resample(f'{self.DOWNSAMPLE}Min', axis=0).mean()
                data = data[tipo_configuracion]
                data = data.to_frame(name=f'{dlgid}-{tipo_configuracion}')
                data = data[f'{dlgid}-{tipo_configuracion}']
                df_boundle = pd.concat([df_boundle, data], axis=1)
        
        return df_boundle


class dataAnalysis:
    '''
        tools for data processing
    '''
    def __init__(self,dataFrame):
        self.dataFrame = dataFrame;

    def show_grafic(self,query_data_list,startTime,endTime):
        '''
            entra self.df_base y a partir del query_data_list hago las graficas que necesito
        '''
               
        # filtrado por magnitud
        try: 
            data_to_grafic = self.dataFrame.loc[:,query_data_list]
        except Exception as err_var:
            data_to_grafic = self.dataFrame 
            print_log('ERROR: EXCEPTION {0}'.format(err_var))
            print_log('ERROR: filtrado incorrecto por magnitud !!!')
            print_log('ERROR: revisar => QUERY_DATA_LIST  !!!')
            print_log('ERROR: se muestran todas las magnitudes !!!')
            
        # filtrado por datetime
        try: 
            data_to_grafic = data_to_grafic.loc[pd.to_datetime(startTime):pd.to_datetime(endTime)]
        except Exception as err_var:
            print_log(self.dataFrame)
            print_log('ERROR: EXCEPTION {0}'.format(err_var))
            print_log('ERROR: filtrado incorrecto por timpo !!!')
            print_log('ERROR: revisar => START_GRAPH_TIME  !!!')
            print_log('ERROR: revisar => END_GRAPH_TIME  !!!')
            print_log('ERROR: se muestra todo todo el tiempo !!!')
            
        data_to_grafic.plot()
        plt.show()

    def linearConvertion(x1,x2,y1,y2):
        '''
            y = mx + n
        '''
        m = ((y2-y1)/(x2-x1))
        n = (y2 - (m*x2))
        self.dataFrame = ((self.dataFrame * m) + n)

    def manning(self):
        ''''''
        # entra df_base
        
        self.dataFrame = self.dataFrame * 2
        print(self.dataFrame)
        
    def francis_inv(self,L,typeConf):
        '''
            calculations of the water height from the waterflow using Francis's formula
            https://drive.google.com/file/d/1YvJ_WRHe2QswFk1U0-lD2LhOqFKbzyM5/view?usp=sharing

            <= L: width of the crest
            <= typeConf: type of cunfiguration that has flow data
            => self.dataFrame: dataframe with the column typeConf modified with the height of the water
        '''
        import numpy as np
        
        def math_inv(L,Q):
            ''' 
                inverse manning calculus
                    <= L: width of the crest
                    <= Q: flow in m3/h
                    => h: heigh of water
            '''
               
            Qlast = 999999999999999999
            hlast = 0 

            discreetHeigh = np.array(list(range(1500)))*0.001                   # possible water height values  

            for hn in discreetHeigh:
                Qn = 1.84*(L-(0.2*hn))*pow(hn,1.5)*3600                         # flow water in m3/h
                if (abs(Qn-Q) < abs(Qlast-Q)):
                    Qlast = Qn
                    hlast = hn
                else:
                    return hlast
                    
        for cau in self.dataFrame.index: self.dataFrame[typeConf][cau] = math_inv(L,self.dataFrame[typeConf][cau])


        
        
        


#################### MAIN ####################
def getDatas():
    '''
        Gets the required datas from a database and put it into a dataframe
    '''
    procData = processingData()

    if(SAVE_CSV):procData.save_to_CSV(procData.read_data_from_db())       # leer los datos de la base de datos y guardarlos en un csv

    wdf_base = procData.index_data(procData.read_from_CSV())              # lee los datos del csv y los devuelve ya indexados

    return wdf_base

def extCall(dlg,typeConf,starDate,endDate,downsample):
    
    procData = processingData(None,dlg,typeConf,starDate,endDate,PATCH_CSV,NAME_CSV_TO_ANALZE,downsample)

    if(SAVE_CSV):procData.save_to_CSV(procData.read_data_from_db())         # leer los datos de la base de datos y guardarlos en un csv

    wdf_base = procData.index_data(procData.read_from_CSV())                # lee los datos del csv y los devuelve ya indexados

    QUERY_DATA_LIST = []
    for dg in dlg:                                                          # build the QUERY_DATA_LIST for plot datas
        for tp in typeConf:
            QUERY_DATA_LIST.append('{0}-{1}'.format(dg,tp))
    
    dataAnalysis(wdf_base).show_grafic(QUERY_DATA_LIST,starDate,endDate)   

def processDatas(datos):
    ''' 
        data analysis process
    '''
    # ICLASS'S INSTANCES
    dA = dataAnalysis(datos)                                                

    # CHANGES IN THE DATA
    dA.francis_inv(0.4,'FRPUL001-CAUDAL ANALÓGICO')
    dA.show_grafic(QUERY_DATA_LIST,START_GRAPH_TIME,END_GRAPH_TIME)                                          

    
    
    


if __name__ == '__main__':
    datos = getDatas()                                  # gest dataframe
    processDatas(datos)                                 # analysis of dataframe
    exit(0)
    



