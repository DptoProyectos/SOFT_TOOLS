#!/usr/aut_env/bin/python3.8
'''
version: 1.0.3

DESCRIPCION
Script para analizar los init y los datos de los datalogger que estan almacenados en la base de datos

'''

from __CORE__.dataAnalize import runAnalize

# DATALOGGERS DE LOS CUALES SE QUIERE ANALIZAR SUS DATOS
LISTA_DLGID = [
                #'PTEST01',
                #'PTEST02',
                #'PTEST03',
                #'PTEST04',
                #'FRICAN002',
                #'EFLUTEST01',
                'CTRLPAY01',

              ]

# TIPO DE DATALOGGER QUE SE QUIERE TESTEAR { 8CH | 5CH }
DLG_TYPE = '5CH'


# FECHA A PARTIR DE LA CUAL SE QUIERE HACER EL ANALISIS DE LOS DATOS
FECHA_INCIO = '2021-05-07 17:00'

















runAnalize(LISTA_DLGID,FECHA_INCIO,DLG_TYPE)