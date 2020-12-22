#!/usr/aut_env/bin/python3.8
'''
version: 1.0.0

DESCRIPCION
Script para realizar graficas en intervalos de tiempos definidos

'''

from __CORE__.graficLevel import extCall

## DATOS DE CONFIGURACION ##

# DATALOGGERS DE LOS CUALES SE QUIERE LEER LOS DATOS
DLGID_LST   = [
                'FRPUL001',
              ]

# MAGNITUDES QUE SE QUIEREN ANALIZAR
TIPO_CONFIG = [
               'CAUDAL ANALÓGICO', 
               'PH',
               'TEMPERATURA', 
               'BAT', 
              ]

# RANGO DE FECHAS EN LAS CUALES SE QUIEREN ANALIZAR LOS DATOS
FECHA_INCIO = '2020-11-24 00:00:00'
FECHA_FIN   = '2021-11-24 07:00:00'   

# TIEMPO DE DOWNSAMPLE EN MINUTOS
DOWNSAMPLE = 10                                   






extCall(DLGID_LST,TIPO_CONFIG,FECHA_INCIO,FECHA_FIN,DOWNSAMPLE)