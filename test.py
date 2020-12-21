#!/usr/aut_env/bin/python3.8
'''
version: 1.0.0

DESCRIPCION
Script para realizar graficas en intervalos de tiempos definidos

'''

from __CORE__.graficLevel import test_OD

## DATOS DE CONFIGURACION ##
DLGID_LST   = [
                'FRPUL001',
                'NSEN25'
              ]
TIPO_CONFIG = [
               'CAUDAL ANALÓGICO', 
               'PH',
               'TEMPERATURA', 
               'BAT', 
               'H_TQ', 
               'PUMP_PERF_STATE', 
               'RESPALDO_TIMER'
               ]

FECHA_INCIO = '2020-11-24 00:00:00'
FECHA_FIN   = '2021-11-24 07:00:00'   

DOWNSAMPLE = 10


test_OD()