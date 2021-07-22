#!/usr/aut_env/bin/python3.8
'''
version: 1.0.2

DESCRIPCION
Script para realizar graficas en intervalos de tiempos definidos

'''

from __CORE__.graficLevel import extCall

## DATOS DE CONFIGURACION ##

# DATALOGGERS DE LOS CUALES SE QUIERE LEER LOS DATOS
DLGID_LST   = [
                #'PPOT04',
                #'FRPUL001'
                #'FRICAN002',
                #'EFLUTEST01',
                #'CTRLPAY01',
                'CCCA01',
                #'PSTPERF06'  
                #'CAUCUS01' , 

              ]

# MAGNITUDES QUE SE QUIEREN ANALIZAR
TIPO_CONFIG = [
               #'MB_AlturaCamara', 
               #'MB_CaudalSellos',
               #'MB_CorrienteVariador', 
               #'MB_FrecuanciaVariador', 
               #'MB_PresionSellos', 
               #'MB_Modo', 
               #'MB_States',
               #'MB_TemperaturaVariador',
               #'MB_UpdateFrequecy',
               #'MB_UpdateModo',
               #'CAUDAL ANALÓGICO',
               'MODBUS_M01',
               #'MB_PH',
               #'PUMP_PERF_STATE',

              ]

# RANGO DE FECHAS EN LAS CUALES SE QUIEREN ANALIZAR LOS DATOS
FECHA_INCIO = '2021-07-16 17:00:00'
FECHA_FIN   = '2022-06-01 00:00:00'   

# TIEMPO DE DOWNSAMPLE EN MINUTOS
DOWNSAMPLE = False                     # [ timeInMinutes | False ]                  






extCall(DLGID_LST,TIPO_CONFIG,FECHA_INCIO,FECHA_FIN,DOWNSAMPLE)