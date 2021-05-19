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
                #'PPOT05',
                #'UYTAC023',
                #'UYTAC044',
                #'UYTAC042', 
                #'FRPUL001'
                #'FRICAN002',
                #'EFLUTEST01',
                'CTRLPAY01',
              ]

# MAGNITUDES QUE SE QUIEREN ANALIZAR
TIPO_CONFIG = [
               #'MB_AlturaCámara', 
               #'MB_CaudalSellos',
               #'MB_CorrienteVariador', 
               #'MB_FrecuanciaVariador', 
               #'MB_PresionSellos', 
               #'MB_Modo', 
               #'MB_States',
               #'MB_TemperaturaVariador',
               #'MB_UpdateFrequecy',
               #'MB_UpdateModo',

              ]

# RANGO DE FECHAS EN LAS CUALES SE QUIEREN ANALIZAR LOS DATOS
FECHA_INCIO = '2021-05-08 16:00:00'
FECHA_FIN   = '2022-05-08 14:00:00'   

# TIEMPO DE DOWNSAMPLE EN MINUTOS
DOWNSAMPLE = 1                                   






extCall(DLGID_LST,TIPO_CONFIG,FECHA_INCIO,FECHA_FIN,DOWNSAMPLE)