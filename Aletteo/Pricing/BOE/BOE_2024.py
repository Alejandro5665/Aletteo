import pandas as pd

# Declaracion de variables con valor unico

Bono_social = 1
Fondo_Nacional = 0.975
Tasa_municipal = 0.015228
OS = 0.17498
OM = 0.04096
IE = 0.051127
IVA = 0.1

# Declaración de las tablas como diccionario

#Terminos en €/kW año
datos_potencia_peaje_transporte_2024 = {
    "Segmento tarifario": [1, 2, 3, 4, 5, 6],
    "P1": [3.117200, 1.547750, 5.038316, 4.905550, 5.436815, 7.310560],
    "P2": [0.039102, 1.009391, 3.101868, 3.148751, 3.407131, 4.116430],
    "P3": [0, 0.440585, 2.385032, 2.144498, 2.678037, 3.161822],
    "P4": [0, 0.358447, 1.892034, 1.723336, 2.034190, 2.877385],
    "P5": [0, 0.042635, 0.078114, 0.093775, 0.131927, 0.194493],
    "P6": [0, 0.042635, 0.078114, 0.093775, 0.131927, 0.194493],
}

datos_potencia_peaje_distribucion_2024 = {
    "Segmento tarifario": [1, 2, 3, 4, 5, 6],
    "P1": [19.284546, 10.450080, 15.519534, 8.232863, 5.037478, 0],
    "P2": [0.737462, 6.678414, 9.661016, 5.602456, 3.103289, 0],
    "P3": [0, 2.868852, 7.541219, 3.471172, 2.563687, 0],
    "P4": [0, 2.433339, 5.956346, 2.947782, 2.104645, 0],
    "P5": [0, 0.891800, 0.247027, 0.144700, 0.209538, 0],
    "P6": [0, 0.891800, 0.247027, 0.144700, 0.209538, 0],
}

datos_potencia_cargos_2024 = {
    "Segmento tarifario": [1, 2, 3, 4, 5, 6],
    "Periodo 1": [2.989915, 3.715217, 3.856557, 2.264702, 1.813304, 0.887008],
    "Periodo 2": [0.192288, 1.859231, 1.930027, 1.133557, 0.907425, 0.443874],
    "Periodo 3": [0, 1.350774, 1.402384, 0.823528, 0.659281, 0.322548],
    "Periodo 4": [0, 1.350774, 1.402384, 0.823528, 0.659281, 0.322548],
    "Periodo 5": [0, 1.350774, 1.402384, 0.823528, 0.659281, 0.322548],
    "Periodo 6": [0, 0.619203, 0.642759, 0.377450, 0.302217, 0.147835],
}

#Terminos en €/kwh
datos_energia_peaje_transporte_2024 = {
    "Segmento tarifario": [1, 2, 3, 4, 5, 6],
    "P1": [0.004528, 0.005720, 0.005224, 0.004401, 0.005280, 0.008757],
    "P2": [0.002589, 0.002979, 0.002713, 0.002283, 0.002858, 0.004806],
    "P3": [0.000075, 0.001785, 0.001742, 0.001423, 0.001839, 0.003067],
    "P4": [0, 0.001301, 0.001273, 0.0010443, 0.001323, 0.002206],
    "P5": [0, 0.000085, 0.000081, 0.000066, 0.000086, 0.000139],
    "P6": [0, 0.000055, 0.000049, 0.000040, 0.000052, 0.000089],
}

datos_energia_peaje_distribucion_2024 = {
    "Segmento tarifario": [1, 2, 3, 4, 5, 6],
    "P1": [0.028553, 0.018254, 0.016675, 0.007471, 0.005119, 0],
    "P2": [0.016595, 0.009841, 0.008962, 0.004247, 0.002793, 0],
    "P3": [0.000482, 0.005788, 0.005652, 0.002663, 0.001764, 0],
    "P4": [0, 0.004194, 0.004103, 0.001730, 0.001336, 0],
    "P5": [0, 0.000339, 0.000325, 0.000183, 0.00005, 0],
    "P6": [0, 0.000179, 0.000163, 0.00005, 0.000088, 0],
}

datos_energia_cargos_2024 = {
    "Segmento tarifario": [1, 2, 3, 4, 5, 6],
    "Periodo 1": [0.043893, 0.024469, 0.013305, 0.006243, 0.005117, 0.001944],
    "Periodo 2": [0.008779, 0.018118, 0.009856, 0.004624, 0.003791, 0.001440],
    "Periodo 3": [0.002195, 0.009788, 0.005322, 0.002497, 0.002047, 0.000778],
    "Periodo 4": [0, 0.004894, 0.002661, 0.001249, 0.001023, 0.000389],
    "Periodo 5": [0, 0.003137, 0.001706, 0.000800, 0.000656, 0.000249],
    "Periodo 6": [0, 0.001958, 0.001064, 0.000499, 0.000409, 0.000156],
}

#Terminos en €/kwh en barras de central (es decir, sin elevar a perdidas)
datos_pagos_por_capacidad_2024 = {
    "Segmento tarifario": [1, 2, 3, 4, 5, 6],
    "Periodo 1": [0.000926, 0.001251, 0.000537, 0.000537, 0.000537, 0.000537],
    "Periodo 2": [0.000154, 0.000578, 0.000247, 0.000247, 0.000247, 0.000247],
    "Periodo 3": [0, 0.000385, 0.000165, 0.000165, 0.000165, 0.000165],
    "Periodo 4": [0, 0.000289, 0.000124, 0.000124, 0.000124, 0.000124],
    "Periodo 5": [0, 0.000289, 0.000124, 0.000124, 0.000124, 0.000124],
    "Periodo 6": [0, 0, 0, 0, 0, 0],
}

#
datos_coeficiente_perdidas_2024 = {
    "Segmento tarifario": [1, 2, 3, 4, 5, 6],
    "Periodo 1": [0.167, 0.166, 0.067, 0.052, 0.042, 0.016],
    "Periodo 2": [0.163, 0.175, 0.068, 0.054, 0.043, 0.016],
    "Periodo 3": [0.180, 0.165, 0.065, 0.049, 0.040, 0.016],
    "Periodo 4": [0, 0.165, 0.065, 0.050, 0.040, 0.016],
    "Periodo 5": [0, 0.138, 0.043, 0.035, 0.030, 0.015],
    "Periodo 6": [0, 0.180, 0.077, 0.054, 0.044, 0.017],
}

# Convertir el diccionario a un DataFrame
df_datos_potencia_peaje_transporte_2024 = pd.DataFrame(datos_potencia_peaje_transporte_2024)
df_datos_potencia_peaje_distribucion_2024 = pd.DataFrame(datos_potencia_peaje_distribucion_2024)
df_datos_energia_peaje_transporte_2024 = pd.DataFrame(datos_energia_peaje_transporte_2024)
df_datos_energia_peaje_distribucion_2024 = pd.DataFrame(datos_energia_peaje_distribucion_2024)
df_datos_potencia_cargos_2024 = pd.DataFrame(datos_potencia_cargos_2024)
df_datos_energia_cargos_2024 = pd.DataFrame(datos_energia_cargos_2024)
df_datos_pagos_por_capacidad_2024 = pd.DataFrame(datos_pagos_por_capacidad_2024)
df_datos_coeficiente_perdidas_2024 = pd.DataFrame(datos_coeficiente_perdidas_2024)

# Mostrar la tabla para verificar
print("datos_potencia_peaje_transporte_2024 cargados:\n", df_datos_potencia_peaje_transporte_2024)
print("datos_potencia_peaje_distribucion_2024 cargados:\n", df_datos_potencia_peaje_distribucion_2024)
print("datos_energia_peaje_transporte_2024 cargados:\n", df_datos_energia_peaje_transporte_2024)
print("datos_energia_peaje_distribucion_2024 cargados:\n", df_datos_energia_peaje_distribucion_2024)
print("datos_datos_potencia_cargos_2024 cargados:\n", df_datos_potencia_cargos_2024)
print("datos_energia_cargos_2024 cargados:\n", df_datos_energia_cargos_2024)
print("datos_pagos_por_capacidad_2024 cargados:\n", df_datos_pagos_por_capacidad_2024)
print("datos_coeficiente_perdidas_2024 cargados:\n", df_datos_coeficiente_perdidas_2024)




# Referencias
# https://www.boe.es/boe/dias/2023/12/25/pdfs/BOE-A-2023-26251.pdf  aqui estan los peajes de distribucion y transporte
# https://www.boe.es/boe/dias/2024/02/14/pdfs/BOE-A-2024-2774.pdf   aqui estan los pagos por capacidad y cargos
# https://www.boe.es/boe/dias/2020/01/24/pdfs/BOE-A-2020-1066.pdf   aqui estan los coeficientes de perdidas
