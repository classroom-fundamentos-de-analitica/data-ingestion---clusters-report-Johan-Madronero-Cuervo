"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd

import re
pattern = r'\n *\n'
pattern2 = r' +'
pattern3 = r' {2,}'

def formatear(entrada):
    salida = []
    salida.append(int(entrada[3:8]))
    salida.append(int(entrada[9:15]))
    salida.append(float(entrada[25:29].replace(',', '.')))
    fetch = entrada[41:]
    fetch = fetch.replace('\n', '')
    fetch = fetch.replace('.', '')
    fetch = re.sub(pattern2, ' ', fetch)
    salida.append(fetch)

    return salida


def ingest_data():

    #
    # Inserte su código aquí
    #

    with open ("clusters_report.txt", "r") as file:
        texto = file.readlines()

    # Formato de nombre columnas
    columnas = texto[:2]
    columnas = list(map(lambda x : x.replace("\n", ""), columnas))
    columnas = list(map(lambda x : x.strip(), columnas))
    columnas = list((map(lambda x : re.split(pattern3,x), columnas)))
    columnas1 = columnas[0]
    columnas2 = columnas[1]
    columnas1[1] = columnas1[1] + " " + columnas2[0]
    columnas1[2] = columnas1[2] + " " + columnas2[1]

    columnas1 = list(map(lambda x : x.lower(), columnas1))
    columnas1 = list(map(lambda x : x.replace(' ', '_'),columnas1))

    # Formato de filas
    texto = texto[4:]

    test = ""
    test = test.join(texto)
    test = re.split(pattern, test)

    test = list(filter(lambda x : x != '', test))


    test = list(map(formatear, test))

    df = pd.DataFrame(test, columns=columnas1)

    return(df)
