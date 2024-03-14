"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras_clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd
import re

def ingest_data():

    #
    # Inserte su código aquí
    #
    # Leer archivo de anchura fija (fwf) y asignar nombres a las columnas
    df=pd.read_fwf('clusters_report.txt', skiprows=4,header=None,index_col=False, names=['cluster','cantidad_de_palabras_clave','porcentaje_de_palabras_clave','principales_palabras_clave'])

    # Cada cierto numero de filas aparece un numero de cluster, una cantidad_de_palabras_clave, un porcentaje_de_palabras_clave y las palabras_clave
    # Las filas intermedias solo tienen palabras_clave
    # Rellenar clusters vacios con el valor de la fila anterior
    df['cluster']=df['cluster'].ffill()
    # Se concatenan las palabras clave de las filas intermedias con las palabras clave de la fila de cluster
    df['principales_palabras_clave']=df.groupby(['cluster'])['principales_palabras_clave'].transform(lambda x: ' '.join(x))
    # Se eliminan los puntos
    df['principales_palabras_clave']=df['principales_palabras_clave'].str.replace('.','')
    # Se eliminan las filas duplicadas
    df=df.drop_duplicates(subset=['cluster'])
    # Reiniciar el indice
    df=df.reset_index(drop=True) 
    #Convertir cantidad_de_palabras_clave a entero
    df['cantidad_de_palabras_clave']=df['cantidad_de_palabras_clave'].astype(int)
    #Eliminar porcentaje en los valores de porcentaje_de_palabras_clave
    df['porcentaje_de_palabras_clave']=df['porcentaje_de_palabras_clave'].str.replace('%','').str.replace(',','.').astype(float)
    # Eliminar espacios multiples en los strings de palabras_clave usando expresiones regulares
    df['principales_palabras_clave']=df['principales_palabras_clave'].replace(r'\s+',' ',regex=True) 

    return df