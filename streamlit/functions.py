#!/usr/bin/env python
# coding: utf-8

# # Funciones Útiles

# Este archivo contiene funciones útiles para el proceso de Extracción, Transformación y Carga (ETL).

# ## Importar Librerías

# In[2]:


import pandas as pd
import numpy as np
import mysql.connector
from sqlalchemy import create_engine


# ## Funciones

# In[3]:


def cargar_datos_desde_excel(archivo, hojas, engine='openpyxl'):
    """
    Carga datos desde un archivo Excel y devuelve un diccionario de DataFrames.

    Parameters:
    - archivo (str): Ruta del archivo Excel.
    - hojas (list): Lista de nombres de hojas a cargar.
    - engine (str, optional): Motor de Excel a utilizar. Por defecto, 'openpyxl'.

    Returns:
    dfs: Un diccionario donde las claves son los nombres de las hojas y los valores son DataFrames correspondientes.

    Example:
    >>> datos = cargar_datos_desde_excel('archivo.xlsx', ['Hoja1', 'Hoja2'])
    >>> df_hoja1 = datos['Hoja1']
    >>> df_hoja2 = datos['Hoja2']
    """
    
    xls_file = pd.ExcelFile(archivo, engine=engine)
    dfs = {}

    for hoja in hojas:
        df = pd.read_excel(xls_file, hoja) 
        dfs[hoja] = df

    return dfs


# In[4]:


def analizar_valores_sd(dataframe):
    """
    Analiza la presencia de valores 'SD' en cada columna del DataFrame.

    Parameters:
    dataframe (pd.DataFrame): El DataFrame a analizar.

    Returns:
    pd.DataFrame: Un DataFrame que muestra la cantidad y porcentaje de valores 'SD' en cada columna.
    """
    columnas_con_sd = dataframe.columns
    resultados = []

    for columna in columnas_con_sd:
        cantidad_sd = dataframe[columna].eq('SD').sum()
        porcentaje_sd = (cantidad_sd / len(dataframe)) * 100
        resultados.append({'Columna': columna, 'Cantidad de SD': cantidad_sd, 'Porcentaje de SD': porcentaje_sd})

    resultados_df = pd.DataFrame(resultados)
    resultados_con_sd = resultados_df[resultados_df['Cantidad de SD'] > 0]

    return resultados_con_sd


# In[1]:


def data_cleaning(df, drop_duplicates=False, drop_na=False, fill_na=None, convert_to_datetime=None, uppercase_columns=None,
                  lowercase_columns=None, titlecase_columns=None, strip_spaces=True, rename_columns=None, drop_columns=None,
                  categorize_columns=None, replace_values=None, new_columns=None, convert_date_columns=None, 
                  convert_to_int_columns=None, convert_to_float=None, new_columns2=None):
    """
    Realiza el proceso de limpieza de datos en un DataFrame.

    Parámetros:
    - df (pd.DataFrame): El DataFrame que se va a limpiar.
    
    - drop_duplicates (bool): Elimina duplicados si es True.
      Ejemplo: cleaned_df = data_cleaning(df_tu_data_frame, drop_duplicates=True)
    
    - drop_na (bool): Elimina filas con valores nulos si es True.
      Ejemplo: cleaned_df = data_cleaning(df_tu_data_frame, drop_na=True)

    - fill_na (dict): Un diccionario donde las claves son los nombres de columnas y los valores son valores para rellenar los nulos.
      Ejemplo: fill_na_dict = {'gravedad': 'leve'}
               cleaned_df = data_cleaning(df_tu_data_frame, fill_na=fill_na_dict)
        
    - convert_to_datetime (list): Lista de columnas para convertir a tipo de dato datetime.
      Ejemplo: columns_to_convert = ['fecha', 'hora']
               cleaned_df = data_cleaning(df_tu_data_frame, convert_to_datetime=columns_to_convert)

    - uppercase_columns (list): Lista de columnas para convertir a mayúsculas.
      Ejemplo: columns_to_uppercase = ['nombre', 'apellido']
               cleaned_df = data_cleaning(df_tu_data_frame, uppercase_columns=columns_to_uppercase)

    - lowercase_columns (list): Lista de columnas para convertir a minúsculas.
      Ejemplo: columns_to_lowercase = ['Ciudad', 'Pais']
               cleaned_df = data_cleaning(df_tu_data_frame, lowercase_columns=columns_to_lowercase)

    - titlecase_columns (list): Lista de columnas para convertir a formato de título (primera letra en mayúscula, resto en minúscula).
      Ejemplo: columns_to_titlecase = ['titulo', 'categoria']
               cleaned_df = data_cleaning(df_tu_data_frame, titlecase_columns=columns_to_titlecase)
            
    - strip_spaces (bool): Elimina espacios en blanco alrededor de los valores de las celdas si es True.
      Ejemplo: cleaned_df = data_cleaning(df_tu_data_frame, strip_spaces=True)

    - rename_columns (dict): Un diccionario donde las claves son los nombres de las columnas actuales y los valores son los nuevos nombres.
      Ejemplo: rename_dict = {'Vieja_Columna': 'Nueva_Columna'}
               cleaned_df = data_cleaning(df_tu_data_frame, rename_columns=rename_dict)
    
    - drop_columns (list): Lista de columnas para eliminar.
      Ejemplo: columns_to_drop = ['columna1', 'columna2']
               cleaned_df = data_cleaning(df_tu_data_frame, drop_columns=columns_to_drop)
        
    - categorize_columns (list): Lista de columnas para convertir a tipo de dato categoría.
      Ejemplo: columns_to_categorize = ['categoria1', 'categoria2']
               cleaned_df = data_cleaning(df_tu_data_frame, categorize_columns=columns_to_categorize)
        
    - replace_values (dict): Un diccionario donde las claves son los nombres de las columnas y los valores son diccionarios de reemplazo.
      Ejemplo: replace_dict = {'columna1': {'Antiguo1': 'Nuevo1', 'Antiguo2': 'Nuevo2'}}
               cleaned_df = data_cleaning(df_tu_data_frame, replace_values=replace_dict)

    - new_columns (dict): Un diccionario donde las claves son los nombres de las nuevas columnas y los valores son valores para esas columnas.
      Ejemplo: new_columns_dict = {'nueva_columna': 0}
               cleaned_df = data_cleaning(df_tu_data_frame, new_columns=new_columns_dict)
               
    - new_columns2 (dict): Un diccionario donde las claves son los nombres de las nuevas columnas y los valores son expresiones
                          para calcular el contenido de las nuevas columnas basadas en otras columnas existentes. 
      Ejemplo: {'nueva_columna1': 'columna_existente * 2'}
      cleaned_df = data_cleaning(df_tu_data_frame, new_columns2=new_columns_dict)

 
    - convert_date_columns (dict): Un diccionario donde las claves son los nombres de las columnas y los valores son los formatos de fecha.
      Ejemplo: date_columns_dict = {'fecha': '%Y-%m-%d', 'hora': '%H:%M:%S'}
               cleaned_df = data_cleaning(df_tu_data_frame, convert_date_columns=date_columns_dict)

    - convert_to_int_columns (list): Lista de columnas para convertir a tipo de dato entero.
      Ejemplo: columns_to_int = ['columna1', 'columna2']
               cleaned_df = data_cleaning(df_tu_data_frame, convert_to_int_columns=columns_to_int)
    
    - convert_to_float (list): Lista de columnas para convertir a tipo de dato float.
      Ejemplo: columns_to_float = ['columna1', 'columna2']
               cleaned_df = data_cleaning(df_tu_data_frame, convert_to_float=columns_to_float)          
            
    Retorna:
    pd.DataFrame: El DataFrame limpio.
    """

    cleaned_df = df.copy()

    # Eliminar duplicados
    if drop_duplicates:
        cleaned_df.drop_duplicates(inplace=True)
        

    # Eliminar filas con valores nulos
    if drop_na:
        cleaned_df.dropna(inplace=True)
        

    # Rellenar valores nulos
    if fill_na:
        cleaned_df.fillna(fill_na, inplace=True)

        
    # Convertir columnas a tipo datetime
    if convert_to_datetime:
        for column in convert_to_datetime:
            cleaned_df[column] = pd.to_datetime(cleaned_df[column], errors='coerce')
            

    # Convertir columnas a mayúsculas
    if uppercase_columns:
        for column in uppercase_columns:
            cleaned_df[column] = cleaned_df[column].str.upper()
            

    # Convertir columnas a minúsculas
    if lowercase_columns:
        for column in lowercase_columns:
            cleaned_df[column] = cleaned_df[column].str.lower()
        

    # Convertir columnas a formato de título
    if titlecase_columns:
        for column in titlecase_columns:
            cleaned_df[column] = cleaned_df[column].str.title()
            
            
    # Tratar columnas con espacios
    if strip_spaces:
        cleaned_df = cleaned_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        

    # Renombrar columnas
    if rename_columns:
        cleaned_df.rename(columns=rename_columns, inplace=True)
        
    
    # Eliminar columnas
    if drop_columns:
        cleaned_df.drop(columns=drop_columns, inplace=True)
        
        
    # Categorizar columnas
    if categorize_columns:
        for column in categorize_columns:
            if column in cleaned_df.columns:
                cleaned_df[column] = cleaned_df[column].astype('category')
            else:
                print(f"La columna '{column}' no existe en el DataFrame.")
                

    # Reemplazar valores en columnas
    if replace_values:
        for column, replacements in replace_values.items():
            cleaned_df[column].replace(replacements, inplace=True)
            
    # Agregar nuevas columnas
    if new_columns:
        for column, value in new_columns.items():
            cleaned_df[column] = value
            
    # Agregar nuevas columnas basadas en otras columnas
    if new_columns2:
        for new_column, column_expr in new_columns2.items():
            # Verificar si la expresión es proporcionada
            if column_expr:
                cleaned_df[new_column] = cleaned_df.eval(column_expr)
            else:
                cleaned_df[new_column] = None  # O cualquier valor predeterminado que prefieras
                  
    # Convertir columnas de fecha con formato específico
    if convert_date_columns:
        for column, date_format in convert_date_columns.items():
            cleaned_df[column] = pd.to_datetime(cleaned_df[column], format=date_format, errors='coerce')

    
    # Convertir columnas a tipo de dato entero
    if convert_to_int_columns:
        for column in convert_to_int_columns:
            cleaned_df[column] = pd.to_numeric(cleaned_df[column], errors='coerce').astype('Int64')
    
    
    # Convertir columnas a tipo float
    if convert_to_float:
        for column in convert_to_float:
            cleaned_df[column] = cleaned_df[column].astype(float)
        
            
    return cleaned_df


# In[1]:


def create_mysql_db(csv_file_path, db_name, table_name, host='localhost', user='tu_usuario', password='tu_contraseña'):
    """
    Crea una base de datos MySQL y una tabla a partir de un archivo CSV.

    Parameters:
    - csv_file_path (str): Ruta del archivo CSV.
    - db_name (str): Nombre de la base de datos a crear.
    - table_name (str): Nombre de la tabla a crear.
    - host (str, optional): Dirección del servidor MySQL. Por defecto, 'localhost'.
    - user (str, optional): Usuario de MySQL. Por defecto, 'tu_usuario'.
    - password (str, optional): Contraseña de MySQL. Por defecto, 'tu_contraseña'.

    Returns:
    None
    """
    try:
        # Validaciones
        if not csv_file_path.endswith('.csv'):
            raise ValueError("El archivo debe tener extensión CSV.")

        # Cargar CSV en un DataFrame
        df = pd.read_csv(csv_file_path)
        
        # Conectar a MySQL y crear la base de datos si no existe
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password
        )
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        cursor.close()
        connection.close()

        # Conectar a MySQL y crear la tabla si no existe
        engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:3306/{db_name}')
        connection = engine.connect()
        df.to_sql(table_name, connection, index=False, if_exists='replace')
        connection.close()

        print("Base de datos y tabla creadas exitosamente.")
    except pd.errors.EmptyDataError:
        raise ValueError("El archivo CSV está vacío.")
    except mysql.connector.Error as err:
        print(f"Error al conectar a MySQL: {err}")
        raise
    except Exception as e:
        print(f"Error inesperado: {e}")
        raise


# In[ ]:




