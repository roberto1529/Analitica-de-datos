import pyodbc
import pandas as pd
# link para descargar contralador : https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16#download-for-windows

# Datos de conexión a la base de datos
server = 'www.formap.co'
database = 'ForMapDW'
username = 'IsesAnaliticaPracticas'
password = '1s3s4n4l1t1c4Prc4t1c4s'
port = '1433'

# Establecer la conexión con SQL Server
conn = pyodbc.connect(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server},{port};DATABASE={database};UID={username};PWD={password}'
)

# Consultar datos desde las vistas
query_inspeccion_tecnica = "SELECT  * from ForMapDW.TRIPLEA.Vista_InspeccionTecnica vit"
query_acta_inspeccion = "SELECT  * from ForMapDW.TRIPLEA.Vista_ActaInspeccion vai"

# Leer los datos en DataFrames
df_inspeccion_tecnica = pd.read_sql(query_inspeccion_tecnica, conn)
df_acta_inspeccion = pd.read_sql(query_acta_inspeccion, conn)

# Cerrar la conexión a la base de datos
conn.close()

# Procesar los datos
# Calcular probabilidad de irregularidades por cliente usando las columnas de irregularidades
df_acta_irregularidades = df_acta_inspeccion[[
    'Póliza', 
    '¿Se_encuentra_irregularidad_en_acometida?', 
    '¿Se_encuentra_irregularidad_en_medidor?'
]]

# Convertir los valores Si/No a binarios (1/0)
df_acta_irregularidades['irregularidad_acometida'] = df_acta_irregularidades['¿Se_encuentra_irregularidad_en_acometida?'].map({'Si': 1, 'No': 0})
df_acta_irregularidades['irregularidad_medidor'] = df_acta_irregularidades['¿Se_encuentra_irregularidad_en_medidor?'].map({'Si': 1, 'No': 0})

# Calcular el total de irregularidades por cliente
df_acta_irregularidades['total_irregularidades'] = df_acta_irregularidades[['irregularidad_acometida', 'irregularidad_medidor']].sum(axis=1)

# Calcular la probabilidad de irregularidades por cliente (promedio)
df_probabilidad_irregularidades = df_acta_irregularidades.groupby('Póliza')['total_irregularidades'].mean().reset_index()
df_probabilidad_irregularidades.columns = ['Póliza', 'Probabilidad de Irregularidades']

# Identificar clientes recurrentes con irregularidades
df_recurrentes = df_acta_irregularidades[df_acta_irregularidades['total_irregularidades'] > 0]
df_recurrentes_count = df_recurrentes.groupby('Póliza').size().reset_index(name='Cantidad de Irregularidades')

# Ordenar los clientes con mayor probabilidad de irregularidades
df_top_clientes = df_probabilidad_irregularidades.sort_values(by='Probabilidad de Irregularidades', ascending=False)

# Guardar los resultados en archivos CSV
df_probabilidad_irregularidades.to_csv('probabilidad_irregularidades.csv', index=False)
df_recurrentes_count.to_csv('clientes_recurrentes.csv', index=False)
df_top_clientes.to_csv('top_clientes_irregularidades.csv', index=False)

print("Informe generado con éxito.")
