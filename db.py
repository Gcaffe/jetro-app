# db.py
"""
Archivo para la conexión centralizada a la base de datos MySQL.
"""
import pymysql

# Conexión a la base de datos
# Ajusta estos valores con tu configuración real
DB_HOST = "178.63.87.166"
DB_USER = "mlmdesal_macpela"
DB_PASSWORD = "Getsemani"
DB_NAME = "mlmdesal_jetro"
DB_PORT = 3306


def conectar_db():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT,
        cursorclass=pymysql.cursors.DictCursor
    )
