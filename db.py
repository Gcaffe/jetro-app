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
DB_PORT = 3306


def conectar_db(test_mode=False):
    """
    Conectar a la base de datos según el modo:
    test_mode=False: Producción (mlmdesal_berseba)
    test_mode=True: Prueba/Capacitación (mlmdesal_jetro)
    """
    
    # Determinar nombre de DB según modo
    if test_mode:
        db_name = "mlmdesal_jetro"    # Base de datos de prueba/capacitación
    else:
        db_name = "mlmdesal_berseba"  # Base de datos de producción real
    
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=db_name,
        port=DB_PORT,
        cursorclass=pymysql.cursors.DictCursor
    )