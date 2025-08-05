# C:\Proyectos\Jetro\BackEnd\main.py
from fastapi import FastAPI, HTTPException, Depends, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from login import router as login_router, get_current_user
from typing import List, Dict, Optional
from db import conectar_db
from datetime import datetime
from pathlib import Path
import pymysql
import os

app = FastAPI()

# CORS PRIMERO
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# RUTAS API ANTES DEL MOUNT
app.include_router(login_router)

# DETECTAR ENTORNO (PC o Servidor)
# En PC: la carpeta static está en el mismo directorio
# En Servidor: está en /app/jetro-backend/static
static_dir = Path("static")
if not static_dir.exists():
    # Estamos en el servidor
    static_dir = Path("/app/jetro-backend/static")

# Verificar que la carpeta static existe
if not static_dir.exists():
    print(f"⚠️  ADVERTENCIA: No se encuentra la carpeta static en {static_dir}")
    print("🔧 Asegúrate de tener la carpeta 'static' con los archivos del frontend")

# RUTA RAÍZ - Servir index.html
@app.get("/")
async def read_root():
    index_path = static_dir / "index.html"
    if index_path.exists():
        return FileResponse(str(index_path))
    else:
        raise HTTPException(status_code=404, detail="Frontend no encontrado")

# ARCHIVOS ESTÁTICOS
# Servir assets (CSS, JS)
if (static_dir / "assets").exists():
    app.mount("/assets", StaticFiles(directory=str(static_dir / "assets")), name="assets")

# Servir otros archivos estáticos (logos, imágenes)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static_files")

# INFO DE INICIO
@app.on_event("startup")
async def startup_event():
    print("🚀 FastAPI iniciado correctamente")
    print(f"📁 Sirviendo archivos estáticos desde: {static_dir.absolute()}")
    print(f"🌐 Frontend disponible en: http://localhost:8000/")
    print(f"📚 Documentación API: http://localhost:8000/docs")

# ================== MODELOS PARA IGLESIAS ==================
class Ingreso(BaseModel):
    fecha: str
    monto: float
    tipo: str
    descripcion: str
    iglesia_id: int

class LocalBase(BaseModel):
    LoCod: int
    LoNombre: str
    LoPasAdm: Optional[str] = None
    LoCalle: Optional[str] = None
    LoCP: Optional[str] = None
    LoCiudad: Optional[str] = None
    LoProvincia: Optional[str] = None
    LoPais: Optional[str] = None
    LoTelefono: Optional[str] = None
    LoSituacion: int = 1

class LocalCreate(LocalBase):
    pass

class LocalUpdate(LocalBase):
    LoID: int

# ================== MODELOS PARA FIELES ==================
from datetime import datetime

class FielBase(BaseModel):
    fiSede: int
    fiCod: int
    fiNIF: Optional[str] = None
    fiNombres: str
    fiApellidos: str
    fiNacidoEn: Optional[str] = None
    fiFecNacido: Optional[str] = None  # Fecha como string "YYYY-MM-DD"
    fiDiezmo: float = 0.0
    fiDirec1: Optional[str] = None
    fiDirec2: Optional[str] = None
    fiPostal: Optional[str] = None
    fiCiudad: Optional[str] = None
    fiTelefono: Optional[str] = None
    fieMail: Optional[str] = None
    fiDesde: Optional[str] = None  # Fecha como string "YYYY-MM-DD"
    fiPasaporte: Optional[str] = None
    fiNacionalidad: Optional[str] = None
    fiEstadoCivil: Optional[str] = "S"
    fiComentario: Optional[str] = None
    Situacion: int = 1

class FielCreate(FielBase):
    pass

class FielUpdate(FielBase):
    fiID: int

# ================== MODELOS PARA USUARIOS ==================
import bcrypt

class UsuarioBase(BaseModel):
    UsCod: str
    UsSedes: str
    UsNivel: int
    UsNombre: str
    UsKeyWeb: Optional[str] = None  # Password - opcional en algunos casos
    UsPermisos: str = ""
    UsFecha: Optional[str] = None
    UsBaja: Optional[str] = None
    UsActivo: int = 1
    UsMail: Optional[str] = None
    UsIntentos: int = 0

class UsuarioCreate(UsuarioBase):
    pass

class UsuarioUpdate(UsuarioBase):
    UsID: int

# ================== MODELOS PARA CARGA SEGUNDO NIVEL ==================
class SegundoNivelResponse(BaseModel):
    MnuCod: int
    MnuNombre: str
    MnuSigAccion: Optional[str] = None

class SegundoNivelResult(BaseModel):
    success: bool
    results: List[SegundoNivelResponse]

# ================== MODELO PARA GRABAR MOVIMIENTO =======================
class MovimientoCreate(BaseModel):
    sede: int
    tipoOperacion: int
    segundoNivel: Optional[int] = None
    tercerNivel: Optional[int] = None
    descripcion: str
    dia: int
    mes: int
    año: int
    importe: float
    origen: str
    saldoCaja: float = 0      # ✅ NUEVO
    saldoBanco: float = 0     # ✅ NUEVO
    moDona: Optional[int] = 0      # Para donantes
    moPers: Optional[int] = 0      # Para fieles/pastores
    moSedeDes: Optional[int] = 0  # Para locales


# ================== MODELO PARA CREAR CIERRE =======================
class CierreCreate(BaseModel):
    sede: int
    anyo: int
    mes: int
    saldoCaja: float
    saldoBanco: float
    usuario: str    

# ================== MODELO PARA CREAR CIERRE TEMPORAL =======================
class CierreCreateTemporal(BaseModel):
    sede: int
    anyo: int
    mes: int
    usuario: str

# ================== MODELOS PARA REPORTES ==================
class ReporteIngresosGastosRequest(BaseModel):
    codigoSede: int
    fechaInicial: str
    fechaFinal: str
    soloDomingos: bool = False
    tipoMovimiento: Optional[str] = None
    formaPago: Optional[str] = None

# Añadir después del modelo existente
class ReporteDiezmosOfrendasRequest(BaseModel):
    codigoSede: int
    fechaInicial: str
    fechaFinal: str
    soloDomingos: bool = False

# ================== MODELOS PARA DIEZMOS X PERSONA ==================
class ReporteDiezmosPorPersonaRequest(BaseModel):
    codigoSede: int
    año: int

# ================== MODELO PARA TRANSPOSICIÓN DATOS ECONÓMICOS ==================
class TransposicionRequest(BaseModel):
    codigoSede: int
    año: int
    limpiarTabla: bool = True  # Para limpiar tabla antes de insertar

# ================== MODELO PARA REPORTE ECONÓMICO FINAL ==================
class ReporteEconomicoFinalRequest(BaseModel):
    codigoSede: int
    año: int
    aplicarPostProceso: bool = True  # Para aplicar las reglas del PDF

# ============ FUNCION PARA CALCULAR TOTALES ==================
def calcular_resumen_movimientos(movimientos):
    """Función auxiliar para calcular totales"""
    resumen = {
        'totalCaja': 0,
        'totalBanco': 0,
        'totalGeneral': 0,
        'totalIngresos': 0,
        'totalGastos': 0,
        'cantidadMovimientos': len(movimientos),
        'saldoNeto': 0
    }
    
    for mov in movimientos:
        caja = float(mov.get('Caja', 0) or 0)
        banco = float(mov.get('Banco', 0) or 0)
        total = caja + banco
        
        resumen['totalCaja'] += caja
        resumen['totalBanco'] += banco
        resumen['totalGeneral'] += total
        
        if total > 0:
            resumen['totalIngresos'] += total
        else:
            resumen['totalGastos'] += abs(total)
    
    resumen['saldoNeto'] = resumen['totalIngresos'] - resumen['totalGastos']
    return resumen

# ============ FUNCION PARA VERIFICAR PERMISOS DE ADMINISTRADOR ==================
def verificar_admin(auth_user):
    if auth_user.get("nivel") != 9:
        raise HTTPException(status_code=403, detail="Solo los administradores pueden realizar esta acción")

# ============ FUNCION PARA CONVERTIR TEXTO A MAYÚSCULAS ==================
def convertir_campos_texto_mayusculas(modelo_data):
    """Convierte campos de texto a mayúsculas, preservando números, fechas, etc."""
    campos_a_convertir = [
        'descripcion', 'MoDesc', 'fiNombres', 'fiApellidos', 'fiDirec1', 'fiDirec2', 
        'fiCiudad', 'LoNombre', 'LoCalle', 'LoCiudad', 'LoProvincia', 'LoPais',
        'UsNombre', 'fiComentario', 'fiNacidoEn', 'fiNacionalidad'
    ]
    
    for campo in campos_a_convertir:
        if hasattr(modelo_data, campo):
            valor = getattr(modelo_data, campo)
            if valor and isinstance(valor, str):
                setattr(modelo_data, campo, valor.upper())
    
    return modelo_data

# ================== ENDPOINTS PARA IGLESIAS ==================
@app.get("/locales/{sede_ids}")
def obtener_locales(sede_ids: str, auth=Depends(get_current_user), test_mode: bool = False):
    """Obtener todos los locales de las sedes especificadas"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Obteniendo locales para sedes: {sede_ids}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)  # Corregido para PyMySQL
        
        # Usar la misma lógica que ya tienes en login.py
        if sede_ids == "999":
            cursor.execute("SELECT * FROM locales WHERE LoSituacion=1 ORDER BY LoNombre")
        elif "," in sede_ids:
            ids = tuple(sede_ids.split(","))
            placeholders = ",".join(["%s"] * len(ids))
            sql = f"SELECT * FROM locales WHERE LoSituacion=1 AND LoCod IN ({placeholders}) ORDER BY LoNombre"
            cursor.execute(sql, ids)
        else:
            cursor.execute("SELECT * FROM locales WHERE LoSituacion=1 AND LoCod = %s ORDER BY LoNombre", (sede_ids,))
        
        locales = cursor.fetchall()
        print(f"✅ Encontrados {len(locales)} locales")
        return locales
        
    except Exception as e:
        print(f"❌ ERROR obteniendo locales: {e}")
        raise HTTPException(status_code=500, detail=f"Error al obtener locales: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== ENDPOINT PARA DETALLES DEL LOCAL ==================
@app.get("/locales/detalle/{local_id}")
def obtener_local_detalle(local_id: int, auth=Depends(get_current_user), test_mode: bool = False):
    """Obtener un local específico por ID"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Obteniendo detalle del local ID: {local_id}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)  # Corregido para PyMySQL
        
        cursor.execute("SELECT * FROM locales WHERE LoID = %s", (local_id,))
        local = cursor.fetchone()
        
        if not local:
            raise HTTPException(status_code=404, detail="Local no encontrado")
        
        print("✅ Local encontrado")
        return local
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ ERROR obteniendo local: {e}")
        raise HTTPException(status_code=500, detail=f"Error al obtener local: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== ENDPOINT PARA NUEVO CODIGO DE LOCAL ==================
@app.get("/nuevo-codigo-local")
def obtener_nuevo_codigo(auth=Depends(get_current_user), test_mode: bool = False):
    conn = None
    cursor = None
    try:
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(LoCod)+1 as nuevoCodigo FROM locales")
        resultado = cursor.fetchone()
        nuevo_codigo = resultado[0] if resultado[0] else 1
        return {"nuevoCodigo": nuevo_codigo}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# ================== ENDPOINT PARA CREAR UN LOCAL ==================        
@app.post("/locales")
def crear_local(local: LocalCreate, auth=Depends(get_current_user), test_mode: bool = False):
    """Crear un nuevo local"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Creando nuevo local: {local.LoNombre}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)  # Corregido para PyMySQL
        
        # Verificar si el código ya existe
        cursor.execute("SELECT LoID FROM locales WHERE LoCod = %s", (local.LoCod,))
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="El código de local ya existe")
        
        sql = """
        INSERT INTO locales (LoCod, LoNombre, LoPasAdm, LoCalle, LoCP, LoCiudad, 
                             LoProvincia, LoPais, LoTelefono, LoSituacion)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        valores = (
            local.LoCod, local.LoNombre, local.LoPasAdm,
            local.LoCalle, local.LoCP, local.LoCiudad, local.LoProvincia,
            local.LoPais, local.LoTelefono, local.LoSituacion
        )
        
        cursor.execute(sql, valores)
        conn.commit()
        new_id = conn.insert_id()  # Corregido para PyMySQL
        
        # Obtener el registro creado
        cursor.execute("SELECT * FROM locales WHERE LoID = %s", (new_id,))
        nuevo_local = cursor.fetchone()
        
        print("✅ Local creado correctamente")
        return nuevo_local
        
    except HTTPException:
        raise
    except pymysql.IntegrityError as e:
        if "Duplicate entry" in str(e):
            raise HTTPException(status_code=400, detail="El código de local ya existe")
        raise HTTPException(status_code=400, detail=f"Error de integridad: {str(e)}")
    except Exception as e:
        print(f"❌ ERROR creando local: {e}")
        raise HTTPException(status_code=500, detail=f"Error al crear local: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== ENDPOINT PARA ACTUALIZAR UN LOCAL ==================
@app.put("/locales/{local_id}")
def actualizar_local(local_id: int, local: LocalUpdate, auth=Depends(get_current_user), test_mode: bool = False):
    """Actualizar un local existente"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Actualizando local ID: {local_id}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)  # Corregido para PyMySQL
        
        # Verificar que el local existe
        cursor.execute("SELECT LoID FROM locales WHERE LoID = %s", (local_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Local no encontrado")
        
        # Verificar si el código ya existe en otro registro
        cursor.execute("SELECT LoID FROM locales WHERE LoCod = %s AND LoID != %s", (local.LoCod, local_id))
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="El código de local ya existe")
        
        sql = """
        UPDATE locales SET 
            LoCod = %s, LoNombre = %s, LoPasAdm = %s, LoCalle = %s, LoCP = %s, 
            LoCiudad = %s, LoProvincia = %s, LoPais = %s, LoTelefono = %s, LoSituacion = %s
        WHERE LoID = %s
        """
        
        valores = (
            local.LoCod, local.LoNombre, local.LoPasAdm,
            local.LoCalle, local.LoCP, local.LoCiudad, local.LoProvincia,
            local.LoPais, local.LoTelefono, local.LoSituacion, local_id
        )
        
        cursor.execute(sql, valores)
        conn.commit()
        
        # Obtener el registro actualizado
        cursor.execute("SELECT * FROM locales WHERE LoID = %s", (local_id,))
        local_actualizado = cursor.fetchone()
        
        print("✅ Local actualizado correctamente")
        return local_actualizado
        
    except HTTPException:
        raise
    except pymysql.IntegrityError as e:
        if "Duplicate entry" in str(e):
            raise HTTPException(status_code=400, detail="El código de local ya existe")
        raise HTTPException(status_code=400, detail=f"Error de integridad: {str(e)}")
    except Exception as e:
        print(f"❌ ERROR actualizando local: {e}")
        raise HTTPException(status_code=500, detail=f"Error al actualizar local: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== ENDPOINT PARA ELIMINAR UN LOCAL ==================
@app.delete("/locales/{local_id}")
def eliminar_local(local_id: int, auth=Depends(get_current_user), test_mode: bool = False):
    """Eliminar un local (soft delete - cambiar situación a 0)"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Eliminando local ID: {local_id}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)  # Corregido para PyMySQL
        
        # Verificar que el local existe
        cursor.execute("SELECT LoID, LoNombre FROM locales WHERE LoID = %s", (local_id,))
        local = cursor.fetchone()
        if not local:
            raise HTTPException(status_code=404, detail="Local no encontrado")
        
        # Soft delete - cambiar situación a 0
        cursor.execute("UPDATE locales SET LoSituacion = 0 WHERE LoID = %s", (local_id,))
        conn.commit()
        
        print(f"✅ Local '{local['LoNombre']}' eliminado correctamente")
        return {"message": f"Local '{local['LoNombre']}' eliminado correctamente"}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ ERROR eliminando local: {e}")
        raise HTTPException(status_code=500, detail=f"Error al eliminar local: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== ENDPOINTS PARA FIELES ==================
@app.get("/fieles/{sede_id}")
def obtener_fieles(sede_id: str, auth=Depends(get_current_user), test_mode: bool = False):
    """Obtener todos los fieles de la sede especificada"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Obteniendo fieles para sede: {sede_id}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        if sede_id == "999":
            cursor.execute("SELECT * FROM fieles WHERE Situacion=1 ORDER BY fiApellidos, fiNombres")
        else:
            cursor.execute("SELECT * FROM fieles WHERE Situacion=1 AND fiSede = %s ORDER BY fiApellidos, fiNombres", (sede_id,))
        
        fieles = cursor.fetchall()
        print(f"✅ Encontrados {len(fieles)} fieles")
        return fieles
        
    except Exception as e:
        print(f"❌ ERROR obteniendo fieles: {e}")
        raise HTTPException(status_code=500, detail=f"Error al obtener fieles: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== ENDPOINT PARA OBTERNE DETALLES DEL FIEL ==================
@app.get("/fieles/detalle/{fiel_id}")
def obtener_fiel_detalle(fiel_id: int, auth=Depends(get_current_user), test_mode: bool = False):
    """Obtener un fiel específico por ID"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Obteniendo detalle del fiel ID: {fiel_id}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        cursor.execute("SELECT * FROM fieles WHERE fiID = %s", (fiel_id,))
        fiel = cursor.fetchone()
        
        if not fiel:
            raise HTTPException(status_code=404, detail="Fiel no encontrado")
        
        print("✅ Fiel encontrado")
        return fiel
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ ERROR obteniendo fiel: {e}")
        raise HTTPException(status_code=500, detail=f"Error al obtener fiel: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== ENDPOINT PARA OBTENER NUEVO CODIGO PARA FIEL ==================
@app.get("/nuevo-codigo-fiel")
def obtener_nuevo_codigo_fiel(auth=Depends(get_current_user), test_mode: bool = False):
    """Obtener el siguiente código disponible para fiel"""
    conn = None
    cursor = None
    try:
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)  # ← Esto hace que sea diccionario
        cursor.execute("SELECT MAX(fiCod)+1 as nuevoCodigo FROM fieles")
        resultado = cursor.fetchone()
        nuevo_codigo = resultado.get('nuevoCodigo', 1001)  # ← Corregido
        print(f"▶️ Nuevo código del fiel: {nuevo_codigo}")
        return {"nuevoCodigo": nuevo_codigo}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# ================== ENDPOINT PARA CREAR UN FIEL ==================
@app.post("/fieles")
def crear_fiel(fiel: FielCreate, auth=Depends(get_current_user), test_mode: bool = False):
    fiel = convertir_campos_texto_mayusculas(fiel)
    conn = None
    cursor = None
    try:
        print(f"▶️ Creando nuevo fiel: {fiel.fiNombres} {fiel.fiApellidos}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # Verificar si el código ya existe
        cursor.execute("SELECT fiID FROM fieles WHERE fiCod = %s", (fiel.fiCod,))
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="El código de fiel ya existe")
        
        # Convertir fechas None a NULL para MySQL
        fec_nacido = fiel.fiFecNacido if fiel.fiFecNacido else None
        fec_desde = fiel.fiDesde if fiel.fiDesde else None
        
        sql = """
        INSERT INTO fieles (fiSede, fiCod, fiNIF, fiNombres, fiApellidos, fiNacidoEn, 
                           fiFecNacido, fiDiezmo, fiDirec1, fiDirec2, fiPostal, fiCiudad, 
                           fiTelefono, fieMail, fiDesde, fiPasaporte, fiNacionalidad, 
                           fiEstadoCivil, fiComentario, Situacion)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        valores = (
            fiel.fiSede, fiel.fiCod, fiel.fiNIF, fiel.fiNombres, fiel.fiApellidos,
            fiel.fiNacidoEn, fec_nacido, fiel.fiDiezmo, fiel.fiDirec1, fiel.fiDirec2,
            fiel.fiPostal, fiel.fiCiudad, fiel.fiTelefono, fiel.fieMail, fec_desde,
            fiel.fiPasaporte, fiel.fiNacionalidad, fiel.fiEstadoCivil, fiel.fiComentario,
            fiel.Situacion
        )
        
        cursor.execute(sql, valores)
        conn.commit()
        new_id = conn.insert_id()
        
        # Obtener el registro creado
        cursor.execute("SELECT * FROM fieles WHERE fiID = %s", (new_id,))
        nuevo_fiel = cursor.fetchone()
        
        print("✅ Fiel creado correctamente")
        return nuevo_fiel
        
    except HTTPException:
        raise
    except pymysql.IntegrityError as e:
        if "Duplicate entry" in str(e):
            raise HTTPException(status_code=400, detail="El código de fiel ya existe")
        raise HTTPException(status_code=400, detail=f"Error de integridad: {str(e)}")
    except Exception as e:
        print(f"❌ ERROR creando fiel: {e}")
        raise HTTPException(status_code=500, detail=f"Error al crear fiel: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== ENDPOINT PARA ACTUALIZAR FIEL ==================
@app.put("/fieles/{fiel_id}")
def actualizar_fiel(fiel_id: int, fiel: FielUpdate, auth=Depends(get_current_user), test_mode: bool = False):
    fiel = convertir_campos_texto_mayusculas(fiel)
    conn = None
    cursor = None
    try:
        print(f"▶️ Actualizando fiel ID: {fiel_id}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # Verificar que el fiel existe
        cursor.execute("SELECT fiID FROM fieles WHERE fiID = %s", (fiel_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Fiel no encontrado")
        
        # Verificar si el código ya existe en otro registro
        cursor.execute("SELECT fiID FROM fieles WHERE fiCod = %s AND fiID != %s", (fiel.fiCod, fiel_id))
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="El código de fiel ya existe")
        
        # Convertir fechas None a NULL para MySQL
        fec_nacido = fiel.fiFecNacido if fiel.fiFecNacido else None
        fec_desde = fiel.fiDesde if fiel.fiDesde else None
        
        sql = """
        UPDATE fieles SET 
            fiSede = %s, fiCod = %s, fiNIF = %s, fiNombres = %s, fiApellidos = %s,
            fiNacidoEn = %s, fiFecNacido = %s, fiDiezmo = %s, fiDirec1 = %s, fiDirec2 = %s,
            fiPostal = %s, fiCiudad = %s, fiTelefono = %s, fieMail = %s, fiDesde = %s,
            fiPasaporte = %s, fiNacionalidad = %s, fiEstadoCivil = %s, fiComentario = %s,
            Situacion = %s
        WHERE fiID = %s
        """
        
        valores = (
            fiel.fiSede, fiel.fiCod, fiel.fiNIF, fiel.fiNombres, fiel.fiApellidos,
            fiel.fiNacidoEn, fec_nacido, fiel.fiDiezmo, fiel.fiDirec1, fiel.fiDirec2,
            fiel.fiPostal, fiel.fiCiudad, fiel.fiTelefono, fiel.fieMail, fec_desde,
            fiel.fiPasaporte, fiel.fiNacionalidad, fiel.fiEstadoCivil, fiel.fiComentario,
            fiel.Situacion, fiel_id
        )
        
        cursor.execute(sql, valores)
        conn.commit()
        
        # Obtener el registro actualizado
        cursor.execute("SELECT * FROM fieles WHERE fiID = %s", (fiel_id,))
        fiel_actualizado = cursor.fetchone()
        
        print("✅ Fiel actualizado correctamente")
        return fiel_actualizado
        
    except HTTPException:
        raise
    except pymysql.IntegrityError as e:
        if "Duplicate entry" in str(e):
            raise HTTPException(status_code=400, detail="El código de fiel ya existe")
        raise HTTPException(status_code=400, detail=f"Error de integridad: {str(e)}")
    except Exception as e:
        print(f"❌ ERROR actualizando fiel: {e}")
        raise HTTPException(status_code=500, detail=f"Error al actualizar fiel: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== ENDPOINT PARA ELIMINAR FIEL ==================
@app.delete("/fieles/{fiel_id}")
def eliminar_fiel(fiel_id: int, auth=Depends(get_current_user), test_mode: bool = False):
    """Eliminar un fiel (soft delete - cambiar situación a 0)"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Eliminando fiel ID: {fiel_id}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # Verificar que el fiel existe
        cursor.execute("SELECT fiID, fiNombres, fiApellidos FROM fieles WHERE fiID = %s", (fiel_id,))
        fiel = cursor.fetchone()
        if not fiel:
            raise HTTPException(status_code=404, detail="Fiel no encontrado")
        
        # Soft delete - cambiar situación a 0
        cursor.execute("UPDATE fieles SET Situacion = 0 WHERE fiID = %s", (fiel_id,))
        conn.commit()
        
        print(f"✅ Fiel '{fiel['fiNombres']} {fiel['fiApellidos']}' eliminado correctamente")
        return {"message": f"Fiel '{fiel['fiNombres']} {fiel['fiApellidos']}' eliminado correctamente"}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ ERROR eliminando fiel: {e}")
        raise HTTPException(status_code=500, detail=f"Error al eliminar fiel: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== ENDPOINTS PARA USUARIOS ==================
@app.get("/usuarios")
def obtener_usuarios(auth=Depends(get_current_user), test_mode: bool = False):
    """Obtener todos los usuarios - Solo administradores"""
    conn = None
    cursor = None
    try:
        # Verificar que sea administrador
        verificar_admin(auth)
        
        print("▶️ Obteniendo usuarios")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        cursor.execute("""
            SELECT UsID, UsCod, UsSedes, UsNivel, UsNombre, UsPermisos, 
                   UsFecha, UsBaja, UsActivo, UsMail, UsIntentos 
            FROM usuarios 
            ORDER BY UsNombre
        """)
        
        usuarios = cursor.fetchall()
        print(f"✅ Encontrados {len(usuarios)} usuarios")
        return usuarios
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ ERROR obteniendo usuarios: {e}")
        raise HTTPException(status_code=500, detail=f"Error al obtener usuarios: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== ENDPOINT PARA OBTENER DETALLE DEL USUARIO ==================
@app.get("/usuarios/detalle/{usuario_id}")
def obtener_usuario_detalle(usuario_id: int, auth=Depends(get_current_user), test_mode: bool = False):
    """Obtener un usuario específico por ID - Solo administradores"""
    conn = None
    cursor = None
    try:
        verificar_admin(auth)
        
        print(f"▶️ Obteniendo detalle del usuario ID: {usuario_id}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        cursor.execute("""
            SELECT UsID, UsCod, UsSedes, UsNivel, UsNombre, UsPermisos, 
                   UsFecha, UsBaja, UsActivo, UsMail, UsIntentos 
            FROM usuarios WHERE UsID = %s
        """, (usuario_id,))
        
        usuario = cursor.fetchone()
        
        if not usuario:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        
        print("✅ Usuario encontrado")
        return usuario
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ ERROR obteniendo usuario: {e}")
        raise HTTPException(status_code=500, detail=f"Error al obtener usuario: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== ENDPOINT PARA OBTENER NUEVO CODIGO DE USUARIO ==================
@app.get("/nuevo-codigo-usuario")
def obtener_nuevo_codigo_usuario(auth=Depends(get_current_user), test_mode: bool = False):
    """Obtener el siguiente código disponible para usuario"""
    conn = None
    cursor = None
    try:
        verificar_admin(auth)
        
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor()
        
        # Buscar el siguiente código alfanumérico disponible
        cursor.execute("SELECT UsCod FROM usuarios ORDER BY UsCod DESC LIMIT 1")
        resultado = cursor.fetchone()
        
        if resultado and resultado[0]:
            ultimo_codigo = resultado[0]
            # Extraer número del código (ej: USR001 -> 001)
            if ultimo_codigo.startswith('USR'):
                numero = int(ultimo_codigo[3:]) + 1
            else:
                numero = 1
        else:
            numero = 1
            
        nuevo_codigo = f"USR{numero:03d}"
        return {"nuevoCodigo": nuevo_codigo}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# ================== ENDPOINT PARA CREAR USUARIO ==================
@app.post("/usuarios")
def crear_usuario(usuario: UsuarioCreate, auth=Depends(get_current_user), test_mode: bool = False):
    usuario = convertir_campos_texto_mayusculas(usuario)
    """Crear un nuevo usuario - Solo administradores"""
    conn = None
    cursor = None
    try:
        verificar_admin(auth)
        
        print(f"▶️ Creando nuevo usuario: {usuario.UsNombre}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # Verificar si el código ya existe
        cursor.execute("SELECT UsID FROM usuarios WHERE UsCod = %s", (usuario.UsCod,))
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="El código de usuario ya existe")
        
        # Convertir fechas None a NULL para MySQL
        fecha_alta = usuario.UsFecha if usuario.UsFecha else None
        fecha_baja = usuario.UsBaja if usuario.UsBaja else None
        
        sql = """
        INSERT INTO usuarios (UsCod, UsSedes, UsNivel, UsNombre, UsKeyWeb, UsPermisos, 
                             UsFecha, UsBaja, UsActivo, UsMail, UsIntentos)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        valores = (
            usuario.UsCod, usuario.UsSedes, usuario.UsNivel, usuario.UsNombre.upper(),
            None,  # UsKeyWeb se manejará por separado en el login
            usuario.UsPermisos, fecha_alta, fecha_baja, usuario.UsActivo,
            usuario.UsMail, usuario.UsIntentos
        )
        
        cursor.execute(sql, valores)
        conn.commit()
        new_id = conn.insert_id()
        
        # Obtener el registro creado (sin password)
        cursor.execute("""
            SELECT UsID, UsCod, UsSedes, UsNivel, UsNombre, UsPermisos, 
                   UsFecha, UsBaja, UsActivo, UsMail, UsIntentos 
            FROM usuarios WHERE UsID = %s
        """, (new_id,))
        nuevo_usuario = cursor.fetchone()
        
        print("✅ Usuario creado correctamente")
        return nuevo_usuario
        
    except HTTPException:
        raise
    except pymysql.IntegrityError as e:
        if "Duplicate entry" in str(e):
            raise HTTPException(status_code=400, detail="El código de usuario ya existe")
        raise HTTPException(status_code=400, detail=f"Error de integridad: {str(e)}")
    except Exception as e:
        print(f"❌ ERROR creando usuario: {e}")
        raise HTTPException(status_code=500, detail=f"Error al crear usuario: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== ENDPOINT PARA ACTUALIZAR USUARIO - SOLO ADMINISTRADORES==================
@app.put("/usuarios/{usuario_id}")
def actualizar_usuario(usuario_id: int, usuario: UsuarioUpdate, auth=Depends(get_current_user), test_mode: bool = False):
    usuario = convertir_campos_texto_mayusculas(usuario)
    conn = None
    cursor = None
    try:
        verificar_admin(auth)
        
        print(f"▶️ Actualizando usuario ID: {usuario_id}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # Verificar que el usuario existe
        cursor.execute("SELECT UsID FROM usuarios WHERE UsID = %s", (usuario_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        
        # Verificar si el código ya existe en otro registro
        cursor.execute("SELECT UsID FROM usuarios WHERE UsCod = %s AND UsID != %s", (usuario.UsCod, usuario_id))
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="El código de usuario ya existe")
        
        # Convertir fechas None a NULL para MySQL
        fecha_alta = usuario.UsFecha if usuario.UsFecha else None
        fecha_baja = usuario.UsBaja if usuario.UsBaja else None
        
        sql = """
        UPDATE usuarios SET 
            UsCod = %s, UsSedes = %s, UsNivel = %s, UsNombre = %s, UsPermisos = %s,
            UsFecha = %s, UsBaja = %s, UsActivo = %s, UsMail = %s, UsIntentos = %s
        WHERE UsID = %s
        """
        
        valores = (
            usuario.UsCod, usuario.UsSedes, usuario.UsNivel, usuario.UsNombre.upper(),
            usuario.UsPermisos, fecha_alta, fecha_baja, usuario.UsActivo,
            usuario.UsMail, usuario.UsIntentos, usuario_id
        )
        
        cursor.execute(sql, valores)
        conn.commit()
        
        # Obtener el registro actualizado (sin password)
        cursor.execute("""
            SELECT UsID, UsCod, UsSedes, UsNivel, UsNombre, UsPermisos, 
                   UsFecha, UsBaja, UsActivo, UsMail, UsIntentos 
            FROM usuarios WHERE UsID = %s
        """, (usuario_id,))
        usuario_actualizado = cursor.fetchone()
        
        print("✅ Usuario actualizado correctamente")
        return usuario_actualizado
        
    except HTTPException:
        raise
    except pymysql.IntegrityError as e:
        if "Duplicate entry" in str(e):
            raise HTTPException(status_code=400, detail="El código de usuario ya existe")
        raise HTTPException(status_code=400, detail=f"Error de integridad: {str(e)}")
    except Exception as e:
        print(f"❌ ERROR actualizando usuario: {e}")
        raise HTTPException(status_code=500, detail=f"Error al actualizar usuario: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== ENDPOINT PARA ELIMINAR USUARIO ==================
@app.delete("/usuarios/{usuario_id}")
def eliminar_usuario(usuario_id: int, auth=Depends(get_current_user), test_mode: bool = False):
    """Eliminar un usuario (soft delete - cambiar UsActivo a 0)"""
    conn = None
    cursor = None
    try:
        verificar_admin(auth)
        
        print(f"▶️ Eliminando usuario ID: {usuario_id}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # Verificar que el usuario existe
        cursor.execute("SELECT UsID, UsNombre FROM usuarios WHERE UsID = %s", (usuario_id,))
        usuario = cursor.fetchone()
        if not usuario:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        
        # Verificar que no es el mismo usuario autenticado
        if auth.get("id") == usuario_id:
            raise HTTPException(status_code=400, detail="No puedes eliminar tu propio usuario")
        
        # Soft delete - cambiar UsActivo a 0
        cursor.execute("UPDATE usuarios SET UsActivo = 0 WHERE UsID = %s", (usuario_id,))
        conn.commit()
        
        print(f"✅ Usuario '{usuario['UsNombre']}' eliminado correctamente")
        return {"message": f"Usuario '{usuario['UsNombre']}' eliminado correctamente"}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ ERROR eliminando usuario: {e}")
        raise HTTPException(status_code=500, detail=f"Error al eliminar usuario: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== ENDPOINT PARA CAMBIAR CONTRASEÑA ==================
@app.put("/usuarios/{usuario_id}/password")
def cambiar_password(usuario_id: int, nueva_password: str, auth=Depends(get_current_user), test_mode: bool = False):
    """Cambiar contraseña de un usuario - Solo administradores o el propio usuario"""
    conn = None
    cursor = None
    try:
        # Verificar permisos: admin o el propio usuario
        if auth.get("nivel") != 9 and auth.get("id") != usuario_id:
            raise HTTPException(status_code=403, detail="No tienes permisos para cambiar esta contraseña")
        
        print(f"▶️ Cambiando contraseña del usuario ID: {usuario_id}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor()
        
        # Verificar que el usuario existe
        cursor.execute("SELECT UsID FROM usuarios WHERE UsID = %s", (usuario_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        
        # Encriptar la nueva contraseña
        hashed_password = bcrypt.hashpw(nueva_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        # Actualizar la contraseña y resetear intentos
        cursor.execute(
            "UPDATE usuarios SET UsKeyWeb = %s, UsIntentos = 0 WHERE UsID = %s",
            (hashed_password, usuario_id)
        )
        conn.commit()
        
        print("✅ Contraseña actualizada correctamente")
        return {"message": "Contraseña actualizada correctamente"}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ ERROR cambiando contraseña: {e}")
        raise HTTPException(status_code=500, detail=f"Error al cambiar contraseña: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== ENDPOINTS PARA REPORTES ==================
@app.post("/api/reportes/ingresos-gastos")
async def obtener_ingresos_gastos(request: ReporteIngresosGastosRequest, test_mode: bool = False):
    print("=== INICIO REPORTE INGRESOS-GASTOS ===")
    print(f"Datos recibidos: {request}")
    
    try:
        # Obtener conexión a la BD
        print("Intentando conectar a la BD...")
        connection = conectar_db(test_mode=test_mode)
        print("Conexión exitosa")
        
        # Para pymysql, usar DictCursor
        print("Creando cursor...")
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        print("Cursor creado exitosamente")
        
        # Construir consulta
        print("Construyendo consulta SQL...")
        sql_query = """
            SELECT 
                m.MoID,
                m.MoFecha, 
                t.GaNombre,
                COALESCE(r.RuNombre, o.OpNombre) AS Rubro,
                m.MoDesc AS Concepto, 
                m.MoCChica AS Caja, 
                m.MoSaldoCaja AS Saldo_Caja,
                m.MoImporte AS Banco, 
                m.MoSaldoBanco AS Saldo_Banco,
                m.MoSede, 
                m.MoTiMo, 
                m.MoTGas, 
                m.MoRubr, 
                l.LoNombre AS Sede
            FROM movimientos m
            LEFT JOIN locales l ON m.MoSede = l.LoCod
            LEFT JOIN tipinggas t ON m.MoTiMo = t.GaCod
            LEFT JOIN opcionbtns o ON m.MoTiMo = o.OpTipoOp AND m.MoTGas = o.OpCod
            LEFT JOIN rubingas r ON m.MoRubr = r.RuCod
            WHERE m.MoSede = %s 
              AND m.MoFecha >= %s 
              AND m.MoFecha <= %s
        """
        
        params = [request.codigoSede, request.fechaInicial, request.fechaFinal]
        # Filtro de domingos SI está marcado
        if request.soloDomingos:
            sql_query += " AND DAYOFWEEK(m.MoFecha) = 1"
            print("Filtro domingos aplicado")
        
        sql_query += " ORDER BY m.MoFecha, m.MoID"

        print(f"Parámetros: {params}")
        print(f"SQL: {sql_query}")
        
        # Ejecutar consulta
        print("Ejecutando consulta...")
        cursor.execute(sql_query, params)
        print("Consulta ejecutada, obteniendo resultados...")
        
        movimientos = cursor.fetchall()
        print(f"Movimientos obtenidos: {len(movimientos)}")
        print(f"Primeros 2 movimientos: {movimientos[:2] if movimientos else 'Sin datos'}")
        
        # Calcular resumen
        print("Calculando resumen...")
        resumen = calcular_resumen_movimientos(movimientos)
        print(f"Resumen calculado: {resumen}")
        
        cursor.close()
        connection.close()
        print("Conexión cerrada exitosamente")
        
        resultado = {
            "success": True,
            "movimientos": movimientos,
            "resumen": resumen,
            "parametros": {
                "sede": request.codigoSede,
                "fechaInicial": request.fechaInicial,
                "fechaFinal": request.fechaFinal,
                "soloDomingos": request.soloDomingos
            }
        }
        
        print("=== REPORTE EXITOSO ===")
        return resultado
        
    except Exception as e:
        print(f"=== ERROR EN REPORTE ===")
        print(f"Tipo de error: {type(e)}")
        print(f"Mensaje de error: {str(e)}")
        print(f"Error completo: {repr(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

# ================== ENDPOINT PARA DIEZMOS Y OFRENDAS ==================
@app.post("/api/reportes/diezmos-ofrendas")
async def obtener_diezmos_ofrendas(request: ReporteDiezmosOfrendasRequest, test_mode: bool = False):
    print("=== INICIO REPORTE DIEZMOS Y OFRENDAS ===")
    print(f"Datos recibidos: {request}")
    
    try:
        connection = conectar_db(test_mode=test_mode)
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        
        # Consulta SQL según especificaciones del PDF
        if request.soloDomingos:
            # SOBRE LOS INGRESOS EN EL CULTO (DOMINGOS)
            sql_query = """
                SELECT 
                    m.MoFecha,
                    'DIEZMOS+OFRENDAS' AS OpNombre,
                    'Culto Dominical' AS MoDesc,
                    SUM(m.MoCChica) AS Caja,
                    SUM(m.MoImporte) AS Banco,
                    SUM(m.MoCChica + m.MoImporte) AS Importe
                FROM movimientos m
                INNER JOIN opcionbtns o ON o.OpTipoOp = m.MoTiMo AND o.OpCod = m.MoTGas
                WHERE m.MoSede = %s 
                AND m.MoTiMo = 100 
                AND m.MoTGas IN (2,3,4,9) 
                AND m.MoFecha >= %s 
                AND m.MoFecha <= %s
                AND DAYOFWEEK(m.MoFecha) = 1
                GROUP BY m.MoFecha
                ORDER BY m.MoFecha, o.OpCod
            """
        else:
            # SOBRE LOS INGRESOS: DIEZMOS, OFRENDAS Y OTROS ENTRE FECHAS
            sql_query = """
                SELECT 
                    m.MoFecha,
                    o.OpNombre,
                    m.MoDesc,
                    m.MoCChica AS Caja,
                    m.MoImporte AS Banco,
                    (m.MoImporte + m.MoCChica) AS Importe
                FROM movimientos m
                INNER JOIN opcionbtns o ON o.OpTipoOp = m.MoTiMo AND o.OpCod = m.MoTGas
                WHERE m.MoSede = %s 
                AND m.MoTiMo = 100 
                AND m.MoTGas IN (2,3,4,9) 
                AND m.MoFecha >= %s 
                AND m.MoFecha <= %s
                ORDER BY m.MoFecha, o.OpCod
            """
        
        params = [request.codigoSede, request.fechaInicial, request.fechaFinal]
        
        print(f"SQL: {sql_query}")
        print(f"Parámetros: {params}")
        
        cursor.execute(sql_query, params)
        movimientos = cursor.fetchall()
        
        print(f"Movimientos encontrados: {len(movimientos)}")
        
        # Calcular resumen
        resumen = {
            'totalImporte': sum(float(mov.get('Importe', 0) or 0) for mov in movimientos),
            'cantidadMovimientos': len(movimientos),
            'porDomingos': request.soloDomingos
        }
        
        cursor.close()
        connection.close()
        
        return {
            "success": True,
            "movimientos": movimientos,
            "resumen": resumen,
            "parametros": {
                "sede": request.codigoSede,
                "fechaInicial": request.fechaInicial,
                "fechaFinal": request.fechaFinal,
                "soloDomingos": request.soloDomingos
            }
        }
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

# ================== ENDPOINT PARA CARGA SEGUNDO NIVEL ==================
@app.get("/segundo-nivel", response_model=SegundoNivelResult)
def obtener_segundo_nivel(tipo: int, sede: int, auth=Depends(get_current_user), test_mode: bool = False):
    """Obtener opciones de segundo nivel según el tipo de operación y sede"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Obteniendo segundo nivel: tipo={tipo}, sede={sede}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        query = """
        SELECT MnuCod, MnuNombre, MnuSigAccion 
        FROM mnusedesbtn 
        WHERE MnuTipoOp = %s AND mnuSede = %s 
        ORDER BY mnuPeso
        """
        
        cursor.execute(query, (tipo, sede))
        opciones = cursor.fetchall()
        
        print(f"✅ Encontradas {len(opciones)} opciones")
        return SegundoNivelResult(
            success=True,
            results=opciones
        )
        
    except Exception as e:
        print(f"❌ ERROR segundo nivel: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== ENDPOINT PARA CARGA TERCEL NIVEL ==================
@app.get("/tercer-nivel")
def obtener_tercer_nivel(accion: str, sede: int, auth=Depends(get_current_user), test_mode: bool = False):
    """Obtener opciones de tercer nivel según MnuSigAccion"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Obteniendo tercer nivel: accion={accion}, sede={sede}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # Determinar la consulta según MnuSigAccion
        if accion == "donantes":
            query = "SELECT DoCod as codigo, DoNombre as nombre FROM donantes WHERE DoTipo=1 ORDER BY DoNombre"
            params = ()
        elif accion == "fieles":
            query = "SELECT fiCod as codigo, CONCAT(fiNombres, ' ', fiApellidos) as nombre FROM fieles WHERE fiSede = %s ORDER BY fiApellidos, fiNombres"
            params = (sede,)
        elif accion == "aporta":
            query = "SELECT RuCod as codigo, RuNombre as nombre FROM rubingassede WHERE RuSede = %s AND RuTipGasto=5 ORDER BY RuOrden"
            params = (sede,)
        elif accion == "cajachica":
            query = "SELECT RuCod as codigo, RuNombre as nombre FROM rubingassede WHERE RuSede = %s AND RuTipGasto = 6 ORDER BY RuOrden"
            params = (sede,)
        elif accion == "cajero":
            query = "SELECT RuCod as codigo, RuNombre as nombre FROM rubingassede WHERE RuSede = %s AND RuTipGasto = 6 ORDER BY RuOrden"
            params = (sede,)    
        elif accion == "eventos":
            query = "SELECT RuCod as codigo, RuNombre as nombre FROM rubingassede WHERE RuSede = %s AND RuTipGasto = 20 ORDER BY RuOrden"
            params = (sede,)
        elif accion == "externas":
            query = "SELECT RuCod as codigo, RuNombre as nombre FROM rubingassede WHERE RuSede = %s AND RuTipGasto = 4 ORDER BY RuOrden"
            params = (sede,)
        elif accion == "gastos":
            query = "SELECT RuCod as codigo, RuNombre as nombre FROM rubingassede WHERE RuSede = %s AND RuTipGasto = 2 ORDER BY RuOrden"
            params = (sede,)
        elif accion == "ventas":
            query = "SELECT RuCod as codigo, RuNombre as nombre FROM rubingassede WHERE RuSede = %s AND RuTipGasto = 2 AND RuCod=17 ORDER BY RuOrden"
            params = (sede,)    
        elif accion == "mensual":
            query = "SELECT RuCod as codigo, RuNombre as nombre FROM rubingassede WHERE RuSede = 11 AND RuTipGasto = 8 ORDER BY RuOrden"
            params = ()
        elif accion == "servicios":
            query = "SELECT RuCod as codigo, RuNombre as nombre FROM rubingassede WHERE RuSede = %s AND RuTipGasto = 7 ORDER BY RuOrden"
            params = (sede,)
        elif accion == "traspaso":
            query = "SELECT RuCod as codigo, RuNombre as nombre FROM rubingassede WHERE RuSede = 11 AND RuTipGasto = 3 ORDER BY RuOrden"
            params = ()
        elif accion == "sedes":
            query = "SELECT LoCod as codigo, LoNombre as nombre FROM locales WHERE LoCod <> %s AND LoSituacion = 1 ORDER BY LoNombre"
            params = (sede,)
        elif accion == "pastores":
            query = "SELECT PeCos as Codigo, peNombre as Nombre FROM personal WHERE PeSede = %s AND PeClase <= 4 ORDER BY PeClase,PeCod;"
            params = (sede,)
        else:
            # Vacío o NULL - no hay opciones
            return {"success": True, "results": []}
        
        cursor.execute(query, params)
        opciones = cursor.fetchall()
        
        print(f"✅ Encontradas {len(opciones)} opciones de tercer nivel")
        return {"success": True, "results": opciones}
        
    except Exception as e:
        print(f"❌ ERROR tercer nivel: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== ENDPOINT PARA CARGA DE MOVIMIENTOS ==================
@app.get("/movimientos")
def obtener_movimientos(año: int, mes: int, sede: int, auth=Depends(get_current_user), test_mode: bool = False):
    """Obtener movimientos de un período específico"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Obteniendo movimientos: año={año}, mes={mes}, sede={sede}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # SQL para obtener movimientos del mes/año/sede
        query = """
        SELECT MoID, MoFecha, MoDesc, MoCChica, MoSaldoCaja, MoImporte, MoSaldoBanco
        FROM movimientos 
        WHERE MoSede = %s 
        AND YEAR(MoFecha) = %s 
        AND MONTH(MoFecha) = %s
        ORDER BY MoFecha, MoID
        """
        
        cursor.execute(query, (sede, año, mes))
        movimientos = cursor.fetchall()
        
        print(f"✅ Encontrados {len(movimientos)} movimientos")
        return {
            "success": True,
            "movimientos": movimientos
        }
        
    except Exception as e:
        print(f"❌ ERROR obteniendo movimientos: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()            

# ================== ENDPOINT PARA GRABAR DE MOVIMIENTOS ==================
@app.post("/grabar-movimiento")
def grabar_movimiento(movimiento: MovimientoCreate, auth=Depends(get_current_user), test_mode: bool = False):
    movimiento = convertir_campos_texto_mayusculas(movimiento)
    # 🔍 PRINTS PARA DEBUG
    print("=" * 50)
    print("🔍 DEBUG - DATOS QUE LLEGAN DEL FRONTEND:")
    print(f"   Tipo Operación: {movimiento.tipoOperacion}")
    print(f"   Importe Original: {movimiento.importe}")
    print(f"   Origen: {movimiento.origen}")
    print(f"   Saldo Caja enviado: {movimiento.saldoCaja}")
    print(f"   Saldo Banco enviado: {movimiento.saldoBanco}")
    print("=" * 50)
    
    conn = None
    cursor = None
    try:
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor()
        
        # Crear fecha completa
        fecha = f"{movimiento.año}-{movimiento.mes:02d}-{movimiento.dia:02d}"

        # El frontend ya envía los valores con signo correcto, usar directamente
        if movimiento.origen == "caja":
            caja = movimiento.importe  # Ya viene con signo correcto desde frontend
            banco = 0
        else:  # banco
            caja = 0
            banco = movimiento.importe  # Ya viene con signo correcto desde frontend

        # Determinar valores según origen
        caja = movimiento.importe if movimiento.origen == "caja" else 0
        banco = movimiento.importe if movimiento.origen == "banco" else 0

        # SQL para insertar movimiento
        query = """
        INSERT INTO movimientos (
            MoSede, MoTiMo, MoTGas, MoRubr, MoFecha, MoDesc, 
            MoImporte, MoCChica, MoSaldoCaja, MoSaldoBanco, 
            MoDona, MoPers, MoSedeDes, MoUser, MoHecho
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """
        
        valores = (
            movimiento.sede,
            movimiento.tipoOperacion,
            movimiento.segundoNivel or 0,
            movimiento.tercerNivel or 0,
            fecha,
            movimiento.descripcion,
            banco,
            caja,
            movimiento.saldoCaja or 0,
            movimiento.saldoBanco or 0,
            movimiento.moDona or 0,        # ✅ NUEVO
            movimiento.moPers or 0,        # ✅ NUEVO  
            movimiento.moSedeDes or 0,    # ✅ NUEVO
            auth.get("sub", "XXX")
        )

        print(f"🔍 DEBUG - Valores para BD: caja={caja}, banco={banco}")

        cursor.execute(query, valores)
        conn.commit()
        
        print("✅ Movimiento grabado correctamente")
        return {"success": True, "message": "Movimiento grabado correctamente"}
        
    except Exception as e:
        print(f"❌ ERROR grabando movimiento: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()            

# ================== OBTENER CIERRES EXISTENTES ==================
@app.get("/cierres/{anyo}")
def obtener_cierres(anyo: int, sede: int = Query(...), auth=Depends(get_current_user), test_mode: bool = False):
    """Obtener cierres mensuales de un año específico"""
    conn = None
    cursor = None
    try:
        # print(f"▶️ Obteniendo cierres: año={anyo}, sede={sede}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # Buscar registros especiales de cierre en movimientos
        query = """
        SELECT MoID as id, MoCtas as mes_nombre, YEAR(MoFecha) as anyo,
               MoSaldoCaja as saldoCaja, MoSaldoBanco as saldoBanco, MoHecho as fechaCierre
        FROM movimientos 
        WHERE MoSede = %s 
        AND YEAR(MoFecha) = %s
        AND MoDona = 9999
        ORDER BY MoFecha
        """
        cursor.execute(query, (sede, anyo))
        cierres = cursor.fetchall()
        
        print(f"✅ Encontrados {len(cierres)} cierres")
        return {
            "success": True,
            "cierres": cierres
        }
        
    except Exception as e:
        print(f"❌ ERROR obteniendo cierres: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()            

# ================== CREAR CIERRE TEMPORAL ==================
@app.post("/crear-cierre-temporal")
def crear_cierre_temporal(cierre: CierreCreateTemporal, auth=Depends(get_current_user), test_mode: bool = False):
    """Crear cierre temporal - busca último registro del mes y crea cierre para revisión"""
    print("🚀 ENDPOINT crear-cierre-temporal INICIADO")
    print(f"📦 Datos recibidos: {cierre}")
    conn = None
    cursor = None
    meses_nombres = {
        1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
        5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto", 
        9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre" }
    try:
        print(f"▶️ Creando cierre temporal: {cierre}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # En la validación, buscar en el mes SIGUIENTE:
        mes_a_verificar = cierre.mes + 1 if cierre.mes < 12 else 1
        anyo_a_verificar = cierre.anyo if cierre.mes < 12 else cierre.anyo + 1

        # Verificar que no existe ya un cierre para ese período
        query_verificar = """
        SELECT MoID FROM movimientos 
        WHERE MoSede = %s 
        AND YEAR(MoFecha) = %s 
        AND MONTH(MoFecha) = %s
        AND MoDona = 9999
        """
        cursor.execute(query_verificar, (cierre.sede, anyo_a_verificar, mes_a_verificar))
        existe = cursor.fetchone()
        
        if existe:
            raise HTTPException(status_code=400, detail=f"Ya existe un cierre para {cierre.mes}/{cierre.anyo}")
        
        # Buscar último movimiento del mes para obtener saldos
        import calendar
        ultimo_dia = calendar.monthrange(cierre.anyo, cierre.mes)[1]
        fecha_fin = f"{cierre.anyo}-{cierre.mes:02d}-{ultimo_dia}"
        
        query_saldos = """
        SELECT MoSaldoCaja, MoSaldoBanco
        FROM movimientos 
        WHERE MoSede = %s 
        AND MoFecha <= %s
        ORDER BY MoFecha DESC, MoID DESC
        LIMIT 1
        """
        
        cursor.execute(query_saldos, (cierre.sede, fecha_fin))
        resultado_saldos = cursor.fetchone()
        
        if not resultado_saldos:
            raise HTTPException(status_code=400, detail=f"No hay movimientos registrados hasta {cierre.mes}/{cierre.anyo}")
        
        saldo_caja = float(resultado_saldos['MoSaldoCaja'] or 0)
        saldo_banco = float(resultado_saldos['MoSaldoBanco'] or 0)
        
        # Crear fecha del primer día del mes siguiente
        mes_siguiente = cierre.mes + 1 if cierre.mes < 12 else 1
        año_siguiente = cierre.anyo if cierre.mes < 12 else cierre.anyo + 1
        fecha_cierre = f"{año_siguiente}-{mes_siguiente:02d}-01"
        
        # Descripción según especificación del PDF
        descripcion = f"SALDO INICIAL DEL MES {cierre.mes:02d}/{cierre.anyo}"
        mes_nombre = meses_nombres.get(cierre.mes, "Desconocido")
        texto_cierre = f"{mes_nombre} {cierre.anyo}"

        # Crear registro temporal en movimientos
        query_crear = """
        INSERT INTO movimientos (
            MoDona, MoProy, MoSede, MoSedeDes, MoPers, MoCtas, MoTiMo, MoTGas, MoRubr,
            MoFecha, MoDesc, MoImporte, MoCChica, MoUser, MoHecho, MoSaldoCaja, MoSaldoBanco
        ) VALUES (9999, 0, %s, 0, 0, %s, 0, 0, 0, %s, %s, 0, 0, %s, NOW(), %s, %s)
        """
        
        valores = (
           cierre.sede, 
           texto_cierre, 
           fecha_cierre, 
           descripcion,
           cierre.usuario, 
           saldo_caja, 
           saldo_banco
        )
        
        cursor.execute(query_crear, valores)
        cierre_id = cursor.lastrowid
        conn.commit()
        
        print(f"✅ Cierre temporal creado con ID: {cierre_id}")
        
        # Devolver datos del cierre para revisión
        return {
            "success": True,
            "message": "Cierre temporal creado para revisión",
            "cierre": {
                "id": cierre_id,
                "saldoCaja": saldo_caja,
                "saldoBanco": saldo_banco,
                "descripcion": descripcion,
                "fecha": fecha_cierre
            }
        }
        
    except Exception as e:
        print(f"❌ ERROR creando cierre temporal: {e}")
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== PARA CONFIRMAR UN CIERRE ==================
@app.post("/confirmar-cierre/{cierre_id}")
def confirmar_cierre(cierre_id: int, auth=Depends(get_current_user), test_mode: bool = False):
    """Confirmar cierre temporal - no hace nada adicional, el registro ya está creado"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Confirmando cierre ID: {cierre_id}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # Verificar que el cierre existe y es un registro de cierre
        query_verificar = """
        SELECT MoID, MoDesc, MoSede, YEAR(MoFecha) as año, MONTH(MoFecha) as mes
        FROM movimientos 
        WHERE MoID = %s 
        AND MoDona = 9999
        """
        
        cursor.execute(query_verificar, (cierre_id,))
        cierre = cursor.fetchone()
        
        if not cierre:
            raise HTTPException(status_code=404, detail="Cierre no encontrado")
        
        print(f"✅ Cierre confirmado: {cierre['MoDesc']}")
        
        return {
            "success": True,
            "message": f"Cierre confirmado exitosamente"
        }
        
    except Exception as e:
        print(f"❌ ERROR confirmando cierre: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()            

# ================== PARA ELIMINAR UN CIERRE ==================
@app.delete("/eliminar-cierre/{cierre_id}")
def eliminar_cierre(cierre_id: int, auth=Depends(get_current_user), test_mode: bool = False):
   """Eliminar cierre mensual (solo si es el último)"""
   conn = None
   cursor = None
   try:
       print(f"▶️ Eliminando cierre ID: {cierre_id}")
       conn = conectar_db(test_mode=test_mode)
       cursor = conn.cursor(pymysql.cursors.DictCursor)
       
       # 1. Obtener IDFinal (último cierre)
       query_ultimo = """
       SELECT MoID AS IDFinal FROM movimientos 
       WHERE MoDona = 9999 ORDER BY MoFecha DESC LIMIT 1
       """
       
       cursor.execute(query_ultimo)
       ultimo = cursor.fetchone()
       
       # 2. Verificar si es el último
       if not ultimo or ultimo['IDFinal'] != cierre_id:
           raise HTTPException(status_code=400, detail="Solo se puede eliminar el último cierre mensual")
       
       # 3. Eliminar el cierre
       query_eliminar = "DELETE FROM movimientos WHERE MoID = %s"
       cursor.execute(query_eliminar, (cierre_id,))
       conn.commit()
       
       print("✅ Cierre eliminado correctamente")
       return {"success": True, "message": "Cierre eliminado correctamente"}
       
   except Exception as e:
       print(f"❌ ERROR eliminando cierre: {e}")
       if conn:
           conn.rollback()
       raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
   finally:
       if cursor:
           cursor.close()
       if conn:
           conn.close()

# ================== PARA RE-CALCULAR LOS SALDOS ==================
@app.post("/recalcular-saldos")
def recalcular_saldos(sede: int = Query(...), auth=Depends(get_current_user), test_mode: bool = False):
    """Recalcular saldos desde el último cierre mensual"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Iniciando recálculo de saldos para sede: {sede}")
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # 1. Buscar último cierre mensual (saldo base)
        print("🔍 Buscando último cierre mensual...")
        query_ultimo_cierre = """
        SELECT MoSaldoCaja, MoSaldoBanco, MoFecha 
        FROM movimientos 
        WHERE MoSede = %s AND MoDona = 9999 
        ORDER BY MoFecha DESC LIMIT 1
        """
        
        cursor.execute(query_ultimo_cierre, (sede,))
        ultimo_cierre = cursor.fetchone()
        
        if not ultimo_cierre:
            # Si no hay cierres, empezar desde el primer movimiento con saldos en 0
            print("⚠️ No hay cierres previos, recalculando desde el inicio")
            saldo_caja_inicial = 0
            saldo_banco_inicial = 0
            fecha_desde = '1900-01-01'  # Desde el inicio
        else:
            print(f"✅ Último cierre encontrado: {ultimo_cierre['MoFecha']}")
            saldo_caja_inicial = float(ultimo_cierre['MoSaldoCaja'] or 0)
            saldo_banco_inicial = float(ultimo_cierre['MoSaldoBanco'] or 0)
            fecha_desde = ultimo_cierre['MoFecha']
        
        # 2. Obtener movimientos desde esa fecha (excluyendo cierres)
        print(f"📊 Obteniendo movimientos desde: {fecha_desde}")
        query_movimientos = """
        SELECT MoID, MoCChica, MoImporte
        FROM movimientos 
        WHERE MoSede = %s 
        AND MoFecha >= %s 
        AND MoDona != 9999
        ORDER BY MoFecha, MoID
        """
        
        cursor.execute(query_movimientos, (sede, fecha_desde))
        movimientos = cursor.fetchall()
        
        print(f"📝 Encontrados {len(movimientos)} movimientos para recalcular")
        
        if len(movimientos) == 0:
            return {
                "success": True,
                "message": "No hay movimientos para recalcular",
                "movimientos_actualizados": 0
            }
        
        # 3. Recalcular con variables acumulativas
        saldo_caja_acum = saldo_caja_inicial
        saldo_banco_acum = saldo_banco_inicial
        movimientos_actualizados = 0
        
        print("🔄 Iniciando recálculo...")
        
        for movimiento in movimientos:
            # Acumular saldos
            saldo_caja_acum += float(movimiento['MoCChica'] or 0)
            saldo_banco_acum += float(movimiento['MoImporte'] or 0)
            
            # Actualizar registro
            query_update = """
            UPDATE movimientos 
            SET MoSaldoCaja = %s, MoSaldoBanco = %s 
            WHERE MoID = %s
            """
            
            cursor.execute(query_update, (saldo_caja_acum, saldo_banco_acum, movimiento['MoID']))
            movimientos_actualizados += 1
            
            if movimientos_actualizados % 100 == 0:  # Log cada 100 registros
                print(f"📈 Procesados {movimientos_actualizados} movimientos...")
        
        # Confirmar cambios
        conn.commit()
        
        print(f"✅ Recálculo completado: {movimientos_actualizados} movimientos actualizados")
        
        return {
            "success": True,
            "message": f"Recálculo completado exitosamente",
            "movimientos_actualizados": movimientos_actualizados,
            "saldo_final_caja": saldo_caja_acum,
            "saldo_final_banco": saldo_banco_acum
        }
        
    except Exception as e:
        print(f"❌ ERROR en recálculo: {e}")
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== PARA VERIFICAR SI UN PERIODO YA ESTA CERRADO Y EVITAR MODIFICACIONES ==================
@app.get("/verificar-periodo-cerrado")
def verificar_periodo_cerrado(sede: int = Query(...), año: int = Query(...), mes: int = Query(...), auth=Depends(get_current_user), test_mode: bool = False):
    """Verificar si un período está cerrado"""
    conn = None
    cursor = None
    try:
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # Buscar cierre del mes SIGUIENTE
        # Si queremos saber si Julio está cerrado, buscamos registro en Agosto
        mes_siguiente = mes + 1 if mes < 12 else 1
        año_siguiente = año if mes < 12 else año + 1

        # Buscar si existe un cierre para ese período
        query = """
        SELECT COUNT(*) as cantidad
        FROM movimientos 
        WHERE MoSede = %s 
        AND YEAR(MoFecha) = %s 
        AND MONTH(MoFecha) = %s
        AND DAY(MoFecha) = 1
        AND MoDona = 9999
        """
        
        cursor.execute(query, (sede, año_siguiente, mes_siguiente))
        resultado = cursor.fetchone()
        
        periodo_cerrado = resultado['cantidad'] > 0
        
        return {
            "periodo_cerrado": periodo_cerrado,
            "año": año,
            "mes": mes
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== PARA MODIFICAR UN REGISTRO DE MOVIMIENTOS ==================
@app.put("/editar-movimiento/{movimiento_id}")
def editar_movimiento(movimiento_id: int, movimiento: MovimientoCreate, auth=Depends(get_current_user), test_mode: bool = False):
    movimiento = convertir_campos_texto_mayusculas(movimiento)
    conn = None
    cursor = None
    try:
        print(f"▶️ Editando movimiento ID: {movimiento_id}")
        print(f"📦 Datos nuevos: {movimiento}")

        # Después de la línea: print(f"📦 Datos nuevos: {movimiento}")
        print("🔍 DEBUG EDICIÓN:")
        print(f"   Tiene cChica: {hasattr(movimiento, 'cChica')}")
        print(f"   Importe: {movimiento.importe}")
        print(f"   Origen: {movimiento.origen}")
        print(f"   Sede: {movimiento.sede}")
        print(f"   saldoCaja: {getattr(movimiento, 'saldoCaja', 'NO EXISTE')}")
        print(f"   saldoBanco: {getattr(movimiento, 'saldoBanco', 'NO EXISTE')}")

        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # 1. Verificar que el movimiento existe y obtener datos originales
        query_original = "SELECT * FROM movimientos WHERE MoID = %s"
        cursor.execute(query_original, (movimiento_id,))
        movimiento_original = cursor.fetchone()
        
        if not movimiento_original:
            raise HTTPException(status_code=404, detail="Movimiento no encontrado")
        
        # 2. Verificar que no es un registro de cierre
        if movimiento_original['MoDona'] == 9999:
            raise HTTPException(status_code=400, detail="No se pueden editar registros de cierre")
        
        # 3. Verificar período cerrado# - VERIFICAR EL PERÍODO ORIGINAL:
        # Obtener fecha del movimiento original
        fecha_original = movimiento_original['MoFecha']
        año_original = fecha_original.year
        mes_original = fecha_original.month

        # Buscar cierre del mes SIGUIENTE
        # Si queremos saber si Julio está cerrado, buscamos registro en Agosto
        mes_siguiente = mes_original + 1 if mes_original < 12 else 1
        año_siguiente = año_original if mes_original < 12 else año_original + 1

        # Verificar período cerrado usando la fecha ORIGINAL
        query_periodo_cerrado = """
        SELECT COUNT(*) as cantidad
        FROM movimientos 
        WHERE MoSede = %s 
        AND YEAR(MoFecha) = %s 
        AND MONTH(MoFecha) = %s
        AND MoDona = 9999
        """

        cursor.execute(query_periodo_cerrado, (movimiento.sede, año_siguiente, mes_siguiente))
        resultado_periodo = cursor.fetchone()

        print(f"🔍 Resultado consulta cierre: {resultado_periodo}")
        print(f"🔍 Cantidad encontrada: {resultado_periodo['cantidad']}")

        if resultado_periodo['cantidad'] > 0:
            raise HTTPException(status_code=400, detail=f"No se puede editar: el período original {mes_original}/{año_original} está cerrado")
        
        # 4. Determinar valores según origen
        nuevo_caja = movimiento.cChica if hasattr(movimiento, 'cChica') else (movimiento.importe if movimiento.origen == "caja" else 0)
        nuevo_banco = movimiento.importe if movimiento.origen == "banco" else 0
        
        # 5. Crear nueva fecha
        nueva_fecha = f"{movimiento.año}-{movimiento.mes:02d}-{movimiento.dia:02d}"
        
        # 6. Actualizar el movimiento
        query_update = """
        UPDATE movimientos 
        SET MoFecha = %s, MoDesc = %s, MoImporte = %s, MoCChica = %s
        WHERE MoID = %s
        """
        
        cursor.execute(query_update, (
            nueva_fecha,
            movimiento.descripcion,
            nuevo_banco,
            nuevo_caja,
            movimiento_id
        ))
        
        conn.commit()
        
        # 7. Recalcular saldos desde el último cierre
        print("🔄 Iniciando recálculo de saldos después de edición...")
        
        # Buscar último cierre
        query_ultimo_cierre = """
        SELECT MoSaldoCaja, MoSaldoBanco, MoFecha 
        FROM movimientos 
        WHERE MoSede = %s AND MoDona = 9999 
        ORDER BY MoFecha DESC LIMIT 1
        """
        
        cursor.execute(query_ultimo_cierre, (movimiento.sede,))
        ultimo_cierre = cursor.fetchone()
        
        if ultimo_cierre:
            saldo_caja_inicial = float(ultimo_cierre['MoSaldoCaja'] or 0)
            saldo_banco_inicial = float(ultimo_cierre['MoSaldoBanco'] or 0)
            fecha_desde = ultimo_cierre['MoFecha']
        else:
            saldo_caja_inicial = 0
            saldo_banco_inicial = 0
            fecha_desde = '1900-01-01'
        
        # Obtener movimientos a recalcular
        query_movimientos = """
        SELECT MoID, MoCChica, MoImporte
        FROM movimientos 
        WHERE MoSede = %s 
        AND MoFecha >= %s 
        AND MoDona != 9999
        ORDER BY MoFecha, MoID
        """
        
        cursor.execute(query_movimientos, (movimiento.sede, fecha_desde))
        movimientos_recalcular = cursor.fetchall()
        
        # Recalcular saldos
        saldo_caja_acum = saldo_caja_inicial
        saldo_banco_acum = saldo_banco_inicial
        
        for mov in movimientos_recalcular:
            saldo_caja_acum += float(mov['MoCChica'] or 0)
            saldo_banco_acum += float(mov['MoImporte'] or 0)
            
            query_update_saldos = """
            UPDATE movimientos 
            SET MoSaldoCaja = %s, MoSaldoBanco = %s 
            WHERE MoID = %s
            """
            
            cursor.execute(query_update_saldos, (saldo_caja_acum, saldo_banco_acum, mov['MoID']))
        
        conn.commit()
        
        print(f"✅ Movimiento editado y saldos recalculados: {len(movimientos_recalcular)} registros")
        
        return {
            "success": True,
            "message": "Movimiento actualizado y saldos recalculados correctamente",
            "movimientos_recalculados": len(movimientos_recalcular)
        }
        
    except Exception as e:
        print(f"❌ ERROR editando movimiento: {e}")
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== PARA ELIMINAR UN REGISTRO DE MOVIMIENTOS ==================
@app.delete("/eliminar-movimiento/{movimiento_id}")
def eliminar_movimiento(movimiento_id: int, auth=Depends(get_current_user), test_mode: bool = False):
    """Eliminar un movimiento y recalcular saldos"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Eliminando movimiento ID: {movimiento_id}")
        
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # 1. Verificar que el movimiento existe y obtener datos
        query_movimiento = "SELECT * FROM movimientos WHERE MoID = %s"
        cursor.execute(query_movimiento, (movimiento_id,))
        movimiento = cursor.fetchone()
        
        if not movimiento:
            raise HTTPException(status_code=404, detail="Movimiento no encontrado")
        
        # 2. Verificar que no es un registro de cierre
        if movimiento['MoDona'] == 9999:
            raise HTTPException(status_code=400, detail="No se pueden eliminar registros de cierre")
        
        # 3. Verificar período cerrado
        # Buscar cierre del mes SIGUIENTE
        # Si queremos saber si Julio está cerrado, buscamos registro en Agosto
        fecha_mov = movimiento['MoFecha']
        mes_mov = fecha_mov.month + 1 if fecha_mov.month < 12 else 1
        año_mov = fecha_mov.year if fecha_mov.month < 12 else fecha_mov.year + 1
        sede_mov = movimiento['MoSede']
        
        query_periodo_cerrado = """
        SELECT COUNT(*) as cantidad
        FROM movimientos 
        WHERE MoSede = %s 
        AND YEAR(MoFecha) = %s 
        AND MONTH(MoFecha) = %s
        AND MoDona = 9999
        """
        
        cursor.execute(query_periodo_cerrado, (sede_mov, año_mov, mes_mov))
        resultado_periodo = cursor.fetchone()
        
        if resultado_periodo['cantidad'] > 0:
            raise HTTPException(status_code=400, detail=f"No se puede eliminar: el período {mes_mov}/{año_mov} está cerrado")
        
        # 4. Eliminar el movimiento
        query_delete = "DELETE FROM movimientos WHERE MoID = %s"
        cursor.execute(query_delete, (movimiento_id,))
        
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Movimiento no encontrado para eliminar")
        
        conn.commit()
        print(f"✅ Movimiento {movimiento_id} eliminado")
        
        # 5. Recalcular saldos desde el último cierre
        print("🔄 Iniciando recálculo de saldos después de eliminación...")
        
        # Buscar último cierre
        query_ultimo_cierre = """
        SELECT MoSaldoCaja, MoSaldoBanco, MoFecha 
        FROM movimientos 
        WHERE MoSede = %s AND MoDona = 9999 
        ORDER BY MoFecha DESC LIMIT 1
        """
        
        cursor.execute(query_ultimo_cierre, (sede_mov,))
        ultimo_cierre = cursor.fetchone()
        
        if ultimo_cierre:
            saldo_caja_inicial = float(ultimo_cierre['MoSaldoCaja'] or 0)
            saldo_banco_inicial = float(ultimo_cierre['MoSaldoBanco'] or 0)
            fecha_desde = ultimo_cierre['MoFecha']
        else:
            saldo_caja_inicial = 0
            saldo_banco_inicial = 0
            fecha_desde = '1900-01-01'
        
        # Obtener movimientos a recalcular
        query_movimientos = """
        SELECT MoID, MoCChica, MoImporte
        FROM movimientos 
        WHERE MoSede = %s 
        AND MoFecha >= %s 
        AND MoDona != 9999
        ORDER BY MoFecha, MoID
        """
        
        cursor.execute(query_movimientos, (sede_mov, fecha_desde))
        movimientos_recalcular = cursor.fetchall()
        
        # Recalcular saldos
        saldo_caja_acum = saldo_caja_inicial
        saldo_banco_acum = saldo_banco_inicial
        
        for mov in movimientos_recalcular:
            saldo_caja_acum += float(mov['MoCChica'] or 0)
            saldo_banco_acum += float(mov['MoImporte'] or 0)
            
            query_update_saldos = """
            UPDATE movimientos 
            SET MoSaldoCaja = %s, MoSaldoBanco = %s 
            WHERE MoID = %s
            """
            
            cursor.execute(query_update_saldos, (saldo_caja_acum, saldo_banco_acum, mov['MoID']))
        
        conn.commit()
        
        print(f"✅ Movimiento eliminado y saldos recalculados: {len(movimientos_recalcular)} registros")
        
        return {
            "success": True,
            "message": "Movimiento eliminado y saldos recalculados correctamente",
            "movimientos_recalculados": len(movimientos_recalcular)
        }
        
    except Exception as e:
        print(f"❌ ERROR eliminando movimiento: {e}")
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== REPORTE DIEZMOS POR PERSONA ==================
@app.post("/api/reportes/diezmos-por-persona")
async def obtener_diezmos_por_persona(request: ReporteDiezmosPorPersonaRequest, test_mode: bool = False):
    try:
        connection = conectar_db(test_mode=test_mode)
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        
        sql_query = """
        SELECT fiCod AS Codigo, 
               CONCAT_WS(' ', fiNombres, fiApellidos) AS Nombres,
               SUM(MoImporte + MoCChica) AS Total
        FROM movimientos
        LEFT JOIN fieles ON MoSede = FiSede AND MoPers = FiCod
        WHERE MoSede = %s 
          AND MoTiMo = 100 
          AND MoTGas = 2 
          AND MoPers > 0 
          AND YEAR(MoFecha) = %s
        GROUP BY MoPers, YEAR(MoFecha)
        ORDER BY Nombres
        """
        
        cursor.execute(sql_query, (request.codigoSede, request.año))
        resultados = cursor.fetchall()
        
        # Calcular total general
        total_general = sum(float(row.get('Total', 0) or 0) for row in resultados)
        
        cursor.close()
        connection.close()
        
        return {
            "success": True,
            "diezmos": resultados,
            "total_general": total_general,
            "parametros": {
                "sede": request.codigoSede,
                "año": request.año
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

# ================== ENDPOINT PARA PROCESAR TRANSPOSICIÓN ==================
@app.post("/api/reportes/procesar-transposicion")
async def procesar_transposicion(request: TransposicionRequest, test_mode: bool = False):
    print("=== INICIO TRANSPOSICIÓN DATOS ECONÓMICOS ===")
    print(f"Datos recibidos: {request}")
    
    conn = None
    cursor = None
    try:
        connection = conectar_db(test_mode=test_mode)
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        
        # 1. Limpiar tabla ingresosygastos si se solicita
        if request.limpiarTabla:
            print("🧹 Limpiando tabla ingresosygastos...")
            cursor.execute("DELETE FROM ingresosygastos WHERE IGSede = %s", (request.codigoSede,))
            print("✅ Tabla limpiada")
        
        # 2. Obtener datos base (reutilizamos la consulta del primer endpoint)
        print("📊 Obteniendo datos base...")
        sql_datos = """
            SELECT MoSede, MoTiMo, MoTGas, MoRubr, OpNombre, MONTH(MoFecha) AS Mes,
                   IF(MoTiMo>0, SUM(MoImporte), MoSaldoBanco) AS Importe
            FROM movimientos
            LEFT JOIN opcionbtns ON OpTipoOp=MoTiMo AND OpCod=MoTGas
            WHERE MoSede = %s AND YEAR(MoFecha) = %s
            GROUP BY MoTiMo, MoTGas, YEAR(MoFecha), MONTH(MoFecha)
            ORDER BY MoTiMo, MoTGas, MONTH(MoFecha)
        """
        
        cursor.execute(sql_datos, (request.codigoSede, request.año))
        datos_verticales = cursor.fetchall()
        print(f"📝 Obtenidos {len(datos_verticales)} registros verticales")
        
        # 3. Procesar transposición (lógica similar al código Delphi del PDF)
        print("🔄 Procesando transposición...")
        
        # Diccionario para acumular datos por grupo (TiMo + TGas)
        grupos = {}
        
        for registro in datos_verticales:
            sede = registro['MoSede']
            tipo_mov = registro['MoTiMo']
            tipo_gas = registro['MoTGas']
            rubro = registro['MoRubr'] or 0
            nombre = registro['OpNombre'] or 'SIN NOMBRE'
            mes = registro['Mes']
            importe = float(registro['Importe'] or 0)
            
            # Crear clave única para el grupo
            clave_grupo = f"{tipo_mov}_{tipo_gas}"
            
            # Inicializar grupo si no existe
            if clave_grupo not in grupos:
                grupos[clave_grupo] = {
                    'IGSede': sede,
                    'IGTiMo': tipo_mov,
                    'IGMoTGas': tipo_gas,
                    'IGOpNom': nombre,
                    'meses': [0.0] * 12,  # Array de 12 meses
                    'IGTotAno': 0.0
                }
            
            # Acumular importe en el mes correspondiente (mes 1-12, array 0-11)
            if 1 <= mes <= 12:
                grupos[clave_grupo]['meses'][mes - 1] += importe
        
        # 4. Calcular totales anuales y preparar datos para inserción
        registros_insertar = []
        for grupo in grupos.values():
            # Calcular total anual (excepto para saldos anteriores)
            if grupo['IGTiMo'] == 0:  # Saldos anteriores
                grupo['IGTotAno'] = 0.0  # No sumar total anual
            else:
                grupo['IGTotAno'] = sum(grupo['meses'])  # Sumar normalmente
            
            # Preparar registro para inserción
            registro = (
                grupo['IGSede'],
                grupo['IGTiMo'], 
                grupo['IGMoTGas'],
                grupo['IGOpNom'],
                *grupo['meses'],  # Desempaquetar los 12 meses
                grupo['IGTotAno']
            )
            registros_insertar.append(registro)
        
        # 5. Insertar en tabla ingresosygastos
        print(f"💾 Insertando {len(registros_insertar)} registros en tabla...")
        
        sql_insert = """
        INSERT INTO ingresosygastos (
            IGSede, IGTiMo, IGMoTGas, IGOpNom,
            IGM1, IGM2, IGM3, IGM4, IGM5, IGM6, 
            IGM7, IGM8, IGM9, IGM10, IGM11, IGM12,
            IGTotAno
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.executemany(sql_insert, registros_insertar)
        connection.commit()
        
        print(f"✅ Transposición completada: {len(registros_insertar)} registros insertados")
        
        # 6. Obtener resultado final para verificación
        cursor.execute("""
            SELECT * FROM ingresosygastos 
            WHERE IGSede = %s 
            ORDER BY IGTiMo, IGMoTGas
        """, (request.codigoSede,))
        
        resultado_final = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        return {
            "success": True,
            "message": f"Transposición completada exitosamente",
            "registros_procesados": len(datos_verticales),
            "registros_insertados": len(registros_insertar),
            "datos_transpuestos": resultado_final[:5],  # Primeros 5 registros para verificar
            "total_registros_finales": len(resultado_final),
            "parametros": {
                "sede": request.codigoSede,
                "año": request.año,
                "tabla_limpiada": request.limpiarTabla
            }
        }
        
    except Exception as e:
        print(f"❌ ERROR en transposición: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== ENDPOINT PARA REPORTE ECONÓMICO FINAL ==================
@app.post("/api/reportes/listado-economico-anual")
async def obtener_listado_economico_anual(request: ReporteEconomicoFinalRequest, test_mode: bool = False):
    print("=== INICIO REPORTE ECONÓMICO FINAL ===")
    print(f"Datos recibidos: {request}")
    
    conn = None
    cursor = None
    try:
        connection = conectar_db(test_mode=test_mode)
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        
        # 1. Obtener datos base de la tabla ingresosygastos
        print("📊 Obteniendo datos transpuestos...")
        cursor.execute("""
            SELECT * FROM ingresosygastos 
            WHERE IGSede = %s 
            ORDER BY IGTiMo, IGMoTGas
        """, (request.codigoSede,))
        
        datos_base = cursor.fetchall()
        print(f"📝 Obtenidos {len(datos_base)} registros base")
        
        if not datos_base:
            return {
                "success": False,
                "message": "No hay datos en ingresosygastos. Ejecute primero la transposición.",
                "reporte": []
            }
        
        # 2. Aplicar post-procesamiento si se solicita
        if request.aplicarPostProceso:
            print("🔄 Aplicando post-procesamiento...")
            datos_procesados = aplicar_post_procesamiento_v2(datos_base)
        else:
            datos_procesados = datos_base
        
        # 3. Obtener información de la sede
        cursor.execute("SELECT LoNombre FROM locales WHERE LoCod = %s", (request.codigoSede,))
        sede_info = cursor.fetchone()
        nombre_sede = sede_info['LoNombre'] if sede_info else f"Sede {request.codigoSede}"
        
        cursor.close()
        connection.close()
        
        return {
            "success": True,
            "reporte": datos_procesados,
            "total_registros": len(datos_procesados),
            "metadatos": {
                "sede": request.codigoSede,
                "nombre_sede": nombre_sede,
                "año": request.año,
                "post_procesamiento_aplicado": request.aplicarPostProceso,
                "fecha_generacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        }
        
    except Exception as e:
        print(f"❌ ERROR en reporte económico final: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== FUNCIÓN AUXILIAR PARA POST-PROCESAMIENTO V2 ==================
def aplicar_post_procesamiento_v2(datos_base):
    """Aplica las reglas de post-procesamiento según especificaciones corregidas"""
    print("🔧 Iniciando post-procesamiento v2...")
    
    datos_procesados = []
    diezmos_acumulado = None
    
    # 1. Procesar registros existentes
    for registro in datos_base:
        tipo_mov = registro['IGTiMo']
        tipo_gas = registro['IGMoTGas']
        
        # Regla 1: Combinar Diezmos (100,2) + Ofrendas (100,3) → (100,2)
        if tipo_mov == 100 and tipo_gas == 2:  # DIEZMOS
            diezmos_acumulado = dict(registro)
            diezmos_acumulado['IGOpNom'] = 'DIEZMOS + OFRENDAS POR BANCO'
            continue
        elif tipo_mov == 100 and tipo_gas == 3:  # OFRENDAS
            if diezmos_acumulado:
                # Sumar ofrendas a diezmos
                for i in range(1, 13):
                    col_mes = f'IGM{i}'
                    diezmos_acumulado[col_mes] = float(diezmos_acumulado[col_mes] or 0) + float(registro[col_mes] or 0)
                
                # Recalcular total anual
                diezmos_acumulado['IGTotAno'] = sum(float(diezmos_acumulado[f'IGM{i}'] or 0) for i in range(1, 13))
                
                # Agregar el registro combinado
                datos_procesados.append(diezmos_acumulado)
                diezmos_acumulado = None
            # No agregar el registro de ofrendas por separado
            continue
        
        # Regla 2: Convertir TRASPASO (300,30) → DIEZMOS DEL CULTO (100,2)
        elif tipo_mov == 300 and tipo_gas == 30:
            registro_convertido = dict(registro)
            registro_convertido['IGTiMo'] = 100  # Cambiar a tipo ingresos
            registro_convertido['IGMoTGas'] = 2  # Cambiar a gasto diezmos
            registro_convertido['IGOpNom'] = 'DIEZMOS + OFRENDAS DEL CULTO'
            datos_procesados.append(registro_convertido)
            continue
        
        # Otros registros se mantienen igual
        datos_procesados.append(dict(registro))
    
    # 2. Agregar diezmos si quedó sin procesar
    if diezmos_acumulado:
        datos_procesados.append(diezmos_acumulado)
    
    # 3. Ordenar por tipo de movimiento y gasto
    datos_procesados.sort(key=lambda x: (x['IGTiMo'], x['IGMoTGas']))
    
    # 4. Agregar líneas de títulos y totales con códigos para ordenamiento
    datos_con_titulos = agregar_titulos_y_totales_v2(datos_procesados)
    
    # 5. Ordenamiento final por tipo y gasto
    datos_con_titulos.sort(key=lambda x: (x['IGTiMo'], x['IGMoTGas']))
    
    print(f"✅ Post-procesamiento v2 completado: {len(datos_con_titulos)} registros finales")
    return datos_con_titulos

# ================== FUNCIÓN PARA AGREGAR TÍTULOS Y TOTALES V2 ==================
def agregar_titulos_y_totales_v2(datos_procesados):
    """Agrega títulos y totales con códigos para ordenamiento correcto"""
    resultado = []
    
    # Preparar totales
    total_ingresos = {f'IGM{i}': 0.0 for i in range(1, 13)}
    total_gastos = {f'IGM{i}': 0.0 for i in range(1, 13)}
    
    # Obtener sede para los registros
    sede = datos_procesados[0]['IGSede'] if datos_procesados else 0
    
    # 1. SALDOS ANTERIORES (TiMo = 0) - Se mantienen al inicio
    saldos_anteriores = [r for r in datos_procesados if r['IGTiMo'] == 0]
    for saldo in saldos_anteriores:
        resultado.append(saldo)
    
    # 2. TÍTULO: INGRESOS (TiMo = 99, TGas = 0)
    resultado.append({
        'IGSede': sede,
        'IGTiMo': 99,
        'IGMoTGas': 0,
        'IGOpNom': 'INGRESOS',
        **{f'IGM{i}': None for i in range(1, 13)},
        'IGTotAno': None,
        'es_titulo': True
    })
    
    # 3. INGRESOS (TiMo = 100)
    ingresos = [r for r in datos_procesados if r['IGTiMo'] == 100]
    for ingreso in ingresos:
        resultado.append(ingreso)
        # Acumular para total
        for i in range(1, 13):
            col_mes = f'IGM{i}'
            total_ingresos[col_mes] += float(ingreso[col_mes] or 0)
    
    # 4. TOTAL DE INGRESOS (TiMo = 199, TGas = 0)
    resultado.append({
        'IGSede': sede,
        'IGTiMo': 199,
        'IGMoTGas': 0,
        'IGOpNom': 'TOTAL DE INGRESOS',
        **total_ingresos,
        'IGTotAno': sum(total_ingresos.values()),
        'es_total': True
    })
    
    # 5. TÍTULO: GASTOS Y TRANSFERENCIAS (TiMo = 200, TGas = 0) ← MOVIDO AQUÍ
    resultado.append({
        'IGSede': sede,
        'IGTiMo': 200,
        'IGMoTGas': 0,
        'IGOpNom': 'GASTOS Y TRANSFERENCIAS',
        **{f'IGM{i}': None for i in range(1, 13)},
        'IGTotAno': None,
        'es_titulo': True
    })
    
    # 6. GASTOS Y TRANSFERENCIAS (TiMo = 200, 300)
    gastos = [r for r in datos_procesados if r['IGTiMo'] in [200, 300]]
    for gasto in gastos:
        resultado.append(gasto)
        # Acumular para total
        for i in range(1, 13):
            col_mes = f'IGM{i}'
            total_gastos[col_mes] += float(gasto[col_mes] or 0)
    
    # 7. TOTAL DE GASTOS Y TRANSFERENCIAS (TiMo = 399, TGas = 0)
    resultado.append({
        'IGSede': sede,
        'IGTiMo': 399,
        'IGMoTGas': 0,
        'IGOpNom': 'TOTAL DE GASTOS Y TRANSFERENCIAS',
        **total_gastos,
        'IGTotAno': sum(total_gastos.values()),
        'es_total': True
    })
    
    # 8. SALDOS AL FIN DE MES (TiMo = 999, TGas = 0)
    saldos_finales = {}
    for i in range(1, 13):
        col_mes = f'IGM{i}'
        saldo_inicial = saldos_anteriores[0][col_mes] if saldos_anteriores else 0
        saldo_final = float(saldo_inicial or 0) + total_ingresos[col_mes] + total_gastos[col_mes]
        saldos_finales[col_mes] = saldo_final
    
    resultado.append({
        'IGSede': sede,
        'IGTiMo': 999,
        'IGMoTGas': 0,
        'IGOpNom': 'SALDOS AL FIN DE MES',
        **saldos_finales,
        'IGTotAno': None,
        'es_total': True
    })
    
    return resultado

# ================== PARA COMPROBRA SI FUNCIONA EL SERVIDOR ==================
@app.get("/")
def inicio():
    return {"mensaje": "Servidor activo"}
