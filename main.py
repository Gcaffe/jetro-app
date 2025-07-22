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

# Función auxiliar para verificar permisos de administrador
def verificar_admin(auth_user):
    if auth_user.get("nivel") != 9:
        raise HTTPException(status_code=403, detail="Solo los administradores pueden realizar esta acción")

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

# ================== ENDPOINTS PARA IGLESIAS ==================
@app.get("/locales/{sede_ids}")
def obtener_locales(sede_ids: str, auth=Depends(get_current_user)):
    """Obtener todos los locales de las sedes especificadas"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Obteniendo locales para sedes: {sede_ids}")
        conn = conectar_db()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
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

@app.get("/locales/{sede_ids}")
def obtener_locales(sede_ids: str, auth=Depends(get_current_user)):
    """Obtener todos los locales de las sedes especificadas"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Obteniendo locales para sedes: {sede_ids}")
        conn = conectar_db()
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

# ================== MODELOS PARA REPORTES ==================
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

@app.get("/locales/detalle/{local_id}")
def obtener_local_detalle(local_id: int, auth=Depends(get_current_user)):
    """Obtener un local específico por ID"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Obteniendo detalle del local ID: {local_id}")
        conn = conectar_db()
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

@app.get("/nuevo-codigo-local")
def obtener_nuevo_codigo(auth=Depends(get_current_user)):
    conn = None
    cursor = None
    try:
        conn = conectar_db()
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
        
@app.post("/locales")
def crear_local(local: LocalCreate, auth=Depends(get_current_user)):
    """Crear un nuevo local"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Creando nuevo local: {local.LoNombre}")
        conn = conectar_db()
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

@app.put("/locales/{local_id}")
def actualizar_local(local_id: int, local: LocalUpdate, auth=Depends(get_current_user)):
    """Actualizar un local existente"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Actualizando local ID: {local_id}")
        conn = conectar_db()
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

@app.delete("/locales/{local_id}")
def eliminar_local(local_id: int, auth=Depends(get_current_user)):
    """Eliminar un local (soft delete - cambiar situación a 0)"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Eliminando local ID: {local_id}")
        conn = conectar_db()
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
def obtener_fieles(sede_id: str, auth=Depends(get_current_user)):
    """Obtener todos los fieles de la sede especificada"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Obteniendo fieles para sede: {sede_id}")
        conn = conectar_db()
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

@app.get("/fieles/detalle/{fiel_id}")
def obtener_fiel_detalle(fiel_id: int, auth=Depends(get_current_user)):
    """Obtener un fiel específico por ID"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Obteniendo detalle del fiel ID: {fiel_id}")
        conn = conectar_db()
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

@app.get("/nuevo-codigo-fiel")
def obtener_nuevo_codigo_fiel(auth=Depends(get_current_user)):
    """Obtener el siguiente código disponible para fiel"""
    conn = None
    cursor = None
    try:
        conn = conectar_db()
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(fiCod)+1 as nuevoCodigo FROM fieles")
        resultado = cursor.fetchone()
        nuevo_codigo = resultado[0] if resultado[0] else 1001
        return {"nuevoCodigo": nuevo_codigo}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@app.post("/fieles")
def crear_fiel(fiel: FielCreate, auth=Depends(get_current_user)):
    """Crear un nuevo fiel"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Creando nuevo fiel: {fiel.fiNombres} {fiel.fiApellidos}")
        conn = conectar_db()
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

@app.put("/fieles/{fiel_id}")
def actualizar_fiel(fiel_id: int, fiel: FielUpdate, auth=Depends(get_current_user)):
    """Actualizar un fiel existente"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Actualizando fiel ID: {fiel_id}")
        conn = conectar_db()
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

@app.delete("/fieles/{fiel_id}")
def eliminar_fiel(fiel_id: int, auth=Depends(get_current_user)):
    """Eliminar un fiel (soft delete - cambiar situación a 0)"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Eliminando fiel ID: {fiel_id}")
        conn = conectar_db()
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
def obtener_usuarios(auth=Depends(get_current_user)):
    """Obtener todos los usuarios - Solo administradores"""
    conn = None
    cursor = None
    try:
        # Verificar que sea administrador
        verificar_admin(auth)
        
        print("▶️ Obteniendo usuarios")
        conn = conectar_db()
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

@app.get("/usuarios/detalle/{usuario_id}")
def obtener_usuario_detalle(usuario_id: int, auth=Depends(get_current_user)):
    """Obtener un usuario específico por ID - Solo administradores"""
    conn = None
    cursor = None
    try:
        verificar_admin(auth)
        
        print(f"▶️ Obteniendo detalle del usuario ID: {usuario_id}")
        conn = conectar_db()
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

@app.get("/nuevo-codigo-usuario")
def obtener_nuevo_codigo_usuario(auth=Depends(get_current_user)):
    """Obtener el siguiente código disponible para usuario"""
    conn = None
    cursor = None
    try:
        verificar_admin(auth)
        
        conn = conectar_db()
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

@app.post("/usuarios")
def crear_usuario(usuario: UsuarioCreate, auth=Depends(get_current_user)):
    """Crear un nuevo usuario - Solo administradores"""
    conn = None
    cursor = None
    try:
        verificar_admin(auth)
        
        print(f"▶️ Creando nuevo usuario: {usuario.UsNombre}")
        conn = conectar_db()
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

@app.put("/usuarios/{usuario_id}")
def actualizar_usuario(usuario_id: int, usuario: UsuarioUpdate, auth=Depends(get_current_user)):
    """Actualizar un usuario existente - Solo administradores"""
    conn = None
    cursor = None
    try:
        verificar_admin(auth)
        
        print(f"▶️ Actualizando usuario ID: {usuario_id}")
        conn = conectar_db()
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

@app.delete("/usuarios/{usuario_id}")
def eliminar_usuario(usuario_id: int, auth=Depends(get_current_user)):
    """Eliminar un usuario (soft delete - cambiar UsActivo a 0)"""
    conn = None
    cursor = None
    try:
        verificar_admin(auth)
        
        print(f"▶️ Eliminando usuario ID: {usuario_id}")
        conn = conectar_db()
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

# Endpoint adicional para cambiar contraseña
@app.put("/usuarios/{usuario_id}/password")
def cambiar_password(usuario_id: int, nueva_password: str, auth=Depends(get_current_user)):
    """Cambiar contraseña de un usuario - Solo administradores o el propio usuario"""
    conn = None
    cursor = None
    try:
        # Verificar permisos: admin o el propio usuario
        if auth.get("nivel") != 9 and auth.get("id") != usuario_id:
            raise HTTPException(status_code=403, detail="No tienes permisos para cambiar esta contraseña")
        
        print(f"▶️ Cambiando contraseña del usuario ID: {usuario_id}")
        conn = conectar_db()
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
async def obtener_ingresos_gastos(request: ReporteIngresosGastosRequest):
    print("=== INICIO REPORTE INGRESOS-GASTOS ===")
    print(f"Datos recibidos: {request}")
    
    try:
        # Obtener conexión a la BD
        print("Intentando conectar a la BD...")
        connection = conectar_db()
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

# Nuevo endpoint para Diezmos y Ofrendas
@app.post("/api/reportes/diezmos-ofrendas")
async def obtener_diezmos_ofrendas(request: ReporteDiezmosOfrendasRequest):
    print("=== INICIO REPORTE DIEZMOS Y OFRENDAS ===")
    print(f"Datos recibidos: {request}")
    
    try:
        connection = conectar_db()
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        
        # Consulta SQL según especificaciones del PDF
        if request.soloDomingos:
            # SOBRE LOS INGRESOS EN EL CULTO (DOMINGOS)
            sql_query = """
                SELECT 
                    m.MoFecha,
                    o.OpNombre,
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
                GROUP BY m.MoFecha, o.OpCod 
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
def obtener_segundo_nivel(tipo: int, sede: int, auth=Depends(get_current_user)):
    """Obtener opciones de segundo nivel según el tipo de operación y sede"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Obteniendo segundo nivel: tipo={tipo}, sede={sede}")
        conn = conectar_db()
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
def obtener_tercer_nivel(accion: str, sede: int, auth=Depends(get_current_user)):
    """Obtener opciones de tercer nivel según MnuSigAccion"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Obteniendo tercer nivel: accion={accion}, sede={sede}")
        conn = conectar_db()
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
def obtener_movimientos(año: int, mes: int, sede: int, auth=Depends(get_current_user)):
    """Obtener movimientos de un período específico"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Obteniendo movimientos: año={año}, mes={mes}, sede={sede}")
        conn = conectar_db()
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
def grabar_movimiento(movimiento: MovimientoCreate, auth=Depends(get_current_user)):
    """Grabar un nuevo movimiento"""
    conn = None
    cursor = None
    try:
        print(f"▶️ Grabando movimiento: {movimiento}")
        conn = conectar_db()
        cursor = conn.cursor()
        
        # Crear fecha completa
        fecha = f"{movimiento.año}-{movimiento.mes:02d}-{movimiento.dia:02d}"
        
        # Determinar valores según origen
        caja = movimiento.importe if movimiento.origen == "caja" else 0
        banco = movimiento.importe if movimiento.origen == "banco" else 0
        
        # SQL para insertar movimiento
        query = """
        INSERT INTO movimientos (
            MoSede, MoTiMo, MoTGas, MoRubr, MoFecha, MoDesc, 
            MoImporte, MoCChica, MoUser, MoHecho
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
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
            auth.get("id", 0)
        )
        
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

# Para comprobar si funciona el Servidor
@app.get("/")
def inicio():
    return {"mensaje": "Servidor activo"}
