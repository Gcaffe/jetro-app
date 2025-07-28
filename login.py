# C:\Proyectos\Jetro\BackEnd\login.py
# ✅ login.py con selección de iglesia y JWT
from fastapi import APIRouter, HTTPException, Depends, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional, List, Dict
import pymysql
import bcrypt
from jose import jwt, JWTError, ExpiredSignatureError
from datetime import datetime, timedelta
from db import conectar_db

# Configuración JWT
SECRET_KEY = "tu_clave_secreta_super_segura"
ALGORITHM = "HS256"
EXPIRATION_MINUTES = 60

router = APIRouter()
security = HTTPBearer()

# Modelos de entrada
class LoginInput(BaseModel):
    usuario: str
    clave: Optional[str] = None

class RegistroInput(BaseModel):
    usuario: str
    clave: Optional[str] = None
    nombre: str
    nivel: int = 5
    sedes: str = "999"

# Modelo de respuesta de token
class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"

# Crea un JWT con expiración como objeto datetime
def crear_token(data: Dict) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=EXPIRATION_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# Verifica el JWT
def verificar_token(authorization: str = Header(...)):
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Token no válido")

    token = authorization.split(" ")[1]

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload  # Retorna el usuario decodificado
    except JWTError:
        raise HTTPException(status_code=403, detail="Token inválido o expirado")

# Obtiene el usuario actual validando el token
def get_current_user(
    creds: HTTPAuthorizationCredentials = Depends(security)
) -> Dict:
    token = creds.credentials
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except ExpiredSignatureError:
        print("❌ JWT expirado")
        raise HTTPException(status_code=401, detail="Token expirado")
    except JWTError as e:
        print("❌ JWT inválido:", e)
        raise HTTPException(status_code=401, detail="Token inválido")
    return payload

# ---- Endpoint Login ----
@router.post("/login")
def login(datos: LoginInput, test_mode: bool = False):
    conn = conectar_db(test_mode=test_mode)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT UsID, UsCod, UsNombre, UsNivel, UsSedes, UsKeyWeb FROM usuarios WHERE UsCod = %s", (datos.usuario,))
            usuario = cursor.fetchone()

            if not usuario:
                raise HTTPException(status_code=401, detail="Usuario no encontrado")

            if not usuario["UsKeyWeb"]:  # Si la clave está vacía o NULL
                # Encriptar la nueva clave
                hashed = bcrypt.hashpw(datos.clave.encode("utf-8"), bcrypt.gensalt())
                cursor.execute("UPDATE usuarios SET UsKeyWeb = %s WHERE UsCod = %s", (hashed.decode(), datos.usuario))
                conn.commit()
            else:
                if not bcrypt.checkpw(datos.clave.encode("utf-8"), usuario["UsKeyWeb"].encode("utf-8")):
                    raise HTTPException(status_code=401, detail="Contraseña incorrecta")

            token_data = {
                "sub": usuario["UsCod"],
                "id": usuario["UsID"],
                "nivel": usuario["UsNivel"],
                "sedes": usuario["UsSedes"],
                "exp": datetime.utcnow() + timedelta(hours=12)
            }

            token = jwt.encode(token_data, SECRET_KEY, algorithm="HS256")
            return {"access_token": token, "token_type": "bearer"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en login: {e}")
    finally:
        conn.close()

# Para seleccionar la Iglesia asignada al Usuario
@router.get("/iglesias/{sede_ids}")
def obtener_iglesias(sede_ids: str, test_mode: bool = False):
    try:
        conexion = conectar_db(test_mode=test_mode)
        cursor = conexion.cursor()

        if sede_ids == "999":
            cursor.execute("SELECT LoCod, LoNombre FROM locales WHERE LoSituacion=1 ORDER BY LoNombre")
        elif "," in sede_ids:
            ids = tuple(sede_ids.split(","))
            sql = f"SELECT LoCod, LoNombre FROM locales WHERE LoSituacion=1 AND LoCod IN {ids}"
            cursor.execute(sql)
        else:
            cursor.execute("SELECT LoCod, LoNombre FROM locales WHERE LoSituacion=1 AND LoCod = %s", (sede_ids,))

        iglesias = cursor.fetchall()
        return iglesias

    except Exception as e:
        print("Error:", e)
        raise HTTPException(status_code=500, detail="Error al obtener iglesias")
    finally:
        cursor.close()
        conexion.close()

# ---- Endpoint Iglesias ----
@router.get("/iglesias", response_model=List[Dict])
def obtener_todas_iglesias(test_mode: bool = False, usuario: dict = Depends(verificar_token)):
    try:
        db = conectar_db(test_mode=test_mode)
        sedes_str = usuario["UsSedes"]
        print("Usuario recibido:", usuario)

        if sedes_str == "999":
            iglesias = db.execute("SELECT LoCod, LoNombre FROM locales WHERE LoSituacion = 1").fetchall()
        elif "," in sedes_str:
            iglesias = db.execute(
                f"SELECT LoCod, LoNombre FROM locales WHERE LoSituacion=1 AND LoCod IN ({sedes_str})"
            ).fetchall()
        else:
            iglesias = db.execute(
                "SELECT LoCod, LoNombre FROM locales WHERE LoSituacion=1 AND LoCod = :cod",
                {"cod": sedes_str}
            ).fetchall()

        return [{"LoCod": i[0], "LoNombre": i[1]} for i in iglesias]

    except Exception as e:
        print(f"Error al obtener iglesias: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ---- Endpoint Me ----
@router.get("/me")
def me(test_mode: bool = False, usuario: Dict = Depends(get_current_user)) -> Dict:
    conn = None
    cursor = None
    try:
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT UsCod, UsNombre, UsNivel, UsSedes FROM usuarios WHERE UsCod = %s",
            (usuario['sub'],)
        )
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        return user
    except HTTPException:
        raise
    except Exception as e:
        print("❌ ERROR en /me:", e)
        raise HTTPException(status_code=500, detail="Error interno al obtener perfil")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

 
# ---- Endpoint Register ----
@router.post("/register")
def registrar_usuario(datos: RegistroInput, test_mode: bool = False):
    conn = None
    cursor = None
    try:
        conn = conectar_db(test_mode=test_mode)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM usuarios WHERE UsCod = %s", (datos.usuario,))
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="Usuario ya existe")

        cursor.execute(
            """
            INSERT INTO usuarios
              (UsCod, UsKeyWeb, UsNombre, UsActivo, UsNivel, UsSedes, UsPermisos, UsFecha) VALUES (%s, %s, %s, 1, %s, %s, '', NOW())
            """,
            (datos.usuario, None, datos.nombre, datos.nivel, datos.sedes)
        )
        conn.commit()
        return {"mensaje": "Usuario creado correctamente"}
    except HTTPException as e:
        raise e
    except Exception as e:
        print("❌ ERROR en registro:", e)
        raise HTTPException(status_code=500, detail="Error al registrar usuario")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()