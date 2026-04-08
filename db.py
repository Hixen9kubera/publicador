"""
db.py — Conexión y operaciones de base de datos para backlogs de publicación ML.

Reemplaza el almacenamiento en archivos JSON por una tabla MySQL.
Las credenciales se inyectan en set_credentials() desde config.py o variables de entorno.
"""

import json
import os
from datetime import datetime


# ── Cifrado de tokens ─────────────────────────────────────────────────────────

def _get_fernet():
    key = os.environ.get("DB_ENCRYPTION_KEY")
    if not key:
        return None
    from cryptography.fernet import Fernet
    return Fernet(key.encode() if isinstance(key, str) else key)


def _encrypt(value: str) -> str:
    f = _get_fernet()
    if not f:
        return value
    return f.encrypt(value.encode()).decode()


def _decrypt(value: str) -> str:
    f = _get_fernet()
    if not f:
        return value
    try:
        return f.decrypt(value.encode()).decode()
    except Exception:
        return value  # si no está cifrado lo retorna tal cual

# ── Credenciales (se asignan desde config.py o set_credentials()) ─────────────
_DB_HOST     = None
_DB_PORT     = 3306
_DB_NAME     = None
_DB_USER     = None
_DB_PASSWORD = None

_conn = None  # conexión reutilizable


def set_credentials(host: str, port: int, name: str, user: str, password: str):
    """Llamar antes de cualquier operación de BD."""
    global _DB_HOST, _DB_PORT, _DB_NAME, _DB_USER, _DB_PASSWORD
    _DB_HOST     = host
    _DB_PORT     = port
    _DB_NAME     = name
    _DB_USER     = user
    _DB_PASSWORD = password


def _get_conn():
    """Retorna una conexión activa, reconectando si es necesario."""
    global _conn
    try:
        import mysql.connector
    except ImportError:
        raise ImportError("Instala mysql-connector-python: pip install mysql-connector-python")

    if _conn is not None:
        try:
            _conn.ping(reconnect=True, attempts=3, delay=2)
        except Exception:
            _conn = None

    if _conn is None:
        _conn = mysql.connector.connect(
            host=_DB_HOST,
            port=_DB_PORT,
            database=_DB_NAME,
            user=_DB_USER,
            password=_DB_PASSWORD,
            charset='utf8mb4',
            autocommit=True,
        )
    return _conn


def ensure_connection(max_retries: int = 5, base_delay: int = 5) -> bool:
    """
    Intenta conectarse a la BD con reintentos y backoff exponencial.
    Retorna True si logró conectar, False si agotó los reintentos.
    """
    import time
    for attempt in range(1, max_retries + 1):
        try:
            conn = _get_conn()
            conn.ping(reconnect=True)
            print(f"  [db] Conexión a BD establecida (intento {attempt}/{max_retries})")
            return True
        except Exception as e:
            global _conn
            _conn = None
            delay = base_delay * (2 ** (attempt - 1))  # 5, 10, 20, 40, 80s
            if attempt < max_retries:
                print(f"  [db] Intento {attempt}/{max_retries} fallido: {e}")
                print(f"  [db] Reintentando en {delay}s...")
                time.sleep(delay)
            else:
                print(f"  [db] No se pudo conectar tras {max_retries} intentos: {e}")
    return False


# ── DDL ───────────────────────────────────────────────────────────────────────

CREATE_TOKENS_SQL = """
CREATE TABLE IF NOT EXISTS ml_tokens (
    cuenta         VARCHAR(50)   NOT NULL PRIMARY KEY,
    access_token   VARCHAR(500)  NOT NULL,
    refresh_token  VARCHAR(500)  NOT NULL,
    updated_at     DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Tokens ML actualizados por refresh';
"""

CREATE_PROGRESS_SQL = """
CREATE TABLE IF NOT EXISTS ml_progress (
    prog_key     VARCHAR(150) NOT NULL PRIMARY KEY COMMENT 'cuenta:sku',
    cuenta       VARCHAR(50)  NOT NULL,
    sku          VARCHAR(100) NOT NULL,
    wc_id        INT          DEFAULT NULL,
    ml_item_id   VARCHAR(60)  DEFAULT NULL,
    ml_url       TEXT         DEFAULT NULL,
    success      TINYINT(1)   NOT NULL DEFAULT 0,
    error        TEXT         DEFAULT NULL,
    gtin_error   TINYINT(1)   NOT NULL DEFAULT 0,
    dry_run      TINYINT(1)   NOT NULL DEFAULT 0,
    published_at DATETIME     DEFAULT NULL,
    updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_prog_cuenta  (cuenta),
    INDEX idx_prog_success (success)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Estado de publicaciones (reemplaza progress.json)';
"""

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ml_backlog (
    id               INT AUTO_INCREMENT PRIMARY KEY,
    run_key          VARCHAR(150)  NOT NULL COMMENT 'cuenta:sku',
    cuenta           VARCHAR(50)   NOT NULL,
    sku              VARCHAR(100)  NOT NULL,
    wc_id            INT           DEFAULT NULL,
    ml_item_id       VARCHAR(60)   DEFAULT NULL,
    ml_url           TEXT          DEFAULT NULL,
    success          TINYINT(1)    NOT NULL DEFAULT 0,
    error            TEXT          DEFAULT NULL,
    ml_status        SMALLINT      DEFAULT NULL COMMENT 'HTTP status de POST /items',
    desc_status      SMALLINT      DEFAULT NULL COMMENT 'HTTP status de PUT /description',
    pics_preuploaded TINYINT       DEFAULT 0,
    payload          JSON          DEFAULT NULL,
    ml_response      JSON          DEFAULT NULL,
    published_at     DATETIME      DEFAULT NULL,
    created_at       DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    gtin_error       TINYINT(1)    NOT NULL DEFAULT 0 COMMENT '1 si falló por GTIN inválido',
    INDEX idx_sku        (sku),
    INDEX idx_cuenta     (cuenta),
    INDEX idx_success    (success),
    INDEX idx_gtin_error (gtin_error),
    INDEX idx_created    (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Historial de publicaciones WC->ML';
"""


def create_tables():
    """Crea todas las tablas si no existen."""
    conn = _get_conn()
    cur  = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    cur.execute(CREATE_PROGRESS_SQL)
    cur.execute(CREATE_TOKENS_SQL)
    cur.close()
    print("  [db] Tablas ml_backlog + ml_progress + ml_tokens listas.")


# ── Tokens ML (persistencia en producción) ────────────────────────────────────

def load_tokens_db() -> dict:
    """Carga y descifra tokens desde la BD. Retorna {} si no hay ninguno."""
    try:
        conn = _get_conn()
        cur  = conn.cursor(dictionary=True)
        cur.execute("SELECT cuenta, access_token, refresh_token FROM ml_tokens")
        rows = cur.fetchall()
        cur.close()
        return {
            r['cuenta']: {
                'access_token':  _decrypt(r['access_token']),
                'refresh_token': _decrypt(r['refresh_token']),
            }
            for r in rows
        }
    except Exception:
        return {}


def save_tokens_db(tokens: dict):
    """Cifra y guarda tokens en la BD (upsert por cuenta)."""
    try:
        conn = _get_conn()
        cur  = conn.cursor()
        for cuenta, data in tokens.items():
            cur.execute("""
                INSERT INTO ml_tokens (cuenta, access_token, refresh_token)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    access_token  = VALUES(access_token),
                    refresh_token = VALUES(refresh_token),
                    updated_at    = CURRENT_TIMESTAMP
            """, (
                cuenta,
                _encrypt(data['access_token']),
                _encrypt(data['refresh_token']),
            ))
        cur.close()
    except Exception as e:
        print(f"  [db] Advertencia — no se pudo guardar tokens en BD: {e}")


# ── Progress (reemplaza progress.json en producción) ──────────────────────────

UPSERT_PROGRESS_SQL = """
INSERT INTO ml_progress
    (prog_key, cuenta, sku, wc_id, ml_item_id, ml_url,
     success, error, gtin_error, dry_run, published_at)
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    wc_id        = VALUES(wc_id),
    ml_item_id   = VALUES(ml_item_id),
    ml_url       = VALUES(ml_url),
    success      = VALUES(success),
    error        = VALUES(error),
    gtin_error   = VALUES(gtin_error),
    dry_run      = VALUES(dry_run),
    published_at = VALUES(published_at),
    updated_at   = CURRENT_TIMESTAMP
"""


def load_progress_db() -> dict:
    """Carga todo el progreso desde la BD. Equivalente a leer progress.json."""
    try:
        conn = _get_conn()
        cur  = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM ml_progress")
        rows = cur.fetchall()
        cur.close()
        result = {}
        for r in rows:
            result[r['prog_key']] = {
                'cuenta':       r['cuenta'],
                'sku':          r['sku'],
                'wc_id':        r['wc_id'],
                'ml_item_id':   r['ml_item_id'],
                'ml_url':       r['ml_url'],
                'success':      bool(r['success']),
                'error':        r['error'],
                'gtin_error':   bool(r['gtin_error']),
                'dry_run':      bool(r['dry_run']),
                'published_at': str(r['published_at']) if r['published_at'] else None,
            }
        return result
    except Exception as e:
        print(f"  [db] No se pudo cargar progress de BD: {e}")
        return {}


def save_progress_db(prog_key: str, entry: dict):
    """Guarda o actualiza una entrada de progreso en la BD."""
    try:
        published_at = None
        if entry.get('published_at'):
            try:
                published_at = datetime.fromisoformat(entry['published_at'])
            except Exception:
                pass

        conn = _get_conn()
        cur  = conn.cursor()
        cur.execute(UPSERT_PROGRESS_SQL, (
            prog_key,
            entry.get('cuenta', ''),
            entry.get('sku', ''),
            entry.get('wc_id'),
            entry.get('ml_item_id'),
            entry.get('ml_url'),
            1 if entry.get('success') else 0,
            entry.get('error'),
            1 if entry.get('gtin_error') else 0,
            1 if entry.get('dry_run')    else 0,
            published_at,
        ))
        cur.close()
    except Exception as e:
        print(f"  [db] Advertencia — no se pudo guardar progress en BD: {e}")


# ── Operaciones ───────────────────────────────────────────────────────────────

INSERT_SQL = """
INSERT INTO ml_backlog
    (run_key, cuenta, sku, wc_id, ml_item_id, ml_url,
     success, error, ml_status, desc_status, pics_preuploaded,
     payload, ml_response, published_at, gtin_error)
VALUES
    (%s, %s, %s, %s, %s, %s,
     %s, %s, %s, %s, %s,
     %s, %s, %s, %s)
"""


def is_published(cuenta: str, sku: str) -> bool:
    """Retorna True si cuenta:sku tiene success=1 en ml_progress."""
    try:
        conn = _get_conn()
        cur  = conn.cursor()
        cur.execute(
            'SELECT success FROM ml_progress WHERE prog_key=%s LIMIT 1',
            (f'{cuenta}:{sku}',)
        )
        row = cur.fetchone()
        cur.close()
        return bool(row and row[0])
    except Exception:
        return False


def save_backlog_db(run_key: str, entry: dict):
    """
    Inserta una entrada de backlog en la base de datos.
    Silencia errores para no interrumpir el flujo principal.
    """
    try:
        result       = entry.get('result', {})
        payload_json = json.dumps(entry.get('payload'),      ensure_ascii=False) if entry.get('payload')      else None
        resp_json    = json.dumps(entry.get('ml_response'),  ensure_ascii=False) if entry.get('ml_response')  else None

        published_at = None
        if entry.get('published_at'):
            try:
                published_at = datetime.fromisoformat(entry['published_at'])
            except Exception:
                pass

        # Detectar si el error fue por GTIN inválido
        gtin_error = 0
        ml_response = entry.get('ml_response', {})
        if ml_response:
            for cause in ml_response.get('cause', []):
                code = cause.get('code', '')
                msg  = cause.get('message', '')
                if 'product_identifier.invalid_format' in code or (
                    'GTIN' in msg and ('invalid' in msg.lower() or 'format' in msg.lower())
                ):
                    gtin_error = 1
                    break

        conn = _get_conn()
        cur  = conn.cursor()
        cur.execute(INSERT_SQL, (
            run_key,
            entry.get('cuenta', ''),
            entry.get('result', {}).get('sku') or run_key.split(':')[-1],
            entry.get('wc_id'),
            result.get('ml_item_id'),
            result.get('ml_url'),
            1 if result.get('success') else 0,
            result.get('error'),
            entry.get('ml_status'),
            entry.get('desc_status'),
            entry.get('pics_preuploaded', 0),
            payload_json,
            resp_json,
            published_at,
            gtin_error,
        ))
        cur.close()
    except Exception as e:
        print(f"  [db] Advertencia — no se pudo guardar en BD: {e}")
