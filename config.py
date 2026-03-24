"""
config.py — Credenciales y constantes para publicaciones en MercadoLibre.

Todas las credenciales se leen EXCLUSIVAMENTE de variables de entorno.
Para desarrollo local crea un archivo .env y usa python-dotenv, o exporta
las variables en tu terminal antes de correr el script.
"""
import os

# ── WooCommerce ────────────────────────────────────────────────────────────────
WC_URL    = os.environ["WC_URL"]
WC_KEY    = os.environ["WC_KEY"]
WC_SECRET = os.environ["WC_SECRET"]

# ── MercadoLibre ───────────────────────────────────────────────────────────────
ML_API_BASE  = "https://api.mercadolibre.com"
ML_SITE_ID   = "MLM"
TOKENS_FILE  = os.environ.get(
    "ML_TOKENS_FILE",
    os.path.join(os.path.dirname(__file__), '..', 'config', 'meli_tokens.json')
)

ML_CUENTAS = ["SANCORFASHION", "BEKURA"]

# ── Publicación ────────────────────────────────────────────────────────────────
DELAY_ENTRE_PRODUCTOS = 10
MAX_IMAGENES          = 10
DEFAULT_CURRENCY      = "MXN"
DEFAULT_LISTING_TYPE  = "gold_special"
DEFAULT_CONDITION     = "new"
DEFAULT_BUYING_MODE   = "buy_it_now"
DEFAULT_QUANTITY      = 1
DEFAULT_BRAND         = "Ferrahome"
FREE_SHIPPING_MIN     = 149.0

# ── Archivos de progreso (solo en local — en producción se usa la BD) ──────────
DATA_DIR      = os.path.join(os.path.dirname(__file__), 'data')
PROGRESS_FILE = os.path.join(DATA_DIR, 'progress.json')

# ── Base de datos (MySQL/MariaDB) ──────────────────────────────────────────────
DB_HOST     = os.environ["DB_HOST"]
DB_PORT     = int(os.environ.get("DB_PORT", "3306"))
DB_NAME     = os.environ["DB_NAME"]
DB_USER     = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
