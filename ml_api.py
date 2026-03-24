"""
ml_api.py — Wrapper para la API de MercadoLibre
"""
import json
import requests
from config import ML_API_BASE, TOKENS_FILE

# Credenciales de la app ML (mismas para ambas cuentas)
_APP_ID     = "8902165405612832"
_SECRET_KEY = "CQeXfw4DjRWaMlg3ouTQIF134vctDxLi"


def _load_tokens() -> dict:
    """Carga tokens: BD (producción) → env var (fallback) → archivo local."""
    import os, db as _db
    if os.environ.get("ML_TOKENS_JSON"):
        # Producción: intentar BD primero (tiene los tokens refrescados)
        db_tokens = _db.load_tokens_db()
        if db_tokens:
            return db_tokens
        # Primera vez: usar env var y persistir en BD
        tokens = json.loads(os.environ["ML_TOKENS_JSON"])
        _db.save_tokens_db(tokens)
        return tokens
    with open(TOKENS_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def _save_tokens(tokens: dict) -> None:
    """Guarda tokens: BD (producción) o archivo local (desarrollo)."""
    import os, db as _db
    if os.environ.get("ML_TOKENS_JSON"):
        _db.save_tokens_db(tokens)
        return
    with open(TOKENS_FILE, 'w', encoding='utf-8') as f:
        json.dump(tokens, f, ensure_ascii=False, indent=2)


def refresh_token(cuenta: str) -> str:
    """
    Refresca el access_token usando el refresh_token guardado.
    Actualiza meli_tokens.json y retorna el nuevo access_token.
    """
    tokens = _load_tokens()
    if cuenta not in tokens:
        raise ValueError(f"Cuenta '{cuenta}' no encontrada.")

    rt = tokens[cuenta].get('refresh_token')
    if not rt:
        raise ValueError(f"No hay refresh_token para '{cuenta}'.")

    resp = requests.post(
        f"{ML_API_BASE}/oauth/token",
        data={
            'grant_type':    'refresh_token',
            'client_id':     _APP_ID,
            'client_secret': _SECRET_KEY,
            'refresh_token': rt,
        },
        headers={'Accept': 'application/json', 'Content-Type': 'application/x-www-form-urlencoded'},
        timeout=20
    )

    if resp.status_code != 200:
        raise RuntimeError(f"Error refrescando token de {cuenta}: {resp.status_code} {resp.text[:200]}")

    data = resp.json()
    tokens[cuenta]['access_token']  = data['access_token']
    tokens[cuenta]['refresh_token'] = data.get('refresh_token', rt)
    _save_tokens(tokens)

    print(f"  [ml_api] Token de {cuenta} refrescado OK (expira en {data.get('expires_in', '?')}s)")
    return data['access_token']


def get_token(cuenta: str, auto_refresh: bool = True) -> str:
    """
    Lee el access_token de la cuenta indicada.
    Si auto_refresh=True, intenta refrescarlo automáticamente si está expirado (401).
    """
    tokens = _load_tokens()
    if cuenta not in tokens:
        raise ValueError(f"Cuenta '{cuenta}' no encontrada. Disponibles: {list(tokens.keys())}")

    token = tokens[cuenta]['access_token']

    if auto_refresh:
        # Verificar si el token es válido con un ping barato
        resp = requests.get(
            f"{ML_API_BASE}/users/me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10
        )
        if resp.status_code == 401:
            print(f"  [ml_api] Token de {cuenta} expirado — refrescando...")
            token = refresh_token(cuenta)

    return token


def get_category_info(category_id: str, token: str) -> dict:
    """Retorna la info completa de una categoría (incluye settings.catalog_domain)."""
    resp = requests.get(
        f"{ML_API_BASE}/categories/{category_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15
    )
    if resp.status_code == 200:
        return resp.json()
    return {}


def get_category_attributes(category_id: str, token: str) -> list:
    """
    Retorna la lista de atributos de una categoría ML.
    Incluye cuáles son obligatorios (tags.required = True).
    """
    resp = requests.get(
        f"{ML_API_BASE}/categories/{category_id}/attributes",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15
    )
    if resp.status_code == 200:
        return resp.json()
    print(f"  [ml_api] Error obteniendo atributos de {category_id}: {resp.status_code}")
    return []


def search_gtin_in_catalog(category_id: str, title: str, token: str) -> str | None:
    """
    Busca un producto similar en el catálogo ML por título+categoría.
    Retorna el primer GTIN encontrado, o None.
    """
    try:
        resp = requests.get(
            f"{ML_API_BASE}/sites/MLM/search",
            params={'category': category_id, 'q': title[:80], 'limit': 5},
            headers={'Authorization': f'Bearer {token}'},
            timeout=15,
        )
        if resp.status_code != 200:
            return None
        results = resp.json().get('results', [])
        for item in results:
            for attr in item.get('attributes', []):
                if attr.get('id') == 'GTIN' and attr.get('value_name') not in (None, '', '0000000000000'):
                    return attr['value_name']
    except Exception:
        pass
    return None


def search_gtin_upc(brand: str, query: str) -> str | None:
    """
    Busca GTIN en UPC Item DB por query (título genérico o modelo).
    Retorna el primer EAN/UPC encontrado, o None.
    """
    try:
        query = f"{brand} {query}".strip() if brand else query.strip()
        resp = requests.get(
            'https://api.upcitemdb.com/prod/trial/search',
            params={'s': query, 'match_mode': 0, 'type': 'product'},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        items = resp.json().get('items', [])
        for item in items:
            for ean in item.get('ean', []):
                if ean and ean != '0000000000000':
                    return ean
            for upc in item.get('upc', []):
                if upc and upc != '000000000000':
                    return upc
    except Exception:
        pass
    return None


def preupload_picture(image_url: str, token: str) -> str | None:
    """
    Pre-sube una imagen a ML descargandola de la URL y subiendola directamente.
    Retorna el picture_id de ML, o None si falla.
    """
    try:
        img_resp = requests.get(image_url, timeout=30)
        if img_resp.status_code != 200:
            return None
        content_type = img_resp.headers.get('Content-Type', 'image/jpeg')
        resp = requests.post(
            f"{ML_API_BASE}/pictures",
            headers={"Authorization": f"Bearer {token}"},
            files={"file": ("image.jpg", img_resp.content, content_type)},
            timeout=60
        )
        if resp.status_code == 201:
            return resp.json().get('id')
        return None
    except Exception:
        return None


def create_item(payload: dict, token: str) -> tuple[dict, int]:
    """
    Crea un item en MercadoLibre.
    Retorna (respuesta_json, status_code).
    El item se crea en status 'paused' para revisión antes de publicar.
    """
    resp = requests.post(
        f"{ML_API_BASE}/items",
        json=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        timeout=30
    )
    try:
        return resp.json(), resp.status_code
    except Exception:
        return {"error": resp.text}, resp.status_code


def upload_pictures(item_id: str, image_urls: list, token: str) -> tuple[dict, int]:
    """
    Sube/actualiza las imágenes de un item existente via PUT.
    image_urls: lista de URLs de imágenes.
    Retorna (respuesta_json, status_code).
    """
    pictures = [{'source': url} for url in image_urls]
    resp = requests.put(
        f"{ML_API_BASE}/items/{item_id}",
        json={'pictures': pictures},
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        timeout=30
    )
    try:
        return resp.json(), resp.status_code
    except Exception:
        return {"error": resp.text}, resp.status_code


def update_description(item_id: str, plain_text: str, token: str) -> int:
    """
    Actualiza (o crea) la descripción de un item existente.
    Usa PUT si ya existe, POST si es nuevo.
    Retorna el status_code de la respuesta.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    body = {"plain_text": plain_text}
    url  = f"{ML_API_BASE}/items/{item_id}/description"

    # Intentar PUT (actualizar)
    resp = requests.put(url, json=body, headers=headers, timeout=20)
    if resp.status_code in (200, 201):
        return resp.status_code

    # Si 404, intentar POST (crear por primera vez)
    if resp.status_code == 404:
        resp = requests.post(url, json=body, headers=headers, timeout=20)

    return resp.status_code


def pause_item(item_id: str, token: str) -> int:
    """Pausa un item activo."""
    try:
        resp = requests.put(
            f"{ML_API_BASE}/items/{item_id}",
            json={"status": "paused"},
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            timeout=30
        )
        return resp.status_code
    except requests.exceptions.Timeout:
        return -1  # timeout — no crashear, el item ya fue creado


def activate_item(item_id: str, token: str) -> int:
    """Activa (publica) un item pausado."""
    resp = requests.put(
        f"{ML_API_BASE}/items/{item_id}",
        json={"status": "active"},
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        timeout=15
    )
    return resp.status_code