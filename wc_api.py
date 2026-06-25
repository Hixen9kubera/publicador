"""
wc_api.py — Obtener productos de WooCommerce para publicar en ML
"""
import re
import time
import xmlrpc.client
import requests
from requests.exceptions import RequestException
from config import WC_URL, WC_KEY, WC_SECRET

AUTH = (WC_KEY, WC_SECRET)
BASE = f"{WC_URL}/wp-json/wc/v3"

# Statuses estándar que acepta la WC REST API
_WC_STANDARD_STATUSES = {'any', 'future', 'trash', 'draft', 'pending', 'private', 'publish'}

# Credenciales XML-RPC (para statuses customizados: inprogress, ready, etc.)
_XMLRPC_URL  = f"{WC_URL}/xmlrpc.php"
_XMLRPC_USER = "brandon@kubera.mx"
_XMLRPC_PASS = "KV^!3nD!Ogh88uYHr)h!fo1a"

# Errores HTTP transitorios que justifican reintento
_RETRY_STATUSES = {429, 500, 502, 503, 504}


def _request_with_retry(method: str, url: str, max_retries: int = 4, **kwargs):
    """
    Wrapper de requests con reintentos + backoff para errores transitorios.

    Cubre tanto excepciones de conexión (RemoteDisconnected, ConnectionError,
    timeouts — frecuentes cuando WooCommerce/Cloudflare cierra la conexión a
    mitad de una tanda grande de GETs por-ID) como respuestas 429/5xx.

    Retorna el objeto Response, o None si se agotaron los reintentos. El llamador
    debe tolerar None (saltar ese item) en vez de crashear toda la corrida.
    """
    kwargs.setdefault('timeout', 30)
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.request(method, url, **kwargs)
        except RequestException as e:
            if attempt < max_retries:
                wait = min(2 ** (attempt - 1), 15)  # 1, 2, 4, 8 → cap 15s
                print(f"  [wc_api] {type(e).__name__} en {url} "
                      f"(intento {attempt}/{max_retries}) — reintento en {wait}s")
                time.sleep(wait)
                continue
            print(f"  [wc_api] Falló tras {max_retries} intentos: {type(e).__name__}: {e}")
            return None
        # Reintento a nivel de status (429 / 5xx)
        if resp.status_code in _RETRY_STATUSES and attempt < max_retries:
            retry_after = resp.headers.get('Retry-After')
            wait = int(retry_after) if (retry_after or '').isdigit() else min(2 ** (attempt - 1), 15)
            print(f"  [wc_api] HTTP {resp.status_code} en {url} "
                  f"(intento {attempt}/{max_retries}) — reintento en {wait}s")
            time.sleep(wait)
            continue
        return resp
    return None


def _get(endpoint: str, params: dict) -> list:
    url = f"{BASE}/{endpoint}"
    print(f"  [wc_api] GET {url}")
    print(f"  [wc_api] Params: {params}")
    resp = _request_with_retry('GET', url, params=params, auth=AUTH)
    if resp is None:
        print(f"  [wc_api] Error de conexión persistente en {endpoint} — devuelvo []")
        return []
    print(f"  [wc_api] Status: {resp.status_code}")
    if resp.status_code == 200:
        data = resp.json()
        print(f"  [wc_api] Productos recibidos: {len(data)}")
        return data
    print(f"  [wc_api] Error {resp.status_code} en {endpoint}")
    print(f"  [wc_api] Respuesta: {resp.text[:500]}")
    return []


def _xmlrpc_get_product_ids(status: str, number: int = 500) -> list[int]:
    """
    Usa XML-RPC para obtener IDs de productos con status customizado
    (inprogress, ready, etc.) que la WC REST API no puede filtrar.
    """
    try:
        proxy = xmlrpc.client.ServerProxy(_XMLRPC_URL)
        posts = proxy.wp.getPosts(
            1, _XMLRPC_USER, _XMLRPC_PASS,
            {
                'post_type':   'product',
                'post_status': status,
                'number':      number,
            },
            ['post_id', 'post_status']
        )
        ids = [int(p['post_id']) for p in posts]
        print(f"  [wc_api] XML-RPC encontró {len(ids)} productos con status='{status}'")
        return ids
    except Exception as e:
        print(f"  [wc_api] XML-RPC error: {e}")
        return []


def get_products(status='pending', tag_id=None, id_min=None, id_max=None,
                 per_page=100) -> list:
    """
    Obtiene todos los productos de WooCommerce con los filtros dados.
    - status:  'pending', 'ready', 'inprogress', 'any', etc.
               Si es un status customizado usa XML-RPC para obtener los IDs
               y luego los enriquece con la WC REST API.
    - tag_id:  filtrar por tag (ej: 1799 para tag '41', 1800 para tag '52')
    - id_min / id_max: rango de IDs para dividir carga entre workers
    """
    is_custom = status not in _WC_STANDARD_STATUSES

    if is_custom:
        # 1. Obtener IDs via XML-RPC
        ids = _xmlrpc_get_product_ids(status)
        if not ids:
            return []

        # 2. Filtrar por rango si aplica
        if id_min or id_max:
            ids = [i for i in ids if
                   (id_min is None or i >= id_min) and
                   (id_max is None or i <= id_max)]

        # 3. Enriquecer con WC REST API — fetch individual por ID
        #    (evita el filtro de status que bloquea custom statuses)
        #    Cada GET usa retry+backoff: si WC/Cloudflare cierra la conexión
        #    (RemoteDisconnected) en un ID, se reintenta y, si persiste, se
        #    SALTA ese producto en vez de tumbar toda la corrida del cron.
        all_products = []
        errores_conexion = 0
        for wc_id in ids:
            resp = _request_with_retry('GET', f"{BASE}/products/{wc_id}", auth=AUTH)
            if resp is None:
                errores_conexion += 1
                print(f"  [wc_api] Saltando ID {wc_id} — error de conexión persistente")
                continue
            if resp.status_code == 200:
                try:
                    prod = resp.json()
                except Exception:
                    print(f"  [wc_api] Respuesta no-JSON para ID {wc_id} — saltando")
                    continue
                # Filtrar por tag si aplica
                if tag_id and not any(t['id'] == tag_id for t in prod.get('tags', [])):
                    continue
                all_products.append(prod)
            else:
                print(f"  [wc_api] Error {resp.status_code} obteniendo producto ID {wc_id}")

        if errores_conexion:
            print(f"  [wc_api] {errores_conexion} producto(s) saltados por error de conexión")
        print(f"  [wc_api] Productos enriquecidos: {len(all_products)}")
        return all_products

    # Status estándar — consulta directa a la WC REST API
    all_products = []
    page = 1
    while True:
        params = {'status': status, 'per_page': per_page, 'page': page}
        if tag_id:
            params['tag'] = tag_id

        batch = _get('products', params)
        if not batch:
            break

        if id_min or id_max:
            batch = [p for p in batch if
                     (id_min is None or p['id'] >= id_min) and
                     (id_max is None or p['id'] <= id_max)]

        all_products.extend(batch)

        if len(batch) < per_page:
            break
        page += 1

    return all_products


def parse_product(wc_product: dict) -> dict:
    """
    Extrae los campos relevantes de un producto WooCommerce
    para construir el payload de ML.
    """
    meta_raw  = wc_product.get('meta_data', [])
    meta      = {m['key']: m['value'] for m in meta_raw}
    images    = wc_product.get('images', [])
    dims      = wc_product.get('dimensions', {})

    # Precio: usar siempre regular_price de WooCommerce
    precio = wc_product.get('regular_price') or wc_product.get('price') or '0'

    # Descripción: limpiar HTML
    desc_html = wc_product.get('description', '')
    desc_plain = _html_to_plain(desc_html)

    # Imágenes: URLs de WC (ML las descarga directamente)
    img_urls = [img['src'] for img in images if img.get('src')]
    # Detalle de imágenes con id + src para cruce con _kubera_editar_imagenes
    images_detail = [
        {'id': img.get('id'), 'src': img.get('src')}
        for img in images
        if img.get('src')
    ]

    # Flags de edición IA por imagen: {wc_image_id: {quitar_fondo, traducir_texto, cambiar_modelo}}
    edit_flags_raw = meta.get('_kubera_editar_imagenes') or []
    edit_flags_by_id: dict = {}
    if isinstance(edit_flags_raw, list):
        for entry in edit_flags_raw:
            if not isinstance(entry, dict):
                continue
            img_id = entry.get('imagen_id')
            if img_id is None:
                continue
            try:
                img_id = int(img_id)
            except (TypeError, ValueError):
                continue
            edit_flags_by_id[img_id] = {
                'quitar_fondo':   bool(entry.get('quitar_fondo')),
                'traducir_texto': bool(entry.get('traducir_texto')),
                'cambiar_modelo': bool(entry.get('cambiar_modelo')),
            }

    # Galería por variante (commercekit plugin)
    commercekit_gallery = meta.get('commercekit_image_gallery') or {}
    if not isinstance(commercekit_gallery, dict):
        commercekit_gallery = {}

    return {
        'wc_id':           wc_product['id'],
        'sku':             wc_product.get('sku', ''),
        'title':           wc_product.get('name', ''),
        'price':           float(precio),
        'description':     desc_plain,
        'images':          img_urls,
        'images_detail':   images_detail,
        'edit_flags':      edit_flags_by_id,
        'commercekit_gallery': commercekit_gallery,
        'weight':          wc_product.get('weight', ''),
        'length':          dims.get('length', ''),
        'width':           dims.get('width', ''),
        'height':          dims.get('height', ''),
        'stock':           wc_product.get('stock_quantity') or 50,
        # Meta ML (categoria cacheada — puede estar desactualizada vs WC visible)
        'ml_category_id':  meta.get('ml_category_id', ''),
        'ml_category_name':meta.get('ml_category_name', ''),
        # Categorias WC visibles (lo que las KAMs editan en admin)
        # cada una: {id, name, slug} — usado para detectar cambios manuales
        'wc_categories':   [{'id': c.get('id'), 'name': c.get('name'), 'slug': c.get('slug')}
                            for c in wc_product.get('categories', [])],
        # Atributos ML (prefijo ml_attr_*)
        'ml_attrs':        {k[len('ml_attr_'):]: v
                            for k, v in meta.items()
                            if k.startswith('ml_attr_') and v},
        # Atributos estándar WC (pa_color, pa_material, etc.)
        'wc_attrs':        {a['name'].lower(): a['options'][0]
                            for a in wc_product.get('attributes', [])
                            if a.get('options')},
        # Tags WC
        'tags':            [t['name'] for t in wc_product.get('tags', []) if t.get('name')],
        # Meta extra
        'meta':            meta,
    }


def update_product_status(wc_id: int, new_status: str = 'publish') -> bool:
    """
    Actualiza el status de un producto en WooCommerce via REST API.
    Retorna True si fue exitoso, False si hubo error.
    """
    url = f"{BASE}/products/{wc_id}"
    resp = _request_with_retry('PUT', url, json={'status': new_status}, auth=AUTH)
    if resp is None:
        print(f"  [wc_api] No se pudo actualizar status WC {wc_id} — error de conexión persistente")
        return False
    if resp.status_code == 200:
        return True
    print(f"  [wc_api] Error actualizando status WC {wc_id}: HTTP {resp.status_code} — {resp.text[:200]}")
    return False


def save_gtin_to_wc(wc_id: int, gtin: str) -> bool:
    """Guarda el GTIN encontrado en el meta _barcode del producto WC."""
    resp = _request_with_retry(
        'PUT', f"{BASE}/products/{wc_id}",
        json={'meta_data': [{'key': '_barcode', 'value': gtin}]},
        auth=AUTH, timeout=15,
    )
    return resp is not None and resp.status_code == 200


def _html_to_plain(html: str) -> str:
    """Convierte HTML a texto plano compatible con ML."""
    if not html:
        return ''
    # Convertir <li> y <br> a saltos de línea
    text = re.sub(r'<li[^>]*>', '- ', html)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'<p[^>]*>', '\n', text)
    # Quitar todas las etiquetas HTML
    text = re.sub(r'<[^>]+>', '', text)
    # Decodificar entidades HTML básicas
    text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>') \
               .replace('&nbsp;', ' ').replace('&#8211;', '-').replace('&#8212;', '-')
    # Limpiar espacios y saltos excesivos
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)
    return text.strip()