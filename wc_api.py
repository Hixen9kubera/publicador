"""
wc_api.py — Obtener productos de WooCommerce para publicar en ML
"""
import re
import xmlrpc.client
import requests
from config import WC_URL, WC_KEY, WC_SECRET

AUTH = (WC_KEY, WC_SECRET)
BASE = f"{WC_URL}/wp-json/wc/v3"

# Statuses estándar que acepta la WC REST API
_WC_STANDARD_STATUSES = {'any', 'future', 'trash', 'draft', 'pending', 'private', 'publish'}

# Credenciales XML-RPC (para statuses customizados: inprogress, ready, etc.)
_XMLRPC_URL  = f"{WC_URL}/xmlrpc.php"
_XMLRPC_USER = "brandon@kubera.mx"
_XMLRPC_PASS = "KV^!3nD!Ogh88uYHr)h!fo1a"


def _get(endpoint: str, params: dict) -> list:
    url = f"{BASE}/{endpoint}"
    print(f"  [wc_api] GET {url}")
    print(f"  [wc_api] Params: {params}")
    resp = requests.get(url, params=params, auth=AUTH, timeout=30)
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
        all_products = []
        for wc_id in ids:
            resp = requests.get(
                f"{BASE}/products/{wc_id}",
                auth=AUTH, timeout=30
            )
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

    return {
        'wc_id':           wc_product['id'],
        'sku':             wc_product.get('sku', ''),
        'title':           wc_product.get('name', ''),
        'price':           float(precio),
        'description':     desc_plain,
        'images':          img_urls,
        'weight':          wc_product.get('weight', ''),
        'length':          dims.get('length', ''),
        'width':           dims.get('width', ''),
        'height':          dims.get('height', ''),
        'stock':           wc_product.get('stock_quantity') or 50,
        # Meta ML
        'ml_category_id':  meta.get('ml_category_id', ''),
        'ml_category_name':meta.get('ml_category_name', ''),
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
    try:
        resp = requests.put(url, json={'status': new_status}, auth=AUTH, timeout=30)
        if resp.status_code == 200:
            return True
        print(f"  [wc_api] Error actualizando status WC {wc_id}: HTTP {resp.status_code} — {resp.text[:200]}")
        return False
    except Exception as e:
        print(f"  [wc_api] Excepción actualizando status WC {wc_id}: {e}")
        return False


def save_gtin_to_wc(wc_id: int, gtin: str) -> bool:
    """Guarda el GTIN encontrado en el meta _barcode del producto WC."""
    try:
        resp = requests.put(
            f"{BASE}/products/{wc_id}",
            json={'meta_data': [{'key': '_barcode', 'value': gtin}]},
            auth=AUTH,
            timeout=15,
        )
        return resp.status_code == 200
    except Exception:
        return False


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